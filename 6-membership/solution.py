from dslib import Context, Message, Node
import random


HEARTBEAT_NOTIFY_POSITIVE_NODES_CNT = 3
HEARTBEAT_NOTIFY_NEGATIVE_NODES_CNT = 3

HEARTBEAT_DELAY = 4
HEARTBEAT_DELAYS = {10: 4, 20: 7.5, 50: 15, 100: 15}
PING_DELAY = 4.5

# detection fault constansts

FIND_NODE_TO_PING_TRIES = 10
WAITING_FIRST_ACK = HEARTBEAT_DELAY * 2
WAITING_SECOND_ACK = HEARTBEAT_DELAY * 3
PING2_CNT = 3

EPS = 0.001


def merge(a, b):
    for node in b:
        if node not in a:
            a[node] = b[node]
        elif abs(a[node]) < abs(b[node]):
            a[node] = b[node]


class GroupMember(Node):
    msg_cnt = 0
    msg_cnts = dict()

    HEARTBEAT_DELAY = 5

    def __init__(self, node_id: str):
        self.id = node_id

        self.members = dict()

        self.status = None
        self.waiting_first_ack_nodes = dict()
        self.waiting_second_ack_nodes = dict()

    def __del__(self):
        if GroupMember.msg_cnt > 0:
            # print("\n-----------")
            # print(len(self.members))
            msg_all_cnt = 0
            for msg_tmp, msg_cnt in GroupMember.msg_cnts.items():
                # print(msg_tmp, msg_cnt)
                msg_all_cnt += msg_cnt
            assert msg_all_cnt == GroupMember.msg_cnt
            GroupMember.msg_cnts = dict()
            # print("SUM", msg_all_cnt)
            # print("-----------\n")
            GroupMember.msg_cnt = 0

    def on_local_message(self, msg: Message, ctx: Context):
        msg_type = msg.type

        # if not msg_type in GroupMember.msg_cnts:
        #     GroupMember.msg_cnts[msg_type] = 0
        # GroupMember.msg_cnt += 1
        # GroupMember.msg_cnts[msg_type] += 1

        if msg.type == "JOIN":
            # Add local node to the group
            self.status = 1
            seed = msg["seed"]
            if seed == self.id:
                # create new empty group and add local node to it
                self.members[self.id] = ctx.time()
            else:
                # join existing group
                self.members[self.id] = ctx.time()
                self.members[seed] = ctx.time()
                join_msg = Message("JOIN " + self.id + " " + str(ctx.time()), {})
                ctx.send(join_msg, seed)
            ctx.set_timer("HEARTBEAT", GroupMember.HEARTBEAT_DELAY + random.random() / 10)
            ctx.set_timer("PING", PING_DELAY + random.random() / 10)

        elif msg.type == "LEAVE":
            # Remove local node from the group
            self.status = -1
            self.members[self.id] = ctx.time() * (-1)
            leave_msg = Message("LEFT_NOTIFY " + self.id + " " + str(self.members[self.id]), {})
            i = 0
            for node, time in sorted(self.members.items(), key=lambda x: random.random()):
                if node == self.id or time < 0:
                    continue
                ctx.send(leave_msg, self.id)
                i += 1
                if i >= HEARTBEAT_NOTIFY_POSITIVE_NODES_CNT:
                    break

        elif msg.type == "GET_MEMBERS":
            # Get a list of group members
            # - return the list of all known alive nodes in MEMBERS message
            members = []
            for node in self.members:
                if self.members[node] > 0:
                    members.append(node)
            ctx.send_local(Message("MEMBERS", {"members": sorted(members)}))

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # Implement node-to-node communication using any message types
        splitted_msg = msg._type.split()
        msg_type = splitted_msg[0]

        if not msg_type in GroupMember.msg_cnts:
            GroupMember.msg_cnts[msg_type] = 0
        GroupMember.msg_cnt += 1
        GroupMember.msg_cnts[msg_type] += 1

        sz = len(self.members)
        if sz <= 10:
            GroupMember.HEARTBEAT_DELAY = HEARTBEAT_DELAYS[10]
        elif sz <= 20:
            GroupMember.HEARTBEAT_DELAY = HEARTBEAT_DELAYS[20]
        elif sz <= 50:
            GroupMember.HEARTBEAT_DELAY = HEARTBEAT_DELAYS[50]
        elif sz <= 100:
            GroupMember.HEARTBEAT_DELAY = HEARTBEAT_DELAYS[100]

        if msg_type == "HEARTBEAT":
            _, node = splitted_msg
            if node not in self.members.keys() or self.members[node] > 0:
                merge(self.members, msg["group"])
            else:
                # merge(self.members, msg["group"])
                pass
        elif msg_type == "JOIN":
            _, node, time = splitted_msg
            self.members[node] = float(time)
        elif msg_type == "LEFT_NOTIFY":
            # merge(self.members, msg["group"])
            _, node, time = splitted_msg
            time = float(time)
            if node in self.members.keys():
                self.members[node] = time
        elif msg_type == "TRY_RECOVER":
            _, node = splitted_msg
            if self.status == 1:
                send_msg = Message("TRY_RECOVER#APPROVE " + self.id + " " + str(ctx.time()), {})
                ctx.send(send_msg, node)
        elif msg_type == "TRY_RECOVER#APPROVE":
            _, node, time = splitted_msg
            self.members[node] = abs(float(time)) + EPS
        elif msg_type == "PING":
            _, node, time = splitted_msg
            if self.members[self.id] > 0:
                if node not in self.members or self.members[node] > 0:
                    self.members[node] = float(time)
                    # ctx.cancel_timer("HEARTBEAT")
                    # ctx.set_timer("HEARTBEAT", HEARTBEAT_DELAY)
                send_msg = Message("PING#APPROVE " + self.id + " " + str(ctx.time()), {})
                ctx.send(send_msg, node)
        elif msg_type == "PING#APPROVE":
            _, node, time = splitted_msg
            time = float(time)
            self.members[node] = time
            del self.waiting_first_ack_nodes[node]

        elif msg_type == "PING2#FORWARD_INTERMEDIATE":
            _, node1, node2, node3 = splitted_msg
            send_msg = Message("PING2#FINAL_NODE " + node1 + " " + node2 + " " + node3, {})
            ctx.send(send_msg, node3)
        elif msg_type == "PING2#FINAL_NODE":
            _, node1, node2, node3 = splitted_msg
            if self.members[self.id] > 0:
                send_msg = Message("PING2#BACWARD_INTERMEDIATE " + node1 + " " + node2 + " " + node3, {})
                ctx.send(send_msg, node2)
        elif msg_type == "PING2#BACWARD_INTERMEDIATE":
            _, node1, node2, node3 = splitted_msg
            send_msg = Message("PING2#APPROVE " + node1 + " " + node2 + " " + node3, {})
            ctx.send(send_msg, node1)
        elif msg_type == "PING2#APPROVE":
            _, node1, node2, node3 = splitted_msg
            if node3 in self.waiting_second_ack_nodes.keys():
                del self.waiting_second_ack_nodes[node3]

    def on_timer(self, timer_name: str, ctx: Context):
        self.members[self.id] = ctx.time() * (1 if self.members[self.id] > 0 else -1)
        if timer_name == "HEARTBEAT":
            # sample = dict(random.sample(self.members.items(), len(self.members) // 3 + 1))
            heartbeat_msg = Message("HEARTBEAT " + self.id, {"group": self.members})
            i = 0
            for node, time in sorted(self.members.items(), key=lambda x: random.random()):
                if node == self.id or time < 0:
                    continue
                ctx.send(heartbeat_msg, node)
                i += 1
                if i >= HEARTBEAT_NOTIFY_POSITIVE_NODES_CNT:
                    break

            try_recover_msg = Message("TRY_RECOVER " + self.id, {})
            i = 0
            for node, time in sorted(self.members.items(), key=lambda x: random.random()):
                if node == self.id or time > 0:
                    continue
                ctx.send(try_recover_msg, node)
                i += 1
                if i >= HEARTBEAT_NOTIFY_NEGATIVE_NODES_CNT:
                    break
            ctx.set_timer("HEARTBEAT", GroupMember.HEARTBEAT_DELAY)
        elif timer_name == "PING":
            i = 0
            while i < FIND_NODE_TO_PING_TRIES:
                node_to_ping, time = sorted(self.members.items(), key=lambda x: random.random())[0]
                if node_to_ping != self.id and time > 0 and node_to_ping not in self.waiting_first_ack_nodes.keys():
                    ping_msg = Message("PING " + self.id + " " + str(ctx.time()), {})
                    ctx.send(ping_msg, node_to_ping)
                    self.waiting_first_ack_nodes[node_to_ping] = ctx.time()
                    break
                i += 1
            ctx.set_timer("PING", PING_DELAY + random.random() / 10)

        nw_time = ctx.time()
        for node_to_ping, time in list(self.waiting_first_ack_nodes.items()):
            if nw_time - time > WAITING_FIRST_ACK:
                del self.waiting_first_ack_nodes[node_to_ping]

                if node_to_ping in self.waiting_second_ack_nodes.keys():
                    continue
                self.waiting_second_ack_nodes[node_to_ping] = nw_time

                i = 0
                for intermediate_node, other_time in sorted(self.members.items(), key=lambda x: random.random()):
                    if intermediate_node == self.id or other_time < 0:
                        continue
                    ping_msg = Message("PING2#FORWARD_INTERMEDIATE " + self.id + " " + intermediate_node + " " + node_to_ping, {})
                    ctx.send(ping_msg, intermediate_node)
                    i += 1
                    if i >= PING2_CNT:
                        break

        for node_to_ping, time in list(self.waiting_second_ack_nodes.items()):
            if nw_time - time > WAITING_SECOND_ACK:
                del self.waiting_second_ack_nodes[node_to_ping]

                self.members[node_to_ping] = (self.members[node_to_ping] + 1) * (-1)
