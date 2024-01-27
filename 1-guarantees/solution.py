from dslib import Context, Message, Node


EPS = 0.0001
MAX_DELAY = 3

# AT MOST ONCE ---------------------------------------------------------------------------------------------------------


class AtMostOnceSender(Node):
    def __init__(self, node_id: str, receiver_id: str):

        self._id = node_id
        self._receiver = receiver_id
        self.msgs_cnt = 0

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        msg["id"] = self.msgs_cnt
        self.msgs_cnt += 1
        ctx.send(msg, self._receiver)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        pass

    def on_timer(self, timer_id: str, ctx: Context):
        # process fired timers here
        pass


class AtMostOnceReceiver(Node):
    def __init__(self, node_id: str):
        self._id = node_id
        self.received_ids = set()
        # or array of constant size
        self.clean_time = 2

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        if msg["id"] not in self.received_ids:
            self.received_ids.add(msg["id"])
            ctx.set_timer(f"ids cleanup {msg['id']}", self.clean_time)
            msg.remove("id")
            ctx.send_local(msg)

    def on_timer(self, timer_id: str, ctx: Context):
        # process fired timers here
        remove_id = int(timer_id.split(" ")[-1])
        self.received_ids.remove(remove_id)


# AT LEAST ONCE --------------------------------------------------------------------------------------------------------


class AtLeastOnceSender(Node):
    def __init__(self, node_id: str, receiver_id: str):
        self._id = node_id
        self._receiver = receiver_id

        self.msgs_cnt = 0
        self.not_approved_msgs_ids = dict()

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        msg["id"] = self.msgs_cnt
        self.msgs_cnt += 1
        ctx.send(msg, self._receiver)
        ctx.set_timer(f"resend {msg['id']}", 2 * MAX_DELAY + EPS)
        self.not_approved_msgs_ids[msg["id"]] = msg

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        id = int(msg._type)
        ctx.cancel_timer(f"resend {id}")
        if id in self.not_approved_msgs_ids:
            self.not_approved_msgs_ids.pop(id)

    def on_timer(self, timer_id: str, ctx: Context):
        # process fired timers here
        msg_id = int(timer_id.split(" ")[-1])
        ctx.send(self.not_approved_msgs_ids[msg_id], self._receiver)
        ctx.set_timer(timer_id, 2 * MAX_DELAY + EPS)


class AtLeastOnceReceiver(Node):
    def __init__(self, node_id: str):
        self._id = node_id

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()

        ctx.send(Message(str(msg["id"]), {}), "sender")
        msg.remove("id")
        ctx.send_local(msg)

    def on_timer(self, timer_id: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE ---------------------------------------------------------------------------------------------------------


class ExactlyOnceSender(Node):
    def __init__(self, node_id: str, receiver_id: str):
        self._id = node_id
        self._receiver = receiver_id

        self.msgs_cnt = 0
        self.not_approved_msgs_ids = dict()

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        msg["id"] = self.msgs_cnt
        self.msgs_cnt += 1
        ctx.send(msg, self._receiver)
        ctx.set_timer(f"resend {msg['id']}", 2 * MAX_DELAY + EPS)
        self.not_approved_msgs_ids[msg["id"]] = msg["text"]

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        id = int(msg._type)
        ctx.cancel_timer(f"resend {id}")
        if id in self.not_approved_msgs_ids:
            self.not_approved_msgs_ids.pop(id)
        ctx.send(msg, self._receiver)

    def on_timer(self, timer_id: str, ctx: Context):
        # process fired timers here
        msg_id = int(timer_id.split(" ")[-1])
        ctx.send(Message("MESSAGE", {"text": self.not_approved_msgs_ids[msg_id], "id": msg_id}), self._receiver)
        ctx.set_timer(timer_id, 2 * MAX_DELAY + EPS)


class ExactlyOnceReceiver(Node):
    def __init__(self, node_id: str):
        self._id = node_id

        self.received_msgs_ids = []

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        if msg._type == "MESSAGE":
            ctx.send(Message(str(msg["id"]), {}), "sender")
            if msg["id"] not in self.received_msgs_ids:
                self.received_msgs_ids.append(msg["id"])
                msg.remove("id")
                ctx.send_local(msg)
        else:
            id = int(msg._type)
            for i in range(len(self.received_msgs_ids)):
                if self.received_msgs_ids[i] == id:
                    self.received_msgs_ids.pop(i)
                    break
            # self.received_msgs_ids.discard(id)

        # print(len(self.received_msgs_ids))

    def on_timer(self, timer_id: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE + ORDERED -----------------------------------------------------------------------------------------------


class ExactlyOnceOrderedSender(Node):
    def __init__(self, node_id: str, receiver_id: str):
        self._id = node_id
        self._receiver = receiver_id

        self.msgs_cnt = 0
        self.not_approved_msgs_ids = dict()
        self.first_not_approved_msg_id = 0

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        msg["id"] = self.msgs_cnt
        self.msgs_cnt += 1
        ctx.send(msg, self._receiver)
        ctx.set_timer(f"resend_msg: {msg['id']}", 2 * MAX_DELAY + EPS)
        self.not_approved_msgs_ids[msg["id"]] = msg

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        if msg._type == "MESSAGE":
            ctx.cancel_timer(f"resend_msg: {msg['id']}")
            if msg["id"] in self.not_approved_msgs_ids:
                self.not_approved_msgs_ids.pop(msg["id"])
        else:
            if len(self.not_approved_msgs_ids.keys()) > 0:
                self.first_not_approved_msg_id = min(self.not_approved_msgs_ids.keys())
            else:
                self.first_not_approved_msg_id = self.msgs_cnt
            msg = Message(str(-self.first_not_approved_msg_id), {})
            ctx.send(msg, self._receiver)

    def on_timer(self, timer_id: str, ctx: Context):
        # process fired timers here
        msg_id = int(timer_id.split(" ")[-1])
        ctx.send(self.not_approved_msgs_ids[msg_id], self._receiver)
        ctx.set_timer(timer_id, 2 * MAX_DELAY + EPS)


class ExactlyOnceOrderedReceiver(Node):
    def __init__(self, node_id: str):
        self._id = node_id

        self.first_not_approved_id = 0
        self.received_msgs = dict()

        self.timers_cnt = 0
        self.timer = 10

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        if msg._type != "MESSAGE":
            self.first_not_approved_id = -int(msg._type)
            for key_id in sorted(self.received_msgs.keys()):
                if key_id >= self.first_not_approved_id:
                    break
                ctx.send_local(self.received_msgs[key_id])
                self.received_msgs.pop(key_id)
        else:
            ctx.send(msg, "sender")
            ctx.set_timer(f"{self.timers_cnt}", self.timer)
            self.timers_cnt += 1
            if msg["id"] not in self.received_msgs:
                id = msg["id"]
                msg.remove("id")
                self.received_msgs[id] = msg

    def on_timer(self, timer_id: str, ctx: Context):
        # process fired timers here
        if len(self.received_msgs.keys()) > 0:
            ctx.set_timer(f"{self.timers_cnt}", self.timer)
            self.timers_cnt += 1
        ctx.send(Message("", {}), "sender")
