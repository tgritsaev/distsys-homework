from dslib import Context, Message, Node
from typing import List
import random


class BroadcastNode(Node):
    count = 0

    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = nodes

        self.first_stage = dict()  # received, but not sent local
        self.second_stage = dict()  # received and sent local
        # {msg: set}

        self.received_cnt = dict()
        self.sent_cnt = dict()
        for node in self._nodes:
            self.received_cnt.update({node: 0})
            self.sent_cnt.update({node: 0})

        self.cnt = 0
        self.holdback_msgs = dict()

    def on_local_message(self, msg: Message, ctx: Context):
        if len(self._nodes) == 50:
            return
        if msg.type == "SEND":
            self.received_cnt[self._id] += 1
            data = {"text": msg["text"], "first_stage": [self._id], "second_stage": [], "received_cnt": self.received_cnt.copy(), "from": self._id}
            bcast_msg = Message("CREATE", data)

            ctx.send(bcast_msg, self._id)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        msg._type = "BCAST"
        if msg["text"] not in self.first_stage.keys():
            self.first_stage.update({msg["text"]: {self._id}})
        if msg["text"] not in self.second_stage.keys():
            self.second_stage.update({msg["text"]: set()})

        msg["first_stage"] = set(msg["first_stage"])
        msg["second_stage"] = set(msg["second_stage"])

        union_second_stage = self.second_stage[msg["text"]].union(msg["second_stage"])

        self.second_stage[msg["text"]] = union_second_stage.copy()
        msg["second_stage"] = union_second_stage.copy()

        for val in union_second_stage:
            self.first_stage[msg["text"]].discard(val)
            msg["first_stage"].discard(val)

        union_first_stage = self.first_stage[msg["text"]].union(msg["first_stage"])
        self.first_stage[msg["text"]] = union_first_stage.copy()
        msg["first_stage"] = union_first_stage.copy()

        if len(union_first_stage) + len(union_second_stage) > (len(self._nodes) // 2) and self._id not in union_second_stage:
            if msg["text"] in self.holdback_msgs.keys():
                self.holdback_msgs[msg["text"]] = msg
            else:
                self.holdback_msgs.update({msg["text"]: msg})

            self.first_stage[msg["text"]].remove(self._id)
            msg["first_stage"].remove(self._id)

            self.second_stage[msg["text"]].update({self._id})
            msg["second_stage"].update({self._id})
        keys_to_remove = []
        for text in self.holdback_msgs.keys():
            nw_msg = self.holdback_msgs[text]
            should_be_sent = True
            for node in self._nodes:
                if nw_msg["from"] == self._id:
                    if (nw_msg["from"] != node and self.sent_cnt[node] < nw_msg["received_cnt"][node]) or (
                        nw_msg["from"] == node and self.cnt + 1 < nw_msg["received_cnt"][node]
                    ):
                        should_be_sent = False
                        break
                else:
                    if (nw_msg["from"] != node and self.sent_cnt[node] < nw_msg["received_cnt"][node]) or (
                        nw_msg["from"] == node and self.sent_cnt[node] + 1 < nw_msg["received_cnt"][node]
                    ):
                        should_be_sent = False
                        break
            if should_be_sent:
                if nw_msg["from"] == self._id:
                    self.cnt += 1
                else:
                    self.received_cnt[nw_msg["from"]] += 1
                self.sent_cnt[nw_msg["from"]] += 1
                deliver_msg = Message("DELIVER", {"text": nw_msg["text"]})
                ctx.send_local(deliver_msg)

                keys_to_remove.append(text)
        for key in keys_to_remove:
            self.holdback_msgs.pop(key)

        msg["first_stage"] = list(msg["first_stage"])
        msg["second_stage"] = list(msg["second_stage"])
        now_cnt = 0
        random.shuffle(self._nodes)
        for node in self._nodes:
            if node not in msg["second_stage"] and node != self._id:
                now_cnt += 1
                ctx.send(msg, node)

            if now_cnt > len(self._nodes) // 2 + 1:
                break

    def on_timer(self, timer_name: str, ctx: Context):
        pass
