from dslib import Context, Message, Node
from typing import List

import random
import hashlib


N = 100
K = 1000
RING_SIZE = N * K


def get_pos_by_key(key):
    return abs(hash(key)) % RING_SIZE


class StorageNode(Node):

    is_virtual_inited = False
    actual_virtual_nodes = []
    set_actual_virtual_nodes = set()
    permutation = None
    virtual_nodes = dict()

    def __del__(self):
        StorageNode.is_virtual_inited = False
        StorageNode.actual_virtual_nodes = []
        StorageNode.set_actual_virtual_nodes = set()
        StorageNode.permutation = None
        StorageNode.virtual_nodes = dict()

    def add_virtual_nodes(self, node):
        l = int(node) * K
        r = (int(node) + 1) * K
        for virtual_node in self.permutation[l:r]:
            if virtual_node not in StorageNode.set_actual_virtual_nodes:
                StorageNode.actual_virtual_nodes.append(virtual_node)
                StorageNode.set_actual_virtual_nodes.add(virtual_node)
        StorageNode.actual_virtual_nodes.sort()

    def virtual_init(self):
        if not StorageNode.is_virtual_inited:
            StorageNode.is_virtual_inited = True
            StorageNode.permutation = [i for i in range(RING_SIZE)]
            random.shuffle(StorageNode.permutation)
            for node in range(N):
                for i in range(K):
                    ind = node * K + i
                    virtual_node = StorageNode.permutation[ind]
                    StorageNode.virtual_nodes[virtual_node] = node

            for node in self._nodes:
                self.add_virtual_nodes(node)

    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = set(nodes)
        self._data = {}

    def find_node_by_key(self, key):
        pos = get_pos_by_key(key)
        l = 0
        r = len(StorageNode.actual_virtual_nodes)
        m = None
        if pos < StorageNode.actual_virtual_nodes[0]:
            virtual_node = StorageNode.actual_virtual_nodes[0]
            return str(StorageNode.virtual_nodes[virtual_node])
        while l + 1 < r:
            m = (l + r) // 2
            if StorageNode.actual_virtual_nodes[m] <= pos:
                l = m
            else:
                r = m
        virtual_node = StorageNode.actual_virtual_nodes[l]
        return str(StorageNode.virtual_nodes[virtual_node])

    def on_local_message(self, msg: Message, ctx: Context):
        # print(msg.type, self._nodes, self._id)

        self.virtual_init()
        # Get key value.
        # Request:
        #   GET {"key": "some key"}
        # Response:
        #   GET_RESP {"key": "some key", "value": "value for this key"}
        #   GET_RESP {"key": "some key", "value": null} - if record for this key is not found
        if msg.type == "GET":
            key = msg["key"]
            node = self.find_node_by_key(key)
            if self._id == node:
                value = self._data.get(key)
                resp = Message("GET_RESP", {"key": key, "value": value})
                ctx.send_local(resp)
            else:
                new_msg = Message("GET_FROM " + self._id + " " + key, {})
                ctx.send(new_msg, node)

        # Store (key, value) record
        # Request:
        #   PUT {"key": "some key", "value: "some value"}
        # Response:
        #   PUT_RESP {"key": "some key", "value: "some value"}
        elif msg.type == "PUT":
            key = msg["key"]
            value = msg["value"]
            node = self.find_node_by_key(key)
            if self._id == node:
                self._data[key] = value
                resp = Message("PUT_RESP", {"key": key, "value": value})
                ctx.send_local(resp)
            else:
                new_msg = Message("PUT_FROM " + self._id, {"key": key, "value": value})
                ctx.send(new_msg, node)

        # Delete value for the key
        # Request:
        #   DELETE {"key": "some key"}
        # Response:
        #   DELETE_RESP {"key": "some key", "value": "some value"}
        elif msg.type == "DELETE":
            key = msg["key"]
            node = self.find_node_by_key(key)
            if self._id == node:
                value = self._data.pop(key, None)
                resp = Message("DELETE_RESP", {"key": key, "value": value})
                ctx.send_local(resp)
            else:
                new_msg = Message("DELETE_FROM " + self._id + " " + key, {})
                ctx.send(new_msg, node)

        # Notification that a new node is added to the system.
        # Request:
        #   NODE_ADDED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == "NODE_ADDED":
            if msg["id"] in self._nodes:
                return
            self._nodes.update(str(msg["id"]))
            self.add_virtual_nodes(msg["id"])
            transfer = dict()
            for key in list(self._data.keys()):
                node = self.find_node_by_key(key)
                if node == msg["id"]:
                    transfer[key] = self._data.pop(key)
            new_msg = Message("TRANSFER_KEYS", {"dict": transfer})
            ctx.send(new_msg, msg["id"])

        # Notification that a node is removed from the system.
        # Request:
        #   NODE_REMOVED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == "NODE_REMOVED":
            if msg["id"] not in self._nodes:
                return
            self._nodes.remove(msg["id"])
            if self._id != msg["id"]:
                return
            l = int(self._id) * K
            r = (int(self._id) + 1) * K
            for virtual_node in self.permutation[l:r]:
                StorageNode.set_actual_virtual_nodes.remove(virtual_node)
            StorageNode.actual_virtual_nodes = list(StorageNode.set_actual_virtual_nodes)
            StorageNode.actual_virtual_nodes.sort()
            transfers = dict()
            for key in list(self._data.keys()):
                node = self.find_node_by_key(key)
                if node not in transfers.keys():
                    transfers[node] = dict()
                transfers[node][key] = self._data.pop(key)
            for node in transfers.keys():
                new_msg = Message("TRANSFER_KEYS", {"dict": transfers[node]})
                ctx.send(new_msg, node)

        # Get number of records stored on the node
        # Request:
        #   COUNT_RECORDS {}
        # Response:
        #   COUNT_RECORDS_RESP {"count": 100}
        elif msg.type == "COUNT_RECORDS":
            resp = Message("COUNT_RECORDS_RESP", {"count": len(self._data)})
            ctx.send_local(resp)

        # Get keys of records stored on the node
        # Request:
        #   DUMP_KEYS {}
        # Response:
        #   DUMP_KEYS_RESP {"keys": ["key1", "key2", ...]}
        elif msg.type == "DUMP_KEYS":
            resp = Message("DUMP_KEYS_RESP", {"keys": list(self._data.keys())})
            ctx.send_local(resp)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # Implement node-to-node communication using any message types
        msg_type = msg.type.split()[0]
        if msg_type == "GET_FROM":
            _, from_node, key = msg.type.split()
            value = self._data.get(key)
            resp = Message("GET_RESP", {"key": key, "value": value})
            ctx.send(resp, from_node)
        elif msg.type == "GET_RESP":
            ctx.send_local(msg)

        elif msg_type == "PUT_FROM":
            _, from_node = msg.type.split()
            key = msg["key"]
            value = msg["value"]
            self._data[key] = value
            resp = Message("PUT_RESP", {"key": key, "value": value})
            ctx.send(resp, from_node)
        elif msg.type == "PUT_RESP":
            ctx.send_local(msg)

        elif msg_type == "DELETE_FROM":
            _, from_node, key = msg.type.split()
            value = self._data.pop(key, None)
            resp = Message("DELETE_RESP", {"key": key, "value": value})
            ctx.send(resp, from_node)
        elif msg.type == "DELETE_RESP":
            ctx.send_local(msg)

        elif msg_type == "TRANSFER_KEYS":
            for key, value in msg["dict"].items():
                self._data[key] = value
        elif msg_type == "TRANSFER_KEY":
            self._data[msg["key"]] = msg["value"]

    def on_timer(self, timer_name: str, ctx: Context):
        pass
