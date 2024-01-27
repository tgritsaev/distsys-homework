# import sys
# setting path
# sys.path.append("..")

from lib2to3.pgen2.literals import simple_escapes
import threading
import time
from concurrent import futures
import grpc

import asyncio

import google.protobuf
import messenger.proto.messenger_pb2 as messenger_pb2
import messenger.proto.messenger_pb2_grpc as messenger_pb2_grpc

import queue

lock = threading.Lock()
NUMBER_OF_REPLY = 10
DELAY = 0.1


def _get_timestamp():
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10**9)
    return google.protobuf.timestamp_pb2.Timestamp(seconds=seconds, nanos=nanos)


def is_bigger(a, b):
    if a.seconds > b.seconds:
        return True
    elif a.seconds == b.seconds:
        return a.nanos > b.nanos
    else:
        return False


def get_nanos_from_Timestamp(a):
    return a.seconds * (10**9) + a.nanos


class Messenger(messenger_pb2_grpc.MessengerServicer):
    def __init__(self):
        self.buffer = dict()
        self.active_listeners_cnt = 0
        self.read_first_message_cnt = 0

    def SendMessage(self, request, context):
        time = _get_timestamp()
        print(f"called Messenger.SendMessage at {time.seconds}.{time.nanos}")
        message = messenger_pb2.ServerMessage(author=request.author, text=request.text, sendTime=time)
        with lock:
            for key in self.buffer.keys():
                self.buffer[key].put(message)
        return messenger_pb2.SendMessageResponse(sendTime=time)

    def GetAndFlushMessages(self, request, context):
        connection_time = _get_timestamp()
        time_in_nanos = get_nanos_from_Timestamp(connection_time)

        def Disconnect():
            print("Disconnecting...")
            with lock:
                print(connection_time, end="")
                self.buffer.pop(time_in_nanos)
            print("Disconnected.")

        context.add_callback(Disconnect)
        with lock:
            self.buffer.update({time_in_nanos: queue.SimpleQueue()})
        print(f"called Messenger.GetAndFlushMessages at {connection_time.seconds}.{connection_time.nanos}")
        while True:
            message = self.buffer[time_in_nanos].get(block=True)
            yield message


def main():
    port = "51075"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messenger_pb2_grpc.add_MessengerServicer_to_server(Messenger(), server)
    server.add_insecure_port("0.0.0.0:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
