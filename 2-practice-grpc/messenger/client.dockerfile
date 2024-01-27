FROM python:3.8-slim

WORKDIR /2-practice-grpc/messenger/


COPY __init__.py __init__.py
COPY client-py/ client-py/
COPY proto proto/

RUN pip install --upgrade pip
RUN pip install -r client-py/requirements.txt

WORKDIR /2-practice-grpc/
RUN python -m grpc_tools.protoc -Imessenger/proto --python_out=messenger/proto --grpc_python_out=messenger/proto messenger/proto/messenger.proto
RUN sed -i '/^import messenger_pb2 as messenger__pb2$/c\import messenger.proto.messenger_pb2 as messenger__pb2' messenger/proto/messenger_pb2_grpc.py
EXPOSE 8080
ENTRYPOINT ["python", "-m", "messenger.client-py.client"]
