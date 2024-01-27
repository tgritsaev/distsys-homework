FROM python:3.9-slim

WORKDIR /http/server

RUN pip install click

COPY . .

ENTRYPOINT ["python", "server.py"]
