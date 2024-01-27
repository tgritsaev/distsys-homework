FROM rust:1.62.0-slim-buster

RUN apt-get -y update &&  apt-get install -y python3 python3-dev build-essential

COPY 1-guarantees /tmp/1-guarantees
RUN (cd /tmp/1-guarantees/test && cargo fetch)

ENV PYTHONUNBUFFERED=1

CMD ["bash"]
