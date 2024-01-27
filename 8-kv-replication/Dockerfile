FROM registry.gitlab.com/nanobjorn/distsys-homework

WORKDIR /
COPY dslib dslib

WORKDIR /kv-replication/
COPY 8-kv-replication/test test

RUN (cd test && cargo fetch)

COPY 8-kv-replication/ .

WORKDIR /kv-replication/test
ENTRYPOINT ["cargo", "run", "--release"]
