FROM registry.gitlab.com/nanobjorn/distsys-homework

WORKDIR /
COPY dslib dslib

WORKDIR /kv-replication-v2/
COPY 9-kv-replication-v2/test test

RUN (cd test && cargo fetch)

COPY 9-kv-replication-v2/ .

WORKDIR /kv-replication-v2/test
ENTRYPOINT ["cargo", "run", "--release"]
