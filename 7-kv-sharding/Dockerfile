FROM registry.gitlab.com/nanobjorn/distsys-homework

WORKDIR /
COPY dslib dslib

WORKDIR /kv-sharding/
COPY 7-kv-sharding/test test

RUN (cd test && cargo fetch)

COPY 7-kv-sharding/ .

WORKDIR /kv-sharding/test
ENTRYPOINT ["cargo", "run", "--release"]
