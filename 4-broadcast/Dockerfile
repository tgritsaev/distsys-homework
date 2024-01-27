FROM registry.gitlab.com/nanobjorn/distsys-homework

WORKDIR /
COPY dslib dslib

WORKDIR /broadcast/
COPY 4-broadcast/test test

RUN (cd test && cargo fetch)

COPY 4-broadcast/ .

WORKDIR /broadcast/test
ENTRYPOINT ["cargo", "run", "--", "-m", "100"]
