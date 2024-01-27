FROM registry.gitlab.com/nanobjorn/distsys-homework

WORKDIR /
COPY dslib dslib

WORKDIR /membership/
COPY 6-membership/test test

RUN (cd test && cargo fetch)

COPY 6-membership/ .

WORKDIR /membership/test
ENTRYPOINT ["cargo", "run", "--release",  "--", "-m", "1000"]
