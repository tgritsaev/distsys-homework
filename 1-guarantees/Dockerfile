FROM registry.gitlab.com/nanobjorn/distsys-homework

WORKDIR /
COPY dslib dslib

WORKDIR /guarantees/
COPY 1-guarantees/test test

RUN (cd test && cargo fetch)

COPY 1-guarantees/ .

WORKDIR /guarantees/test
ENTRYPOINT ["cargo", "run", "--", "-m", "100", "-o"]
