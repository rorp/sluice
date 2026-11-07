# Builds the regtest loop server inside a checkout of lightninglabs/loop, so that it uses loop's own go.mod and
# packages (swap scripts, swapserverrpc).
FROM golang:1.25-bookworm AS builder

ARG LOOP_REPO=https://github.com/lightninglabs/loop.git
ARG LOOP_COMMIT=fd3ed6cf4f71d9e914c9a6082be160203bcd8787
ENV GOTOOLCHAIN=auto

RUN git clone ${LOOP_REPO} /src/loop && cd /src/loop && git checkout ${LOOP_COMMIT}
WORKDIR /src/loop
RUN go mod download

COPY *.go /src/loop/cmd/regtestloopserver/
RUN CGO_ENABLED=0 go build -o /bin/regtestloopserver ./cmd/regtestloopserver

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/regtestloopserver /bin/regtestloopserver
EXPOSE 11009
ENTRYPOINT ["/bin/regtestloopserver"]
