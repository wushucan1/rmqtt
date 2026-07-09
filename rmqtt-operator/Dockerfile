FROM rust:1.89-alpine AS builder

ENV PATH="/usr/local/cargo/bin:${PATH}"
RUN apk add --no-cache build-base pkgconf openssl-dev openssl-libs-static zlib-static

WORKDIR /src
COPY . .
RUN cargo build --release -p rmqtt-operator

FROM alpine:3.24.1

RUN adduser -D -u 10001 rmqtt-operator
COPY --from=builder /src/target/release/rmqtt-operator /usr/local/bin/rmqtt-operator

USER 10001
ENTRYPOINT ["/usr/local/bin/rmqtt-operator"]
