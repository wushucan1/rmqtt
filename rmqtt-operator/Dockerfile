FROM rust:1.89-alpine AS builder

ARG RMQTT_OPERATOR_BUILD_ID
ENV PATH="/usr/local/cargo/bin:${PATH}"
RUN apk add --no-cache build-base pkgconf openssl-dev openssl-libs-static zlib-static

WORKDIR /src
COPY . .
RUN test -n "${RMQTT_OPERATOR_BUILD_ID}" \
    && RMQTT_OPERATOR_BUILD_ID="${RMQTT_OPERATOR_BUILD_ID}" cargo build --release -p rmqtt-operator

FROM alpine:3.24.1

ARG RMQTT_OPERATOR_BUILD_ID
LABEL org.opencontainers.image.version="${RMQTT_OPERATOR_BUILD_ID}"

RUN adduser -D -u 10001 rmqtt-operator
COPY --from=builder /src/target/release/rmqtt-operator /usr/local/bin/rmqtt-operator
RUN test -n "${RMQTT_OPERATOR_BUILD_ID}" \
    && mkdir -p /usr/local/share/rmqtt-operator \
    && printf '%s\n' "${RMQTT_OPERATOR_BUILD_ID}" > /usr/local/share/rmqtt-operator/BUILD_ID

USER 10001
ENTRYPOINT ["/usr/local/bin/rmqtt-operator"]
