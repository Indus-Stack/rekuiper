# Copyright (c) 2024-2026 I-Dacs Labs (https://i-dacs.com)
# Copyright 2024-2026 rekuiper Authors
#
# Licensed under the MIT License or Apache License, Version 2.0 at your option.
# See LICENSE and LICENSE-APACHE for details.

# ==============================================================================
# Build Stage: Multi-stage compilation using official Rust Alpine image
# ==============================================================================
FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static sqlite-dev sqlite-static build-base

WORKDIR /usr/src/rekuiper

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY etc ./etc

# Build release binaries in parallel
RUN cargo build --release -j6

# ==============================================================================
# Runtime Stage: Ultra-lightweight secure Alpine runtime
# ==============================================================================
FROM alpine:latest

ENV KUIPER_HOME="/kuiper" \
    KUIPER__BASIC__CONSOLELOG="true" \
    KUIPER__BASIC__RESTIP="0.0.0.0" \
    KUIPER__BASIC__HTTPSERVERIP="0.0.0.0" \
    RUST_LOG="info"

ARG KUIPER_USER="kuiper"
ARG KUIPER_USER_ID="1001"
ARG WITH_FFMPEG="true"

RUN apk add --no-cache ca-certificates libgcc libssl3 sqlite-libs tzdata && \
    if [ "${WITH_FFMPEG}" = "true" ]; then apk add --no-cache ffmpeg; fi && \
    adduser -DH -s /sbin/nologin -u ${KUIPER_USER_ID} ${KUIPER_USER} && \
    mkdir -p ${KUIPER_HOME}/bin ${KUIPER_HOME}/etc ${KUIPER_HOME}/data ${KUIPER_HOME}/log && \
    chown -R ${KUIPER_USER}:${KUIPER_USER} ${KUIPER_HOME}

WORKDIR ${KUIPER_HOME}

# Copy compiled binaries from builder stage
COPY --from=builder --chown=${KUIPER_USER}:${KUIPER_USER} /usr/src/rekuiper/target/release/rekuiperd /kuiper/bin/rekuiperd
COPY --from=builder --chown=${KUIPER_USER}:${KUIPER_USER} /usr/src/rekuiper/target/release/kuiperd /kuiper/bin/kuiperd
COPY --from=builder --chown=${KUIPER_USER}:${KUIPER_USER} /usr/src/rekuiper/target/release/rekuiper /kuiper/bin/rekuiper
COPY --from=builder --chown=${KUIPER_USER}:${KUIPER_USER} /usr/src/rekuiper/target/release/kuiper /kuiper/bin/kuiper
COPY --from=builder --chown=${KUIPER_USER}:${KUIPER_USER} /usr/src/rekuiper/target/release/rekuiper-mcp /kuiper/bin/rekuiper-mcp

# Copy default configurations
COPY --chown=${KUIPER_USER}:${KUIPER_USER} etc/ /kuiper/etc/

USER ${KUIPER_USER}

VOLUME ["/kuiper/etc", "/kuiper/data", "/kuiper/log"]

EXPOSE 9081 20498 20499 59720

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:9081/ || wget --no-verbose --tries=1 --spider http://127.0.0.1:59720/ || exit 1

ENTRYPOINT ["/kuiper/bin/rekuiperd"]
CMD ["--etc", "/kuiper/etc"]
