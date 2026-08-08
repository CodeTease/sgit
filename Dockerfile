# --- Stage 1: Build binary with MUSL ---
FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static pkgconfig gcc make perl
WORKDIR /app
COPY . .
RUN cargo build --release

# --- Stage 2: Runtime Minimal ---
FROM alpine:latest
RUN apk add --no-cache git ca-certificates

RUN addgroup -g 10001 -S sgitgroup && \
    adduser -u 10001 -S sgituser -G sgitgroup

RUN mkdir -p /var/lib/sgit && \
    chown -R sgituser:sgitgroup /var/lib/sgit

COPY --from=builder /app/target/release/sgit /usr/local/bin/sgit
RUN chown sgituser:sgitgroup /usr/local/bin/sgit

USER sgituser:sgitgroup

ENV SGIT_PORT=3000
EXPOSE 3000

ENTRYPOINT ["sgit"]
