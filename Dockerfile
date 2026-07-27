# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
FROM rust:1.88.0-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder

ARG APP_PACKAGE=alpaca-autotrader
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo build --locked --release --package "${APP_PACKAGE}" && \
    cp "/build/target/release/${APP_PACKAGE}" /tmp/alpaca-autotrader

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:fccdbb0a547c14e23fcf4ce8ad62ca5d43b4faae8d22cd292f490fef9946c96e AS runtime

COPY --from=builder --chown=65532:65532 /tmp/alpaca-autotrader /app/alpaca-autotrader

USER nonroot:nonroot
WORKDIR /app

ENV RUST_BACKTRACE=0

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD ["/app/alpaca-autotrader", "health", "--local"]

ENTRYPOINT ["/app/alpaca-autotrader"]
