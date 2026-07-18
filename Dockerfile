FROM rust:1.94-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.title="Memoree" \
    org.opencontainers.image.description="Local, artifact-first memory for machine agents" \
    org.opencontainers.image.url="https://memoree.dev" \
    org.opencontainers.image.documentation="https://memoree.dev"

RUN useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin memoree \
    && install -d -m 0700 -o memoree -g memoree /data

COPY --from=builder /build/target/release/memoree /usr/local/bin/memoree
COPY LICENSE /usr/share/licenses/memoree/

USER memoree
ENV MEMOREE_HOME=/data
VOLUME ["/data"]
EXPOSE 17878
STOPSIGNAL SIGINT

ENTRYPOINT ["memoree"]
CMD ["serve", "--listen", "tcp://127.0.0.1:17878"]
