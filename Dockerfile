FROM rust:1.86 AS builder

WORKDIR /build

# Copy rebar dependency
COPY rebar/ /build/rebar/

# Copy project
COPY mega-obj-soaker/ /build/mega-obj-soaker/

WORKDIR /build/mega-obj-soaker
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/mega-obj-soaker/target/release/mega-obj-soaker /usr/local/bin/mega-obj-soaker

ENTRYPOINT ["mega-obj-soaker"]
