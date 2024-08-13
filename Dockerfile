FROM rust:slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y pkg-config libssl-dev
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./

RUN mkdir src/
RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs
RUN cargo build --release
RUN rm -f target/release/deps/s3_reproxy*
RUN rm src -rf

COPY ./src ./src
RUN cargo build --release


FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev ca-certificates
COPY --from=builder /build/target/release/s3-reproxy .
ENTRYPOINT ["./s3-reproxy"]

