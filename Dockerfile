FROM rust:slim-buster AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y pkg-config libssl-dev
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY ./src ./src
RUN cargo build --release


FROM debian:buster-slim
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev ca-certificates
COPY --from=builder /build/target/release/s3-reproxy .
ENTRYPOINT ["./s3-reproxy"]

