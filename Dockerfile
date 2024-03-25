FROM rust:1.77-buster as builder

ENV HOME=/home/root
WORKDIR /data

RUN apt update && apt install -y libssl-dev
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/data/target \
    cargo b -r &&\
    cp target/release/s3s-rados ./

FROM debian:buster
RUN apt update &&\
    apt install -y openssl &&\
    rm -rf /var/lib/apt/lists
COPY --from=builder /data/s3s-rados /s3s-rados
CMD ["/s3s-rados", "-c", "/config.yaml"]