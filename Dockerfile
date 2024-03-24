FROM scratch
COPY ./target/x86_64-unknown-linux-musl/release/elefant-sync .
USER 1000
ENTRYPOINT ["./elefant-sync"]