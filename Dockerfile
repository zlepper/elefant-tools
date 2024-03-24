FROM scratch
COPY ./elefant-sync .
USER 1000
ENTRYPOINT ["./elefant-sync"]