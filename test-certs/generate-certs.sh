#!/usr/bin/env bash
set -euo pipefail

# Generate self-signed CA and server certificates for TLS testing.
# The generated certs are committed to the repo for reproducibility.

cd "$(dirname "$0")"

# CA key and certificate (10 year expiry)
openssl req -new -x509 -days 3650 -nodes \
    -keyout ca.key -out ca.crt \
    -subj "/CN=Elefant Test CA"

# Server key
openssl genrsa -out server.key 2048

# Server CSR with SANs
openssl req -new -key server.key -out server.csr \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# Sign server cert with CA (10 year expiry)
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt -days 3650 \
    -copy_extensions copyall

# Clean up intermediate files
rm -f server.csr ca.srl

# PostgreSQL requires server.key to be mode 0600
chmod 600 server.key

echo "Certificates generated successfully."
echo "  CA:     ca.crt, ca.key"
echo "  Server: server.crt, server.key"
