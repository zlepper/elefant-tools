use crate::protocol::async_io::{ElefantAsyncRead, ElefantAsyncReadWrite, ElefantAsyncWrite};
use der::Decode;
use rustls::ClientConnection;
use sha2::Digest;
use std::io::{self, Read, Write};
use x509_cert::Certificate;

enum HashAlgorithm {
    Sha256,
    Sha384,
    Sha512,
}

pub struct TlsStream<S> {
    inner: S,
    tls: ClientConnection,
    write_buf: Vec<u8>,
}

impl<S: ElefantAsyncReadWrite> TlsStream<S> {
    pub fn new(inner: S, tls: ClientConnection) -> Self {
        Self {
            inner,
            tls,
            write_buf: Vec::new(),
        }
    }

    /// Push all pending encrypted data from rustls to the inner stream.
    async fn drain_tls_writes(&mut self) -> io::Result<()> {
        while self.tls.wants_write() {
            self.write_buf.clear();
            self.tls.write_tls(&mut self.write_buf)?;
            self.inner.write_all(&self.write_buf).await?;
        }
        self.inner.flush().await?;
        Ok(())
    }

    /// Drive the TLS handshake to completion.
    pub async fn handshake(&mut self) -> io::Result<()> {
        let mut read_buf = [0u8; 4096];
        loop {
            self.drain_tls_writes().await?;

            if !self.tls.is_handshaking() {
                break;
            }

            let n = self.inner.read(&mut read_buf).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF during TLS handshake",
                ));
            }

            self.tls
                .read_tls(&mut &read_buf[..n])
                .expect("read_tls from slice cannot fail");

            self.tls
                .process_new_packets()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
        Ok(())
    }

    /// Send a TLS close_notify alert and flush it to the underlying stream.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.tls.send_close_notify();
        self.drain_tls_writes().await
    }

    /// Extract channel binding data (hash of the server's DER-encoded certificate).
    ///
    /// Per RFC 5929 §4.1: use the certificate's signature hash algorithm,
    /// unless it's MD5 or SHA-1, in which case use SHA-256.
    pub fn channel_binding_data(&self) -> Option<Vec<u8>> {
        let certs = self.tls.peer_certificates()?;
        let end_entity_der = certs.first()?;
        let hash_bytes = match Self::signature_hash_algorithm(end_entity_der.as_ref()) {
            HashAlgorithm::Sha256 => sha2::Sha256::digest(end_entity_der.as_ref()).to_vec(),
            HashAlgorithm::Sha384 => sha2::Sha384::digest(end_entity_der.as_ref()).to_vec(),
            HashAlgorithm::Sha512 => sha2::Sha512::digest(end_entity_der.as_ref()).to_vec(),
        };
        Some(hash_bytes)
    }

    /// Determine the hash algorithm from the certificate's signature algorithm OID.
    fn signature_hash_algorithm(der: &[u8]) -> HashAlgorithm {
        let cert = Certificate::from_der(der).ok();
        match cert {
            Some(cert) => oid_to_hash(&cert.signature_algorithm.oid),
            None => HashAlgorithm::Sha256, // fallback
        }
    }
}

impl<S: ElefantAsyncReadWrite> ElefantAsyncRead for TlsStream<S> {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            // Try to read decrypted data that rustls already has buffered.
            let read_result: io::Result<usize> = self.tls.reader().read(buf);
            match read_result {
                Ok(n) if n > 0 => return Ok(n),
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
                // WouldBlock means rustls needs more encrypted data from the network.
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Ok(_) => {} // 0 bytes — need more data
                Err(e) => return Err(e),
            }

            // Flush any pending writes (e.g. key updates, alerts).
            self.drain_tls_writes().await?;

            // Read encrypted data from the network.
            let mut read_buf = [0u8; 4096];
            let n = self.inner.read(&mut read_buf).await?;
            if n == 0 {
                // EOF on the underlying stream. Let rustls handle it.
                self.tls
                    .read_tls(&mut &[][..])
                    .expect("read_tls from empty slice cannot fail");
                self.tls
                    .process_new_packets()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                // Try once more to read whatever rustls decoded.
                let eof_result: io::Result<usize> = self.tls.reader().read(buf);
                return match eof_result {
                    Ok(n) => Ok(n),
                    Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(0),
                    Err(e) => Err(e),
                };
            }

            self.tls
                .read_tls(&mut &read_buf[..n])
                .expect("read_tls from slice cannot fail");

            self.tls
                .process_new_packets()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
    }
}

impl<S: ElefantAsyncReadWrite> ElefantAsyncWrite for TlsStream<S> {
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        // Write plaintext into rustls (synchronous, just buffers it).
        self.tls.writer().write_all(buf)?;
        // Push the resulting encrypted data out.
        self.drain_tls_writes().await
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.tls.writer().flush()?;
        self.drain_tls_writes().await?;
        self.inner.flush().await
    }
}

/// Result of TLS negotiation with the PostgreSQL server.
pub enum TlsNegotiationResult<S> {
    /// TLS handshake succeeded.
    Tls(Box<TlsStream<S>>, Option<Vec<u8>>),
    /// Server declined SSL (responded with 'N').
    Declined(S),
}

/// Perform PostgreSQL SSLRequest negotiation and TLS handshake over a raw stream.
///
/// Sends the SSLRequest message, reads the server's response, and if accepted,
/// performs the TLS handshake. If the server declines, returns `Declined` with
/// the original stream so the caller can decide whether to proceed unencrypted.
///
/// This function is runtime-agnostic — it works with any `ElefantAsyncReadWrite` stream.
pub async fn negotiate_tls<S: ElefantAsyncReadWrite>(
    mut stream: S,
    host: &str,
    tls_config: &std::sync::Arc<rustls::ClientConfig>,
) -> Result<TlsNegotiationResult<S>, crate::ElefantClientError> {
    use rustls_pki_types::ServerName;

    // Send SSLRequest message: 8-byte message with length=8 and code=80877103
    let ssl_request: [u8; 8] = [
        0x00, 0x00, 0x00, 0x08, // length = 8
        0x04, 0xd2, 0x16, 0x2f, // code = 80877103
    ];
    stream.write_all(&ssl_request).await?;
    stream.flush().await?;

    // Read single-byte response
    let mut response = [0u8; 1];
    let n = stream.read(&mut response).await?;
    if n == 0 {
        return Err(crate::ElefantClientError::TlsError(
            "server closed connection during SSL negotiation".into(),
        ));
    }

    match response[0] {
        b'S' => {
            // Server accepts SSL — proceed with TLS handshake
            let server_name = ServerName::try_from(host)
                .map_err(|e| {
                    crate::ElefantClientError::TlsError(format!("invalid server name: {e}"))
                })?
                .to_owned();

            let tls_conn =
                ClientConnection::new(tls_config.clone(), server_name).map_err(|e| {
                    crate::ElefantClientError::TlsError(format!(
                        "failed to create TLS connection: {e}"
                    ))
                })?;

            let mut tls_stream = TlsStream::new(stream, tls_conn);
            tls_stream.handshake().await.map_err(|e| {
                crate::ElefantClientError::TlsError(format!("TLS handshake failed: {e}"))
            })?;

            let channel_binding = tls_stream.channel_binding_data();
            Ok(TlsNegotiationResult::Tls(Box::new(tls_stream), channel_binding))
        }
        b'N' => Ok(TlsNegotiationResult::Declined(stream)),
        other => Err(crate::ElefantClientError::TlsError(format!(
            "unexpected SSL response byte: 0x{other:02x}"
        ))),
    }
}

/// Map a signature algorithm OID to the hash algorithm to use for
/// `tls-server-end-point` channel binding (RFC 5929 §4.1).
///
/// MD5, SHA-1, and unknown algorithms default to SHA-256.
fn oid_to_hash(oid: &der::oid::ObjectIdentifier) -> HashAlgorithm {
    // OID constants for common signature algorithms
    use der::oid::ObjectIdentifier as Oid;

    // SHA-256 with RSA: 1.2.840.113549.1.1.11
    const SHA256_RSA: Oid = Oid::new_unwrap("1.2.840.113549.1.1.11");
    // SHA-384 with RSA: 1.2.840.113549.1.1.12
    const SHA384_RSA: Oid = Oid::new_unwrap("1.2.840.113549.1.1.12");
    // SHA-512 with RSA: 1.2.840.113549.1.1.13
    const SHA512_RSA: Oid = Oid::new_unwrap("1.2.840.113549.1.1.13");
    // ECDSA with SHA-256: 1.2.840.10045.4.3.2
    const ECDSA_SHA256: Oid = Oid::new_unwrap("1.2.840.10045.4.3.2");
    // ECDSA with SHA-384: 1.2.840.10045.4.3.3
    const ECDSA_SHA384: Oid = Oid::new_unwrap("1.2.840.10045.4.3.3");
    // ECDSA with SHA-512: 1.2.840.10045.4.3.4
    const ECDSA_SHA512: Oid = Oid::new_unwrap("1.2.840.10045.4.3.4");

    if *oid == SHA384_RSA || *oid == ECDSA_SHA384 {
        HashAlgorithm::Sha384
    } else if *oid == SHA512_RSA || *oid == ECDSA_SHA512 {
        HashAlgorithm::Sha512
    } else if *oid == SHA256_RSA || *oid == ECDSA_SHA256 {
        HashAlgorithm::Sha256
    } else {
        // MD5/SHA-1 with RSA, Ed25519/Ed448, or unknown → SHA-256 per RFC 5929
        HashAlgorithm::Sha256
    }
}
