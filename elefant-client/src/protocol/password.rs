// From https://github.com/sfackler/rust-postgres/blob/master/postgres-protocol/src/password/mod.rs

use base64::display::Base64Display;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};
use crate::protocol::sasl;

const SCRAM_DEFAULT_ITERATIONS: u32 = 4096;
const SCRAM_DEFAULT_SALT_LEN: usize = 16;

/// Hash password using SCRAM-SHA-256 with a randomly-generated
/// salt.
///
/// The client may assume the returned string doesn't contain any
/// special characters that would require escaping in an SQL command.
pub fn scram_sha_256(password: &[u8]) -> String {
    let mut salt: [u8; SCRAM_DEFAULT_SALT_LEN] = [0; SCRAM_DEFAULT_SALT_LEN];
    let mut rng = rand::thread_rng();
    rng.fill_bytes(&mut salt);
    scram_sha_256_salt(password, salt)
}

// Internal implementation of scram_sha_256 with a caller-provided
// salt. This is useful for testing.
pub(crate) fn scram_sha_256_salt(password: &[u8], salt: [u8; SCRAM_DEFAULT_SALT_LEN]) -> String {
    // Prepare the password, per [RFC
    // 4013](https://tools.ietf.org/html/rfc4013), if possible.
    //
    // Postgres treats passwords as byte strings (without embedded NUL
    // bytes), but SASL expects passwords to be valid UTF-8.
    //
    // Follow the behavior of libpq's PQencryptPasswordConn(), and
    // also the backend. If the password is not valid UTF-8, or if it
    // contains prohibited characters (such as non-ASCII whitespace),
    // just skip the SASLprep step and use the original byte
    // sequence.
    let prepared: Vec<u8> = match std::str::from_utf8(password) {
        Ok(password_str) => {
            match stringprep::saslprep(password_str) {
                Ok(p) => p.into_owned().into_bytes(),
                // contains invalid characters; skip saslprep
                Err(_) => Vec::from(password),
            }
        }
        // not valid UTF-8; skip saslprep
        Err(_) => Vec::from(password),
    };

    // salt password
    let salted_password = sasl::hi(&prepared, &salt, SCRAM_DEFAULT_ITERATIONS);

    // client key
    let mut hmac = Hmac::<Sha256>::new_from_slice(&salted_password)
        .expect("HMAC is able to accept all key sizes");
    hmac.update(b"Client Key");
    let client_key = hmac.finalize().into_bytes();

    // stored key
    let mut hash = Sha256::default();
    hash.update(client_key.as_slice());
    let stored_key = hash.finalize_fixed();

    // server key
    let mut hmac = Hmac::<Sha256>::new_from_slice(&salted_password)
        .expect("HMAC is able to accept all key sizes");
    hmac.update(b"Server Key");
    let server_key = hmac.finalize().into_bytes();

    format!(
        "SCRAM-SHA-256${}:{}${}:{}",
        SCRAM_DEFAULT_ITERATIONS,
        Base64Display::new(&salt, &STANDARD),
        Base64Display::new(&stored_key, &STANDARD),
        Base64Display::new(&server_key, &STANDARD)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encrypt_scram_sha_256() {
        // Specify the salt to make the test deterministic. Any bytes will do.
        let salt: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        assert_eq!(
            scram_sha_256_salt(b"secret", salt),
            "SCRAM-SHA-256$4096:AQIDBAUGBwgJCgsMDQ4PEA==$8rrDg00OqaiWXJ7p+sCgHEIaBSHY89ZJl3mfIsf32oY=:05L1f+yZbiN8O0AnO40Og85NNRhvzTS57naKRWCcsIA="
        );
    }
}