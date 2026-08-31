use argon2::{
    Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::{
    aead, digest,
    rand::{SecureRandom, SystemRandom},
};
use zeroize::Zeroizing;

use super::{AccountError, error};
use venue_control_protocol::accounts::{AccountErrorCode, SecretValue};

pub struct CredentialCipher {
    key: aead::LessSafeKey,
}

impl CredentialCipher {
    pub fn from_environment() -> Result<Self, AccountError> {
        let encoded = Zeroizing::new(
            std::env::var("VENUE_ACCOUNT_MASTER_KEY")
                .map_err(|_| error(AccountErrorCode::Unavailable))?,
        );
        let bytes = Zeroizing::new(
            STANDARD
                .decode(encoded.as_bytes())
                .map_err(|_| error(AccountErrorCode::Unavailable))?,
        );
        Self::from_key(&bytes)
    }

    pub fn from_key(bytes: &[u8]) -> Result<Self, AccountError> {
        let key = aead::UnboundKey::new(&aead::AES_256_GCM, bytes)
            .map_err(|_| error(AccountErrorCode::Unavailable))?;
        Ok(Self {
            key: aead::LessSafeKey::new(key),
        })
    }

    pub fn encrypt(&self, scope: &str, plaintext: &[u8]) -> Result<Vec<u8>, AccountError> {
        let nonce = random::<12>()?;
        let mut ciphertext = plaintext.to_vec();
        self.key
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(scope.as_bytes()),
                &mut ciphertext,
            )
            .map_err(|_| error(AccountErrorCode::Unavailable))?;
        let mut envelope = Vec::with_capacity(13 + ciphertext.len());
        envelope.push(1);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&ciphertext);
        Ok(envelope)
    }

    pub fn decrypt(
        &self,
        scope: &str,
        envelope: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, AccountError> {
        if envelope.len() < 29 || envelope.first() != Some(&1) {
            return Err(error(AccountErrorCode::Unavailable));
        }
        let nonce: [u8; 12] = envelope[1..13]
            .try_into()
            .map_err(|_| error(AccountErrorCode::Unavailable))?;
        let mut bytes = Zeroizing::new(envelope[13..].to_vec());
        let length = self
            .key
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(scope.as_bytes()),
                &mut bytes,
            )
            .map_err(|_| error(AccountErrorCode::Unavailable))?
            .len();
        bytes.truncate(length);
        Ok(bytes)
    }
}

pub fn random<const N: usize>() -> Result<[u8; N], AccountError> {
    let mut bytes = [0; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| error(AccountErrorCode::Unavailable))?;
    Ok(bytes)
}

pub fn opaque_id() -> Result<String, AccountError> {
    let mut bytes = random::<16>()?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = hex(&bytes);
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}

pub fn new_token() -> Result<SecretValue, AccountError> {
    Ok(SecretValue::new(hex(&random::<32>()?)))
}

pub fn fingerprint(bytes: &[u8]) -> Vec<u8> {
    digest::digest(&digest::SHA256, bytes).as_ref().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn password_hasher() -> Result<Argon2<'static>, AccountError> {
    let params =
        Params::new(19_456, 2, 1, Some(32)).map_err(|_| error(AccountErrorCode::Unavailable))?;
    Ok(Argon2::new(
        argon2::Algorithm::Argon2id,
        argon2::Version::V0x13,
        params,
    ))
}

pub fn hash_password(password: &SecretValue) -> Result<String, AccountError> {
    let salt = SaltString::encode_b64(&random::<16>()?)
        .map_err(|_| error(AccountErrorCode::Unavailable))?;
    password_hasher()?
        .hash_password(password.expose().as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| error(AccountErrorCode::Unavailable))
}

pub fn verify_password(password: &SecretValue, encoded: &str) -> Result<bool, AccountError> {
    let hash = PasswordHash::new(encoded).map_err(|_| error(AccountErrorCode::Unavailable))?;
    Ok(password_hasher()?
        .verify_password(password.expose().as_bytes(), &hash)
        .is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelopes_reject_tampering_different_owners_and_keys() -> Result<(), AccountError> {
        let cipher = CredentialCipher::from_key(&[42; 32])?;
        let a = cipher.encrypt("user-a:credential-a", b"a real secret")?;
        let b = cipher.encrypt("user-a:credential-a", b"a real secret")?;
        assert_ne!(a, b);
        assert_eq!(
            cipher.decrypt("user-a:credential-a", &a)?.as_slice(),
            b"a real secret"
        );
        assert!(cipher.decrypt("user-b:credential-a", &a).is_err());
        assert!(
            CredentialCipher::from_key(&[43; 32])?
                .decrypt("user-a:credential-a", &a)
                .is_err()
        );
        let mut damaged = a;
        damaged[20] ^= 1;
        assert!(cipher.decrypt("user-a:credential-a", &damaged).is_err());
        assert!(cipher.decrypt("x", &[0; 5]).is_err());
        Ok(())
    }

    #[test]
    fn passwords_are_salted_and_verified() -> Result<(), AccountError> {
        let password = SecretValue::new("long unique passphrase".into());
        let a = hash_password(&password)?;
        assert_ne!(a, hash_password(&password)?);
        assert!(verify_password(&password, &a)?);
        assert!(!verify_password(&SecretValue::new("wrong".into()), &a)?);
        assert!(!a.contains(password.expose()));
        assert!(venue_domain::is_canonical_trading_account_id(&opaque_id()?));
        Ok(())
    }
}
