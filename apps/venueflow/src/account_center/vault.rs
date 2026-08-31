use serde::{Deserialize, Serialize};
use venue_control_protocol::accounts::{LoginRequest, SessionResponse};
use zeroize::Zeroizing;

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SavedAccount {
    version: u8,
    endpoint: String,
    pub login: Option<LoginRequest>,
    pub session: Option<SessionResponse>,
}

// This record never goes through eframe storage. API keys and account projections
// are deliberately not part of it; the server remains the authority for identity.
pub(super) struct Vault {
    endpoint: String,
    #[cfg(target_os = "windows")]
    entry: keyring::Entry,
}

impl Vault {
    pub fn supported() -> bool {
        cfg!(target_os = "windows")
    }

    pub fn open(endpoint: &str) -> Result<Option<Self>, ()> {
        #[cfg(all(target_os = "windows", not(test)))]
        {
            let endpoint = endpoint_key(endpoint).ok_or(())?;
            let entry = keyring::Entry::new("VenueFlow.Account.v1", &endpoint).map_err(|_| ())?;
            Ok(Some(Self { endpoint, entry }))
        }
        #[cfg(any(not(target_os = "windows"), test))]
        {
            // Tests never access the user's OS vault, even through app startup.
            let _ = endpoint;
            Ok(None)
        }
    }

    pub fn load(&self, now: u64) -> Result<SavedAccount, ()> {
        #[cfg(target_os = "windows")]
        {
            match self.entry.get_secret() {
                Ok(bytes) => decode(&Zeroizing::new(bytes), &self.endpoint, now),
                Err(keyring::Error::NoEntry) => Ok(SavedAccount::default()),
                Err(_) => Err(()),
            }
        }
        #[cfg(not(target_os = "windows"))]
        {
            let _ = now;
            Err(())
        }
    }

    pub fn save(
        &self,
        login: Option<&LoginRequest>,
        session: Option<&SessionResponse>,
    ) -> Result<(), ()> {
        #[cfg(target_os = "windows")]
        if login.is_none() && session.is_none() {
            return match self.entry.delete_credential() {
                Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
                Err(_) => Err(()),
            };
        }
        let record = SavedAccount {
            version: 1,
            endpoint: self.endpoint.clone(),
            login: login.cloned(),
            session: session.cloned(),
        };
        let bytes = Zeroizing::new(serde_json::to_vec(&record).map_err(|_| ())?);
        if bytes.len() > 2560 {
            return Err(());
        }
        #[cfg(target_os = "windows")]
        return self.entry.set_secret(&bytes).map_err(|_| ());
        #[cfg(not(target_os = "windows"))]
        Err(())
    }

    #[cfg(all(test, target_os = "windows"))]
    pub fn fixture(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.into(),
            entry: keyring::Entry::new_with_credential(Box::new(
                keyring::mock::MockCredential::default(),
            )),
        }
    }
}

#[cfg(any(target_os = "windows", test))]
fn endpoint_key(endpoint: &str) -> Option<String> {
    if endpoint.len() > 512 || !crate::account_client::safe_endpoint(endpoint) {
        return None;
    }
    reqwest::Url::parse(endpoint)
        .ok()
        .map(|url| url.as_str().trim_end_matches('/').to_owned())
}

#[cfg(any(target_os = "windows", test))]
fn decode(bytes: &[u8], endpoint: &str, now: u64) -> Result<SavedAccount, ()> {
    if bytes.len() > 2560 {
        return Err(());
    }
    let mut record: SavedAccount = serde_json::from_slice(bytes).map_err(|_| ())?;
    if record.version != 1 || endpoint_key(&record.endpoint).as_deref() != Some(endpoint) {
        return Err(());
    }
    if let Some(login) = &record.login
        && (login.normalized_username().is_none()
            || login.password.expose().is_empty()
            || login.password.expose().len() > 512)
    {
        return Err(());
    }
    if record.session.as_ref().is_some_and(|session| {
        session.expires_ms <= now
            || session.user.user_id.is_empty()
            || session.user.user_id.len() > 128
            || session.user.username.len() > 64
            || session.token.expose().is_empty()
            || session.token.expose().len() > 512
    }) {
        record.session = None;
    }
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn vault_scope_and_schema_fail_closed() {
        let endpoint = "https://control.example.com";
        assert_eq!(
            endpoint_key("https://CONTROL.example.com/"),
            Some(endpoint.into())
        );
        assert!(endpoint_key("http://remote.example.com").is_none());
        let bytes = br#"{"version":1,"endpoint":"https://control.example.com","login":null,"session":null}"#;
        assert!(decode(bytes, endpoint, 1).is_ok());
        assert!(decode(bytes, "https://other.example.com", 1).is_err());
        assert!(decode(b"broken", endpoint, 1).is_err());
        assert!(decode(&vec![0; 2561], endpoint, 1).is_err());
        assert!(decode(br#"{"version":2,"endpoint":"https://control.example.com","login":null,"session":null}"#, endpoint, 1).is_err());
    }
}
