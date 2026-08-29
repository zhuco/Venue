use std::env;

/// Reads only the explicitly requested credential names. Unlike `dotenvy::dotenv`, this never
/// copies unrelated exchange secrets from the workspace `.env` into the process environment.
pub(crate) fn required(name: &str) -> Result<String, CredentialEnvError> {
    required_any(&[name])
}

/// Names are ordered aliases. The first non-empty value wins, with an already supplied process
/// environment value taking precedence over the workspace `.env` file.
pub(crate) fn required_any(names: &[&str]) -> Result<String, CredentialEnvError> {
    if names.is_empty() {
        return Err(CredentialEnvError);
    }
    for name in names {
        if let Ok(value) = env::var(name)
            && !value.trim().is_empty()
        {
            return Ok(value);
        }
    }

    let mut values = Vec::new();
    let iter = dotenvy::from_filename_iter(".env").map_err(|_| CredentialEnvError)?;
    for entry in iter {
        values.push(entry.map_err(|_| CredentialEnvError)?);
    }
    select_non_empty(
        names,
        values
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
    )
    .ok_or(CredentialEnvError)
}

fn select_non_empty<'a>(
    names: &[&str],
    values: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Option<String> {
    let values = values.into_iter().collect::<Vec<_>>();
    names.iter().find_map(|requested| {
        values
            .iter()
            .find(|(name, value)| name == requested && !value.trim().is_empty())
            .map(|(_, value)| (*value).to_owned())
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CredentialEnvError;

#[cfg(test)]
mod tests {
    use super::select_non_empty;

    #[test]
    fn selects_only_an_explicit_name_without_returning_sibling_secret() {
        let values = [
            ("GATEIO_API_KEY", "gate-key"),
            ("BITGET_API_KEY", "bitget-key"),
        ];
        assert_eq!(
            select_non_empty(&["GATEIO_API_KEY"], values),
            Some("gate-key".to_owned())
        );
    }

    #[test]
    fn ordered_aliases_accept_the_existing_bitget_passphrase_name() {
        let values = [("BITGET_PASSPHRASE", "passphrase")];
        assert_eq!(
            select_non_empty(&["BITGET_API_PASSPHRASE", "BITGET_PASSPHRASE"], values),
            Some("passphrase".to_owned())
        );
    }
}
