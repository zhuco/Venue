use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

const MAX_ASSET_LEN: usize = 24;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Symbol {
    base: String,
    quote: String,
}

impl Symbol {
    pub fn new(base: &str, quote: &str) -> Result<Self, SymbolError> {
        let base = asset(base, "base")?;
        let quote = asset(quote, "quote")?;

        if base == quote {
            return Err(SymbolError::SameAsset);
        }

        Ok(Self { base, quote })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn quote(&self) -> &str {
        &self.quote
    }
}

impl FromStr for Symbol {
    type Err = SymbolError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let (base, quote) = raw.split_once('/').ok_or(SymbolError::Format)?;
        if quote.contains('/') {
            return Err(SymbolError::Format);
        }
        Self::new(base, quote)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SymbolError {
    #[error("symbol must use BASE/QUOTE")]
    Format,

    #[error("{part} asset must contain 1 to {MAX_ASSET_LEN} ASCII letters or digits")]
    Asset { part: &'static str },

    #[error("base and quote assets must differ")]
    SameAsset,
}

fn asset(raw: &str, part: &'static str) -> Result<String, SymbolError> {
    let value = raw.trim().to_ascii_uppercase();
    let valid_len = (1..=MAX_ASSET_LEN).contains(&value.len());
    let valid_chars = value.bytes().all(|byte| byte.is_ascii_alphanumeric());

    if !valid_len || !valid_chars {
        return Err(SymbolError::Asset { part });
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_symbol() -> Result<(), SymbolError> {
        let symbol = Symbol::from_str(" btc / usdt ")?;

        assert_eq!(symbol.base(), "BTC");
        assert_eq!(symbol.quote(), "USDT");
        assert_eq!(symbol.to_string(), "BTC/USDT");
        Ok(())
    }

    #[test]
    fn rejects_native_or_ambiguous_forms() {
        assert_eq!(Symbol::from_str("BTCUSDT"), Err(SymbolError::Format));
        assert_eq!(Symbol::from_str("BTC-USDT"), Err(SymbolError::Format));
        assert_eq!(Symbol::from_str("BTC/BTC"), Err(SymbolError::SameAsset));
    }
}
