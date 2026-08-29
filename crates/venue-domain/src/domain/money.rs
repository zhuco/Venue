use std::{fmt, str::FromStr};

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

const MAX_ASSET_LEN: usize = 24;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Asset(String);

impl Asset {
    pub fn new(raw: &str) -> Result<Self, AmountError> {
        let value = raw.trim().to_ascii_uppercase();
        let valid = (1..=MAX_ASSET_LEN).contains(&value.len())
            && value.bytes().all(|byte| byte.is_ascii_alphanumeric());
        if valid {
            Ok(Self(value))
        } else {
            Err(AmountError::Asset)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Asset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for Asset {
    type Err = AmountError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::new(raw)
    }
}

impl Serialize for Asset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Asset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Amount {
    pub asset: Asset,
    #[serde(with = "rust_decimal::serde::str")]
    pub value: Decimal,
}

impl Amount {
    pub const fn new(asset: Asset, value: Decimal) -> Self {
        Self { asset, value }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(#[serde(with = "rust_decimal::serde::str")] Decimal);

impl Price {
    pub fn new(value: Decimal) -> Result<Self, AmountError> {
        if value.is_sign_positive() && !value.is_zero() {
            Ok(Self(value))
        } else {
            Err(AmountError::NonPositivePrice)
        }
    }

    pub const fn value(self) -> Decimal {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AmountError {
    #[error("asset must contain 1 to {MAX_ASSET_LEN} ASCII letters or digits")]
    Asset,
    #[error("price must be positive")]
    NonPositivePrice,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn amount_serializes_decimal_without_float_loss() -> Result<(), Box<dyn std::error::Error>> {
        let amount = Amount::new(Asset::new("usdt")?, Decimal::new(123, 2));

        assert_eq!(
            serde_json::to_string(&amount)?,
            r#"{"asset":"USDT","value":"1.23"}"#
        );
        Ok(())
    }
}
