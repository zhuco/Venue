//! One sizing contract for ordinary and managed followers.
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum FollowSizing {
    #[default]
    Proportional,
    FixedNotional {
        #[serde(with = "rust_decimal::serde::str")]
        notional: Decimal,
    },
}

impl<'de> Deserialize<'de> for FollowSizing {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // A tagged unit variant silently accepts extra fields; an empty struct does not.
        #[derive(Deserialize)]
        #[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Proportional {},
            FixedNotional {
                #[serde(with = "rust_decimal::serde::str")]
                notional: Decimal,
            },
        }
        Ok(match Wire::deserialize(deserializer)? {
            Wire::Proportional {} => Self::Proportional,
            Wire::FixedNotional { notional } => Self::FixedNotional { notional },
        })
    }
}

impl FollowSizing {
    pub fn valid_for(self, order_cap: Decimal) -> bool {
        match self {
            Self::Proportional => true,
            Self::FixedNotional { notional } => notional > Decimal::ZERO && notional <= order_cap,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_amount_is_positive_capped_and_rejects_ambiguous_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let sizing: FollowSizing =
            serde_json::from_str(r#"{"mode":"fixed_notional","notional":"5.5"}"#)?;
        assert!(sizing.valid_for(Decimal::new(55, 1)));
        assert!(!sizing.valid_for(Decimal::from(5)));
        for input in [
            r#"{"mode":"fixed_notional"}"#,
            r#"{"mode":"proportional","notional":"5.5"}"#,
            r#"{"mode":"unknown"}"#,
        ] {
            assert!(serde_json::from_str::<FollowSizing>(input).is_err());
        }
        for notional in [Decimal::ZERO, Decimal::NEGATIVE_ONE] {
            assert!(!FollowSizing::FixedNotional { notional }.valid_for(Decimal::from(10)));
        }
        Ok(())
    }
}
