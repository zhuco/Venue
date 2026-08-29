use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{Amount, Asset, Price, Symbol};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    Spot,
    LinearPerpetual,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Instrument {
    pub symbol: Symbol,
    pub market: MarketKind,
    pub settlement_asset: Option<Asset>,
    pub generation: u64,
    pub price_tick: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity_step: Decimal,
    pub minimum_notional: Amount,
}

impl Instrument {
    pub fn validate(&self) -> Result<(), InstrumentError> {
        if self.generation == 0 {
            return Err(InstrumentError::Generation);
        }
        if !self.quantity_step.is_sign_positive() || self.quantity_step.is_zero() {
            return Err(InstrumentError::QuantityStep);
        }
        if self.minimum_notional.value.is_sign_negative() {
            return Err(InstrumentError::MinimumNotional);
        }
        if matches!(self.market, MarketKind::LinearPerpetual) && self.settlement_asset.is_none() {
            return Err(InstrumentError::SettlementAsset);
        }
        Ok(())
    }

    /// Returns the venue-independent identity; native symbols remain adapter-owned.
    #[must_use]
    pub fn identity(&self) -> InstrumentIdentity {
        InstrumentIdentity {
            symbol: self.symbol.clone(),
            market: self.market,
            settlement_asset: self.settlement_asset.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InstrumentError {
    #[error("instrument generation must be positive")]
    Generation,
    #[error("quantity step must be positive")]
    QuantityStep,
    #[error("minimum notional cannot be negative")]
    MinimumNotional,
    #[error("linear perpetual requires a settlement asset")]
    SettlementAsset,
}

/// Stable identity shared across venues, metadata generations and adapter-native symbols.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct InstrumentIdentity {
    pub symbol: Symbol,
    pub market: MarketKind,
    pub settlement_asset: Option<Asset>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Precision {
    #[serde(with = "rust_decimal::serde::str")]
    pub step: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum: Decimal,
}

impl Precision {
    pub fn new(step: Decimal, minimum: Decimal) -> Result<Self, InstrumentValueError> {
        let value = Self { step, minimum };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), InstrumentValueError> {
        if self.step <= Decimal::ZERO || self.minimum < Decimal::ZERO {
            return Err(InstrumentValueError::Precision);
        }
        Ok(())
    }

    pub fn accepts(&self, value: Decimal) -> Result<bool, InstrumentValueError> {
        self.validate()?;
        Ok(value >= self.minimum && value % self.step == Decimal::ZERO)
    }

    pub fn floor(&self, value: Decimal) -> Result<Decimal, InstrumentValueError> {
        self.validate()?;
        if value < Decimal::ZERO {
            return Err(InstrumentValueError::Quantity);
        }
        Ok(value - value % self.step)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueUnit {
    Base,
    Quote,
}

/// Contract-lot metadata supplied by an adapter, never inferred from a symbol.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContractSpec {
    #[serde(with = "rust_decimal::serde::str")]
    pub value_per_lot: Decimal,
    pub value_unit: ValueUnit,
    pub lots: Precision,
}

impl ContractSpec {
    pub fn new(
        value_per_lot: Decimal,
        value_unit: ValueUnit,
        lots: Precision,
    ) -> Result<Self, InstrumentValueError> {
        let value = Self {
            value_per_lot,
            value_unit,
            lots,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), InstrumentValueError> {
        self.lots.validate()?;
        if self.value_per_lot <= Decimal::ZERO {
            return Err(InstrumentValueError::Contract);
        }
        Ok(())
    }

    pub fn quote_notional(
        &self,
        lots: Decimal,
        price: Option<Price>,
    ) -> Result<Decimal, InstrumentValueError> {
        self.validate()?;
        if lots < Decimal::ZERO || !self.lots.accepts(lots)? {
            return Err(InstrumentValueError::Quantity);
        }
        let value = lots
            .checked_mul(self.value_per_lot)
            .ok_or(InstrumentValueError::Overflow)?;
        match self.value_unit {
            ValueUnit::Quote => Ok(value),
            ValueUnit::Base => value
                .checked_mul(required_price(price)?)
                .ok_or(InstrumentValueError::Overflow),
        }
    }

    pub fn lots_for_quote_notional(
        &self,
        notional: Decimal,
        price: Option<Price>,
    ) -> Result<Decimal, InstrumentValueError> {
        self.validate()?;
        if notional < Decimal::ZERO {
            return Err(InstrumentValueError::Notional);
        }
        let divisor = match self.value_unit {
            ValueUnit::Quote => self.value_per_lot,
            ValueUnit::Base => self
                .value_per_lot
                .checked_mul(required_price(price)?)
                .ok_or(InstrumentValueError::Overflow)?,
        };
        let raw = notional
            .checked_div(divisor)
            .ok_or(InstrumentValueError::Overflow)?;
        let lots = self.lots.floor(raw)?;
        if !self.lots.accepts(lots)? {
            return Err(InstrumentValueError::Quantity);
        }
        Ok(lots)
    }
}

fn required_price(price: Option<Price>) -> Result<Decimal, InstrumentValueError> {
    price
        .map(Price::value)
        .ok_or(InstrumentValueError::PriceRequired)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InstrumentValueError {
    #[error("precision step must be positive and minimum cannot be negative")]
    Precision,
    #[error("contract value per lot must be positive")]
    Contract,
    #[error("price is required for base-denominated value")]
    PriceRequired,
    #[error("quantity is negative, below minimum, or not aligned")]
    Quantity,
    #[error("quote notional is invalid or below minimum")]
    Notional,
    #[error("quote notional asset does not match")]
    NotionalAsset,
    #[error("instrument is not enabled for trading")]
    TradingDisabled,
    #[error("instrument metadata is invalid or inconsistent")]
    Metadata,
    #[error("decimal arithmetic overflow")]
    Overflow,
}

/// Complete canonical rules for one generation; legacy `Instrument` construction stays intact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InstrumentMetadata {
    pub instrument: Instrument,
    pub price: Precision,
    pub quantity: Precision,
    pub contract: Option<ContractSpec>,
    pub trading_enabled: bool,
}

impl InstrumentMetadata {
    pub fn new(
        instrument: Instrument,
        price: Precision,
        quantity: Precision,
        contract: Option<ContractSpec>,
        trading_enabled: bool,
    ) -> Result<Self, InstrumentMetadataError> {
        let value = Self {
            instrument,
            price,
            quantity,
            contract,
            trading_enabled,
        };
        value.validate()?;
        Ok(value)
    }

    #[must_use]
    pub fn identity(&self) -> InstrumentIdentity {
        self.instrument.identity()
    }

    pub fn validate(&self) -> Result<(), InstrumentMetadataError> {
        self.instrument
            .validate()
            .map_err(|_| InstrumentMetadataError::Instrument)?;
        self.price
            .validate()
            .map_err(|_| InstrumentMetadataError::PricePrecision)?;
        self.quantity
            .validate()
            .map_err(|_| InstrumentMetadataError::QuantityPrecision)?;
        if self.price.minimum <= Decimal::ZERO
            || self.price.step != self.instrument.price_tick.value()
        {
            return Err(InstrumentMetadataError::PricePrecision);
        }
        if self.quantity.minimum <= Decimal::ZERO
            || self.quantity.step != self.instrument.quantity_step
        {
            return Err(InstrumentMetadataError::QuantityPrecision);
        }
        if self.instrument.minimum_notional.asset.as_str() != self.instrument.symbol.quote() {
            return Err(InstrumentMetadataError::NotionalAsset);
        }
        match self.instrument.market {
            MarketKind::Spot => {
                if self.instrument.settlement_asset.is_some() || self.contract.is_some() {
                    return Err(InstrumentMetadataError::Market);
                }
            }
            MarketKind::LinearPerpetual => {
                if self
                    .instrument
                    .settlement_asset
                    .as_ref()
                    .is_none_or(|asset| asset.as_str() != self.instrument.symbol.quote())
                {
                    return Err(InstrumentMetadataError::Market);
                }
            }
        }
        if let Some(contract) = &self.contract {
            contract
                .validate()
                .map_err(|_| InstrumentMetadataError::Contract)?;
            if contract.lots != self.quantity {
                return Err(InstrumentMetadataError::ContractQuantity);
            }
        }
        Ok(())
    }

    pub fn quote_notional(
        &self,
        quantity: Decimal,
        price: Option<Price>,
    ) -> Result<Amount, InstrumentValueError> {
        self.validate()
            .map_err(|_| InstrumentValueError::Metadata)?;
        if !self.trading_enabled {
            return Err(InstrumentValueError::TradingDisabled);
        }
        if quantity <= Decimal::ZERO || !self.quantity.accepts(quantity)? {
            return Err(InstrumentValueError::Quantity);
        }
        let value = match &self.contract {
            Some(contract) => contract.quote_notional(quantity, price)?,
            None => quantity
                .checked_mul(required_price(price)?)
                .ok_or(InstrumentValueError::Overflow)?,
        };
        if value < self.instrument.minimum_notional.value {
            return Err(InstrumentValueError::Notional);
        }
        Ok(Amount::new(
            self.instrument.minimum_notional.asset.clone(),
            value,
        ))
    }

    pub fn quantity_for_quote_notional(
        &self,
        notional: &Amount,
        price: Option<Price>,
    ) -> Result<Decimal, InstrumentValueError> {
        self.validate()
            .map_err(|_| InstrumentValueError::Metadata)?;
        if !self.trading_enabled {
            return Err(InstrumentValueError::TradingDisabled);
        }
        if notional.asset != self.instrument.minimum_notional.asset {
            return Err(InstrumentValueError::NotionalAsset);
        }
        if notional.value <= Decimal::ZERO
            || notional.value < self.instrument.minimum_notional.value
        {
            return Err(InstrumentValueError::Notional);
        }
        let quantity = match &self.contract {
            Some(contract) => contract.lots_for_quote_notional(notional.value, price)?,
            None => self.quantity.floor(
                notional
                    .value
                    .checked_div(required_price(price)?)
                    .ok_or(InstrumentValueError::Overflow)?,
            )?,
        };
        if quantity <= Decimal::ZERO || !self.quantity.accepts(quantity)? {
            return Err(InstrumentValueError::Quantity);
        }
        if self.quote_notional(quantity, price)?.value < self.instrument.minimum_notional.value {
            return Err(InstrumentValueError::Notional);
        }
        Ok(quantity)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InstrumentMetadataError {
    #[error("legacy instrument fields are invalid")]
    Instrument,
    #[error("price precision is invalid or does not match price_tick")]
    PricePrecision,
    #[error("quantity precision is invalid or does not match quantity_step")]
    QuantityPrecision,
    #[error("minimum notional is not denominated in the quote asset")]
    NotionalAsset,
    #[error("market, settlement asset, and contract rules are inconsistent")]
    Market,
    #[error("contract specification is invalid")]
    Contract,
    #[error("contract lot precision differs from quantity precision")]
    ContractQuantity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InstrumentSnapshot {
    pub metadata: InstrumentMetadata,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
}

impl InstrumentSnapshot {
    pub fn new(
        metadata: InstrumentMetadata,
        observed_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<Self, InstrumentSnapshotError> {
        let value = Self {
            metadata,
            observed_at_ms,
            expires_at_ms,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), InstrumentSnapshotError> {
        self.metadata
            .validate()
            .map_err(|_| InstrumentSnapshotError::Metadata)?;
        if self.observed_at_ms == 0 {
            return Err(InstrumentSnapshotError::ObservationTime);
        }
        if self.expires_at_ms <= self.observed_at_ms {
            return Err(InstrumentSnapshotError::InvalidWindow);
        }
        Ok(())
    }

    /// Requires exact identity/generation and the half-open window `[observed, expires)`.
    pub fn require(
        &self,
        expected_identity: &InstrumentIdentity,
        expected_generation: u64,
        now_ms: u64,
    ) -> Result<&InstrumentMetadata, InstrumentSnapshotError> {
        self.validate()?;
        if &self.metadata.identity() != expected_identity {
            return Err(InstrumentSnapshotError::IdentityMismatch);
        }
        if self.metadata.instrument.generation != expected_generation {
            return Err(InstrumentSnapshotError::GenerationMismatch);
        }
        if now_ms < self.observed_at_ms {
            return Err(InstrumentSnapshotError::NotYetObserved);
        }
        if now_ms >= self.expires_at_ms {
            return Err(InstrumentSnapshotError::Expired);
        }
        if !self.metadata.trading_enabled {
            return Err(InstrumentSnapshotError::TradingDisabled);
        }
        Ok(&self.metadata)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InstrumentSnapshotError {
    #[error("instrument metadata is invalid")]
    Metadata,
    #[error("observation time must be positive")]
    ObservationTime,
    #[error("expiry must be later than observation")]
    InvalidWindow,
    #[error("snapshot identity does not match")]
    IdentityMismatch,
    #[error("snapshot generation does not match")]
    GenerationMismatch,
    #[error("snapshot cannot be used before observation")]
    NotYetObserved,
    #[error("snapshot has expired")]
    Expired,
    #[error("instrument is not enabled for trading")]
    TradingDisabled,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(generation: u64) -> Result<InstrumentMetadata, Box<dyn std::error::Error>> {
        let quote = Asset::new("USDT")?;
        let quantity = Precision::new(Decimal::new(1, 1), Decimal::new(2, 1))?;
        InstrumentMetadata::new(
            Instrument {
                symbol: "BTC/USDT".parse()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(quote.clone()),
                generation,
                price_tick: Price::new(Decimal::new(1, 1))?,
                quantity_step: Decimal::new(1, 1),
                minimum_notional: Amount::new(quote, Decimal::from(5)),
            },
            Precision::new(Decimal::new(1, 1), Decimal::new(1, 1))?,
            quantity.clone(),
            Some(ContractSpec::new(
                Decimal::new(1, 3),
                ValueUnit::Base,
                quantity,
            )?),
            true,
        )
        .map_err(Into::into)
    }

    #[test]
    fn identity_excludes_generation_and_rules() -> Result<(), Box<dyn std::error::Error>> {
        let first = metadata(1)?;
        let mut second = metadata(2)?;
        second.instrument.minimum_notional.value = Decimal::from(100);
        assert_eq!(first.identity(), second.identity());
        Ok(())
    }

    #[test]
    fn precision_checks_boundary_step_and_floor() -> Result<(), Box<dyn std::error::Error>> {
        let value = Precision::new(Decimal::new(5, 2), Decimal::new(1, 1))?;
        assert!(!value.accepts(Decimal::new(5, 2))?);
        assert!(value.accepts(Decimal::new(10, 2))?);
        assert!(!value.accepts(Decimal::new(12, 2))?);
        assert_eq!(value.floor(Decimal::new(128, 3))?, Decimal::new(10, 2));
        assert_eq!(
            Precision::new(Decimal::ZERO, Decimal::ZERO),
            Err(InstrumentValueError::Precision)
        );
        Ok(())
    }

    #[test]
    fn contract_conversion_rounds_down_and_fails_closed() -> Result<(), Box<dyn std::error::Error>>
    {
        let lots = Precision::new(Decimal::ONE, Decimal::ONE)?;
        let base = ContractSpec::new(Decimal::new(1, 3), ValueUnit::Base, lots.clone())?;
        let price = Price::new(Decimal::from(50_000))?;
        assert_eq!(
            base.quote_notional(Decimal::from(10), Some(price))?,
            Decimal::from(500)
        );
        assert_eq!(
            base.lots_for_quote_notional(Decimal::from(549), Some(price))?,
            Decimal::from(10)
        );
        assert_eq!(
            base.quote_notional(Decimal::ONE, None),
            Err(InstrumentValueError::PriceRequired)
        );
        let quote = ContractSpec::new(Decimal::from(10), ValueUnit::Quote, lots)?;
        assert_eq!(
            quote.quote_notional(Decimal::MAX, None),
            Err(InstrumentValueError::Overflow)
        );
        Ok(())
    }

    #[test]
    fn metadata_sizes_with_minimum_and_asset_checks() -> Result<(), Box<dyn std::error::Error>> {
        let value = metadata(7)?;
        let price = Price::new(Decimal::from(50_000))?;
        assert_eq!(
            value.quote_notional(Decimal::new(2, 1), Some(price))?.value,
            Decimal::from(10)
        );
        assert_eq!(
            value.quantity_for_quote_notional(
                &Amount::new(Asset::new("USDT")?, Decimal::from(19)),
                Some(price),
            )?,
            Decimal::new(3, 1)
        );
        assert_eq!(
            value.quantity_for_quote_notional(
                &Amount::new(Asset::new("USDC")?, Decimal::from(10)),
                Some(price),
            ),
            Err(InstrumentValueError::NotionalAsset)
        );
        Ok(())
    }

    #[test]
    fn snapshot_checks_exact_generation_identity_and_expiry()
    -> Result<(), Box<dyn std::error::Error>> {
        let observed_metadata = metadata(7)?;
        let identity = observed_metadata.identity();
        let snapshot = InstrumentSnapshot::new(observed_metadata, 100, 200)?;
        assert!(snapshot.require(&identity, 7, 100).is_ok());
        assert_eq!(
            snapshot.require(&identity, 6, 150),
            Err(InstrumentSnapshotError::GenerationMismatch)
        );
        assert_eq!(
            snapshot.require(&identity, 7, 99),
            Err(InstrumentSnapshotError::NotYetObserved)
        );
        assert_eq!(
            snapshot.require(&identity, 7, 200),
            Err(InstrumentSnapshotError::Expired)
        );
        let mut other = identity;
        other.symbol = "ETH/USDT".parse()?;
        assert_eq!(
            snapshot.require(&other, 7, 150),
            Err(InstrumentSnapshotError::IdentityMismatch)
        );
        assert_eq!(
            InstrumentSnapshot::new(metadata(8)?, 200, 200),
            Err(InstrumentSnapshotError::InvalidWindow)
        );
        Ok(())
    }
}
