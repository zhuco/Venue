use std::{fmt, str::FromStr};

use bitflags::bitflags;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use venue_domain::domain::{MarketKind, Symbol, is_canonical_trading_account_id};

mod capability_promotion;

pub use capability_promotion::{
    CanaryAdmissionReceipt, CapabilityProbeCandidate, CapabilityPromotionError,
    CompleteOrderFamilyEvidence, ControlAppliedReceipt, ControlState, EvidenceCommitment,
    HostAdmissionEvidence, HostAdmittedCapability, MAX_PROMOTION_TTL_MS, OrderFamilyEvidence,
    OrderFamilySupport, OwnerRecoveryReceipt, PromotionScope, WalRecoveryReceipt,
    WriterFenceReceipt, promote_capability,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VenueId {
    Binance,
    Bitget,
    Bybit,
    Gate,
    Hyperliquid,
    Okx,
}

impl VenueId {
    pub const ALL: [Self; 6] = [
        Self::Binance,
        Self::Bitget,
        Self::Bybit,
        Self::Gate,
        Self::Hyperliquid,
        Self::Okx,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bitget => "bitget",
            Self::Bybit => "bybit",
            Self::Gate => "gate",
            Self::Hyperliquid => "hyperliquid",
            Self::Okx => "okx",
        }
    }
}

impl fmt::Display for VenueId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for VenueId {
    type Err = GatewayApiError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "binance" => Ok(Self::Binance),
            "bitget" => Ok(Self::Bitget),
            "bybit" => Ok(Self::Bybit),
            "gate" | "gateio" | "gate.io" => Ok(Self::Gate),
            "hyperliquid" => Ok(Self::Hyperliquid),
            "okx" => Ok(Self::Okx),
            _ => Err(GatewayApiError::Venue),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum GatewayMode {
    #[serde(rename = "TEST")]
    Test,
    #[serde(rename = "LIVE")]
    Live,
}

impl GatewayMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Test => "TEST",
            Self::Live => "LIVE",
        }
    }
}

impl fmt::Display for GatewayMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for GatewayMode {
    type Err = GatewayApiError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim() {
            "TEST" => Ok(Self::Test),
            "LIVE" => Ok(Self::Live),
            _ => Err(GatewayApiError::Mode),
        }
    }
}

/// Secret-free, immutable scope for one fixed gateway process. Endpoint selection and credentials
/// remain adapter-local and are never inferred from the venue or mode at runtime.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatewayBinding {
    pub venue: VenueId,
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub symbol: Symbol,
}

impl GatewayBinding {
    pub fn new(
        venue: VenueId,
        mode: GatewayMode,
        trading_account_id: impl Into<String>,
        symbol: Symbol,
    ) -> Result<Self, GatewayApiError> {
        let trading_account_id = trading_account_id.into();
        if !is_canonical_trading_account_id(&trading_account_id) {
            return Err(GatewayApiError::TradingAccountId);
        }
        Ok(Self {
            venue,
            mode,
            trading_account_id,
            symbol,
        })
    }

    pub fn validate(&self) -> Result<(), GatewayApiError> {
        if is_canonical_trading_account_id(&self.trading_account_id) {
            Ok(())
        } else {
            Err(GatewayApiError::TradingAccountId)
        }
    }
}

/// Secret-free display subscription scope for the first local public-market feed.
///
/// This type is deliberately separate from [`GatewayBinding`]: it never identifies an account,
/// carries no credential material, and cannot be used as mutation or capability evidence. The
/// initial contract is limited to Binance USDⓈ-M public data on the production endpoint.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PublicMarketBinding {
    pub venue: VenueId,
    pub mode: GatewayMode,
    pub market: MarketKind,
    pub symbol: Symbol,
}

impl PublicMarketBinding {
    pub fn binance_usds_m(symbol: Symbol) -> Result<Self, GatewayApiError> {
        Self::new(
            VenueId::Binance,
            GatewayMode::Live,
            MarketKind::LinearPerpetual,
            symbol,
        )
    }

    pub fn new(
        venue: VenueId,
        mode: GatewayMode,
        market: MarketKind,
        symbol: Symbol,
    ) -> Result<Self, GatewayApiError> {
        let value = Self {
            venue,
            mode,
            market,
            symbol,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), GatewayApiError> {
        if self.venue != VenueId::Binance {
            return Err(GatewayApiError::PublicMarketVenue);
        }
        if self.mode != GatewayMode::Live {
            return Err(GatewayApiError::PublicMarketMode);
        }
        if self.market != MarketKind::LinearPerpetual {
            return Err(GatewayApiError::PublicMarketProduct);
        }
        if !matches!(self.symbol.quote(), "USDT" | "USDC") {
            return Err(GatewayApiError::PublicMarketSettlement);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct UncheckedPublicMarketBinding {
    venue: VenueId,
    mode: GatewayMode,
    market: MarketKind,
    symbol: Symbol,
}

impl<'de> Deserialize<'de> for PublicMarketBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = UncheckedPublicMarketBinding::deserialize(deserializer)?;
        Self::new(raw.venue, raw.mode, raw.market, raw.symbol).map_err(D::Error::custom)
    }
}

bitflags! {
    /// Versioned adapter evidence. Unsupported capabilities stay absent; adapters must not fill a
    /// lowest-common-denominator profile or infer trading authority from successful reads.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct CapabilityFlags: u16 {
        const READ_ACCOUNT = 1 << 0;
        const READ_ORDERS = 1 << 1;
        const READ_FILLS = 1 << 2;
        const PRIVATE_STREAM = 1 << 3;
        const TRADE = 1 << 4;
        const WITHDRAW = 1 << 5;
        const PLACE_LIMIT = 1 << 6;
        const PLACE_MARKET = 1 << 7;
        const CANCEL = 1 << 8;
        const AMEND = 1 << 9;
        const HEDGE_POSITION = 1 << 10;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationCapability {
    PlaceLimit,
    PlaceMarket,
    Cancel,
    Amend,
}

impl MutationCapability {
    const fn flag(self) -> CapabilityFlags {
        match self {
            Self::PlaceLimit => CapabilityFlags::PLACE_LIMIT,
            Self::PlaceMarket => CapabilityFlags::PLACE_MARKET,
            Self::Cancel => CapabilityFlags::CANCEL,
            Self::Amend => CapabilityFlags::AMEND,
        }
    }
}

/// Account-, symbol-, mode-, and version-bound proof used at the physical mutation boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapabilitySnapshot {
    pub binding: GatewayBinding,
    pub version: u64,
    pub observed_ms: u64,
    pub expires_ms: u64,
    pub flags: CapabilityFlags,
}

impl CapabilitySnapshot {
    pub fn authorize(
        &self,
        expected_binding: &GatewayBinding,
        expected_version: u64,
        now_ms: u64,
        mutation: MutationCapability,
    ) -> Result<(), GatewayApiError> {
        self.binding.validate()?;
        expected_binding.validate()?;
        if &self.binding != expected_binding
            || self.version == 0
            || self.version != expected_version
        {
            return Err(GatewayApiError::CapabilityScope);
        }
        if self.observed_ms == 0
            || self.expires_ms <= self.observed_ms
            || now_ms < self.observed_ms
            || now_ms >= self.expires_ms
        {
            return Err(GatewayApiError::CapabilityFreshness);
        }
        let required_reads = CapabilityFlags::READ_ACCOUNT
            | CapabilityFlags::READ_ORDERS
            | CapabilityFlags::READ_FILLS
            | CapabilityFlags::PRIVATE_STREAM;
        if !self.flags.contains(required_reads)
            || !self.flags.contains(CapabilityFlags::TRADE)
            || self.flags.contains(CapabilityFlags::WITHDRAW)
            || !self.flags.contains(mutation.flag())
        {
            return Err(GatewayApiError::CapabilityDenied);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GatewayApiError {
    #[error("venue must be Binance, Bitget, Bybit, Gate.io, Hyperliquid, or OKX")]
    Venue,
    #[error("gateway mode must be exactly TEST or LIVE")]
    Mode,
    #[error("trading account id must be a canonical UUID string")]
    TradingAccountId,
    #[error("local public market data currently supports Binance only")]
    PublicMarketVenue,
    #[error("local public market data currently supports LIVE only")]
    PublicMarketMode,
    #[error("local public market data currently supports Binance USDⓈ-M perpetuals only")]
    PublicMarketProduct,
    #[error("Binance USDⓈ-M public market symbols must settle in USDT or USDC")]
    PublicMarketSettlement,
    #[error("gateway capability scope or version does not match the mutation")]
    CapabilityScope,
    #[error("gateway capability evidence is stale, future-dated, or malformed")]
    CapabilityFreshness,
    #[error("gateway capability evidence does not authorize this mutation")]
    CapabilityDenied,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn venue_set_is_exactly_the_six_approved_adapters() {
        assert_eq!(
            VenueId::ALL.map(VenueId::as_str),
            ["binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
        );
    }

    #[test]
    fn gateway_mode_rejects_shadow_and_implicit_case_variants() {
        assert_eq!("TEST".parse(), Ok(GatewayMode::Test));
        assert_eq!("LIVE".parse(), Ok(GatewayMode::Live));
        assert!("Shadow".parse::<GatewayMode>().is_err());
        assert!("live".parse::<GatewayMode>().is_err());
        assert!("".parse::<GatewayMode>().is_err());
    }

    #[test]
    fn serde_preserves_the_explicit_mode_boundary() -> Result<(), serde_json::Error> {
        assert_eq!(serde_json::to_string(&GatewayMode::Test)?, "\"TEST\"");
        assert_eq!(
            serde_json::from_str::<GatewayMode>("\"LIVE\"")?,
            GatewayMode::Live
        );
        assert!(serde_json::from_str::<GatewayMode>("\"SHADOW\"").is_err());
        Ok(())
    }

    fn binding(venue: VenueId) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            venue,
            GatewayMode::Live,
            "00000000-0000-0000-0000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    fn executable_snapshot() -> Result<CapabilitySnapshot, Box<dyn std::error::Error>> {
        Ok(CapabilitySnapshot {
            binding: binding(VenueId::Bybit)?,
            version: 7,
            observed_ms: 1_000,
            expires_ms: 2_000,
            flags: CapabilityFlags::READ_ACCOUNT
                | CapabilityFlags::READ_ORDERS
                | CapabilityFlags::READ_FILLS
                | CapabilityFlags::PRIVATE_STREAM
                | CapabilityFlags::TRADE
                | CapabilityFlags::PLACE_LIMIT
                | CapabilityFlags::CANCEL,
        })
    }

    #[test]
    fn binding_requires_a_canonical_account_id() -> Result<(), Box<dyn std::error::Error>> {
        let symbol = "BTC/USDT".parse()?;
        assert_eq!(
            GatewayBinding::new(VenueId::Okx, GatewayMode::Test, "account-name", symbol),
            Err(GatewayApiError::TradingAccountId)
        );
        Ok(())
    }

    #[test]
    fn public_market_binding_accepts_only_binance_live_usds_m()
    -> Result<(), Box<dyn std::error::Error>> {
        for symbol in ["BTC/USDT", "SOL/USDC"] {
            assert_eq!(
                PublicMarketBinding::binance_usds_m(symbol.parse()?)?,
                PublicMarketBinding::new(
                    VenueId::Binance,
                    GatewayMode::Live,
                    MarketKind::LinearPerpetual,
                    symbol.parse()?,
                )?
            );
        }
        Ok(())
    }

    #[test]
    fn public_market_binding_rejects_out_of_scope_venue_product_and_settlement()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "BTC/USDT".parse()?;
        assert_eq!(
            PublicMarketBinding::new(
                VenueId::Okx,
                GatewayMode::Live,
                MarketKind::LinearPerpetual,
                symbol.clone(),
            ),
            Err(GatewayApiError::PublicMarketVenue)
        );
        assert_eq!(
            PublicMarketBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                MarketKind::Spot,
                symbol,
            ),
            Err(GatewayApiError::PublicMarketProduct)
        );
        assert_eq!(
            PublicMarketBinding::binance_usds_m("BTC/USD".parse()?),
            Err(GatewayApiError::PublicMarketSettlement)
        );
        Ok(())
    }

    #[test]
    fn public_market_binding_deserialization_revalidates_the_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let decoded: PublicMarketBinding = serde_json::from_str(
            r#"{"venue":"binance","mode":"LIVE","market":"linear_perpetual","symbol":"BTC/USDT"}"#,
        )?;
        assert_eq!(
            decoded,
            PublicMarketBinding::binance_usds_m("BTC/USDT".parse()?)?
        );

        for raw in [
            r#"{"venue":"okx","mode":"LIVE","market":"linear_perpetual","symbol":"BTC/USDT"}"#,
            r#"{"venue":"binance","mode":"TEST","market":"linear_perpetual","symbol":"BTC/USDT"}"#,
            r#"{"venue":"binance","mode":"LIVE","market":"spot","symbol":"BTC/USDT"}"#,
            r#"{"venue":"binance","mode":"LIVE","market":"linear_perpetual","symbol":"BTC/USD"}"#,
        ] {
            assert!(serde_json::from_str::<PublicMarketBinding>(raw).is_err());
        }
        Ok(())
    }

    #[test]
    fn public_market_binding_serialization_has_no_account_credential_or_capability_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = PublicMarketBinding::binance_usds_m("BTC/USDT".parse()?)?;
        let encoded = serde_json::to_value(value)?;
        let fields = encoded
            .as_object()
            .ok_or("public market binding must serialize to an object")?;
        assert_eq!(fields.len(), 4);
        for field in ["venue", "mode", "market", "symbol"] {
            assert!(fields.contains_key(field));
        }
        for field in [
            "trading_account_id",
            "account",
            "credentials",
            "capabilities",
            "flags",
        ] {
            assert!(!fields.contains_key(field));
        }
        Ok(())
    }

    #[test]
    fn capability_authority_is_exact_fresh_and_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = executable_snapshot()?;
        assert_eq!(
            snapshot.authorize(&snapshot.binding, 7, 1_999, MutationCapability::PlaceLimit),
            Ok(())
        );
        assert_eq!(
            snapshot.authorize(&snapshot.binding, 7, 2_000, MutationCapability::PlaceLimit),
            Err(GatewayApiError::CapabilityFreshness)
        );
        assert_eq!(
            snapshot.authorize(&snapshot.binding, 8, 1_500, MutationCapability::PlaceLimit),
            Err(GatewayApiError::CapabilityScope)
        );
        assert_eq!(
            snapshot.authorize(
                &binding(VenueId::Okx)?,
                7,
                1_500,
                MutationCapability::PlaceLimit
            ),
            Err(GatewayApiError::CapabilityScope)
        );
        assert_eq!(
            snapshot.authorize(&snapshot.binding, 7, 1_500, MutationCapability::Amend),
            Err(GatewayApiError::CapabilityDenied)
        );
        Ok(())
    }

    #[test]
    fn withdrawal_permission_invalidates_mutation_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut snapshot = executable_snapshot()?;
        snapshot.flags.insert(CapabilityFlags::WITHDRAW);
        assert_eq!(
            snapshot.authorize(&snapshot.binding, 7, 1_500, MutationCapability::Cancel),
            Err(GatewayApiError::CapabilityDenied)
        );
        Ok(())
    }
}
