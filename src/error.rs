use std::{io, path::PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cannot read config {path}: {source}", path = path.display())]
    Read { path: PathBuf, source: io::Error },

    #[error("invalid config {path}: {source}", path = path.display())]
    Config {
        path: PathBuf,
        source: toml::de::Error,
    },

    #[error("Binance private custody freshness must be between {min}ms and {max}ms, got {value}ms")]
    PrivateCustodyFreshness { value: u64, min: u64, max: u64 },

    #[error("hedged-grid grid_count must be between {min} and {max}, got {value}")]
    HedgedGridCount { value: u8, min: u8, max: u8 },

    #[error("hedged-grid exposure take-profit parameters differ from the fixed release")]
    HedgedGridExposureRelease,

    #[error("exactly one enabled exchange configuration is required")]
    ExchangeConfiguration,

    #[error("trading_account_id must be one canonical 36-character UUID")]
    TradingAccountId,

    #[error("hedged-grid deployment binary for {expected} cannot load {actual} configuration")]
    HedgedGridDeploymentExchange {
        expected: &'static str,
        actual: &'static str,
    },

    #[error("command is not available in a hedged-grid deployment binary")]
    HedgedGridDeploymentCommand,

    #[error("cannot initialize logging: {0}")]
    Log(String),

    #[error("Binance private credential or transport check failed: {0}")]
    Private(#[from] crate::exchange::binance::PrivateError),

    #[error("Binance private readback check failed: {0}")]
    PrivateReadback(#[from] crate::exchange::binance::PrivateReadbackError),

    #[error("Binance public capability check failed: {0}")]
    Public(#[from] crate::exchange::binance::PublicError),

    #[error("Binance public capability payload failed validation: {0}")]
    Binance(#[from] crate::exchange::binance::BinanceError),

    #[error("Gate.io adapter operation failed: {0}")]
    Gate(#[from] crate::exchange::gate::GateError),

    #[error("Bitget adapter operation failed: {0}")]
    Bitget(#[from] crate::exchange::bitget::BitgetError),

    #[error("Binance market scan failed closed: {0}")]
    BinanceMarketScan(#[from] crate::runtime::BinanceMarketScanError),

    #[error("Binance automatic resident failed closed: {0}")]
    BinanceAutoShadow(#[from] crate::runtime::BinanceAutoShadowError),

    #[error("capability evidence failed: {0}")]
    CapabilityEvidence(#[from] crate::execution::CapabilityEvidenceError),

    #[error("system clock is unavailable for capability evidence")]
    CapabilityEvidenceClock,

    #[error("raw market replay input failed validation: {0}")]
    RawMarket(#[from] crate::market::RawError),

    #[error("scalping Shadow evidence input failed validation: {0}")]
    ScalpingEvidence(#[from] crate::storage::ScalpingEvidenceError),

    #[error("scalping Shadow replay failed: {0}")]
    ScalpingShadow(#[from] crate::runtime::ShadowReplayError),

    #[error("scalping resident failed closed: {0}")]
    ScalpingShadowResident(#[from] crate::runtime::ScalpingShadowResidentError),

    #[error("scalping Core file-only ingress failed closed: {0}")]
    ScalpingCoreIngress(#[from] crate::runtime::ScalpingCoreIngressError),

    #[error("scalping controller command failed closed: {0}")]
    ScalpingControl(#[from] crate::runtime::ScalpingControlError),

    #[error("fixed Shadow replay binding is invalid")]
    ShadowBinding,

    #[error("doctor --stream requires --private")]
    PrivateStreamRequiresPrivate,

    #[error("command `{cmd}` is disabled in the current migration phase")]
    Disabled { cmd: &'static str },

    #[error("real Binance Canary requires --confirm-mainnet-real-orders")]
    CanaryConfirmation,

    #[error("Binance Canary failed closed: {0}")]
    Canary(#[from] crate::runtime::BinanceCanaryError),

    #[error("signed Binance Canary recovery requires --confirm-mainnet-private-readback")]
    CanaryRecoveryConfirmation,

    #[error("Binance Canary recovery failed closed: {0}")]
    CanaryRecovery(#[from] crate::runtime::BinanceCanaryRecoveryError),

    #[error("hedged-grid runtime failed closed: {0}")]
    HedgedGridLive(#[from] crate::runtime::HedgedGridLiveError),

    #[error("stage-7 exchange hedged-grid runtime failed closed: {0}")]
    Stage7Grid(#[from] crate::runtime::Stage7GridError),
}

pub type Result<T> = std::result::Result<T, Error>;
