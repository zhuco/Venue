//! Static chart-study metadata used for menu discovery and configuration forms.
//!
//! This registry deliberately describes only the fixed study set implemented by this crate. It
//! does not construct studies dynamically and carries no market or strategy authority.

/// Stable identifier for a first-batch chart study.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ChartIndicatorId {
    Sma,
    Ema,
    Bollinger,
    Vwap,
    Rsi,
    Macd,
    Atr,
}

impl ChartIndicatorId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sma => "sma",
            Self::Ema => "ema",
            Self::Bollinger => "bollinger_bands",
            Self::Vwap => "vwap",
            Self::Rsi => "rsi",
            Self::Macd => "macd",
            Self::Atr => "atr",
        }
    }
}

/// Visual location of a study relative to its source candlestick chart.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChartIndicatorPlacement {
    Overlay,
    Pane,
}

/// Static form metadata. Values remain canonical configuration owned by the caller; this schema
/// only provides display defaults and bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChartParameterDescriptor {
    pub key: &'static str,
    pub name_zh_cn: &'static str,
    pub name_en: &'static str,
    pub default: &'static str,
    pub minimum: Option<&'static str>,
    pub maximum: Option<&'static str>,
    pub unit: &'static str,
}

/// Static, versioned description of one explicitly implemented chart study.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChartIndicatorDescriptor {
    pub id: ChartIndicatorId,
    pub name_zh_cn: &'static str,
    pub name_en: &'static str,
    pub placement: ChartIndicatorPlacement,
    pub input: &'static str,
    pub output: &'static str,
    pub parameters: &'static [ChartParameterDescriptor],
    pub warmup: &'static str,
    pub algorithm_version: &'static str,
}

const PERIOD_20: &[ChartParameterDescriptor] = &[ChartParameterDescriptor {
    key: "period",
    name_zh_cn: "周期",
    name_en: "Period",
    default: "20",
    minimum: Some("1"),
    maximum: Some("100000"),
    unit: "bars",
}];

const PERIOD_14: &[ChartParameterDescriptor] = &[ChartParameterDescriptor {
    key: "period",
    name_zh_cn: "周期",
    name_en: "Period",
    default: "14",
    minimum: Some("1"),
    maximum: Some("100000"),
    unit: "bars",
}];

const BOLLINGER_PARAMETERS: &[ChartParameterDescriptor] = &[
    ChartParameterDescriptor {
        key: "period",
        name_zh_cn: "周期",
        name_en: "Period",
        default: "20",
        minimum: Some("1"),
        maximum: Some("100000"),
        unit: "bars",
    },
    ChartParameterDescriptor {
        key: "multiplier",
        name_zh_cn: "标准差倍数",
        name_en: "Standard deviation multiplier",
        default: "2",
        minimum: Some("0.000001"),
        maximum: Some("1000"),
        unit: "x",
    },
];

const MACD_PARAMETERS: &[ChartParameterDescriptor] = &[
    ChartParameterDescriptor {
        key: "fast_period",
        name_zh_cn: "快速周期",
        name_en: "Fast period",
        default: "12",
        minimum: Some("1"),
        maximum: Some("99999"),
        unit: "bars",
    },
    ChartParameterDescriptor {
        key: "slow_period",
        name_zh_cn: "慢速周期",
        name_en: "Slow period",
        default: "26",
        minimum: Some("2"),
        maximum: Some("100000"),
        unit: "bars",
    },
    ChartParameterDescriptor {
        key: "signal_period",
        name_zh_cn: "信号周期",
        name_en: "Signal period",
        default: "9",
        minimum: Some("1"),
        maximum: Some("100000"),
        unit: "bars",
    },
];

const ALL: &[ChartIndicatorDescriptor] = &[
    ChartIndicatorDescriptor {
        id: ChartIndicatorId::Sma,
        name_zh_cn: "简单移动平均",
        name_en: "Simple Moving Average",
        placement: ChartIndicatorPlacement::Overlay,
        input: "closed_bar.close",
        output: "scalar",
        parameters: PERIOD_20,
        warmup: "period closed bars",
        algorithm_version: "sma-v1",
    },
    ChartIndicatorDescriptor {
        id: ChartIndicatorId::Ema,
        name_zh_cn: "指数移动平均",
        name_en: "Exponential Moving Average",
        placement: ChartIndicatorPlacement::Overlay,
        input: "closed_bar.close",
        output: "scalar",
        parameters: PERIOD_20,
        warmup: "period closed bars",
        algorithm_version: "ema-v1",
    },
    ChartIndicatorDescriptor {
        id: ChartIndicatorId::Bollinger,
        name_zh_cn: "布林带",
        name_en: "Bollinger Bands",
        placement: ChartIndicatorPlacement::Overlay,
        input: "closed_bar.ohlcv",
        output: "upper,middle,lower",
        parameters: BOLLINGER_PARAMETERS,
        warmup: "period closed bars",
        algorithm_version: "bollinger-v1",
    },
    ChartIndicatorDescriptor {
        id: ChartIndicatorId::Vwap,
        name_zh_cn: "成交量加权平均价",
        name_en: "Volume Weighted Average Price",
        placement: ChartIndicatorPlacement::Overlay,
        input: "closed_bar.ohlcv",
        output: "scalar",
        parameters: &[],
        warmup: "first closed bar with positive base volume",
        algorithm_version: "vwap-v1",
    },
    ChartIndicatorDescriptor {
        id: ChartIndicatorId::Rsi,
        name_zh_cn: "相对强弱指数",
        name_en: "Relative Strength Index",
        placement: ChartIndicatorPlacement::Pane,
        input: "closed_bar.close",
        output: "scalar",
        parameters: PERIOD_14,
        warmup: "period + 1 closed bars",
        algorithm_version: "rsi-wilder-v1",
    },
    ChartIndicatorDescriptor {
        id: ChartIndicatorId::Macd,
        name_zh_cn: "指数平滑异同移动平均线",
        name_en: "Moving Average Convergence Divergence",
        placement: ChartIndicatorPlacement::Pane,
        input: "closed_bar.close",
        output: "macd,signal,histogram",
        parameters: MACD_PARAMETERS,
        warmup: "slow_period + signal_period - 1 closed bars",
        algorithm_version: "macd-ema-v1",
    },
    ChartIndicatorDescriptor {
        id: ChartIndicatorId::Atr,
        name_zh_cn: "平均真实波幅",
        name_en: "Average True Range",
        placement: ChartIndicatorPlacement::Pane,
        input: "closed_bar.ohlc",
        output: "scalar",
        parameters: PERIOD_14,
        warmup: "period closed bars",
        algorithm_version: "atr-wilder-v1",
    },
];

/// Static discovery surface for the explicit first-batch chart studies.
pub struct ChartIndicatorRegistry;

impl ChartIndicatorRegistry {
    #[must_use]
    pub const fn all() -> &'static [ChartIndicatorDescriptor] {
        ALL
    }

    #[must_use]
    pub fn find(id: &str) -> Option<&'static ChartIndicatorDescriptor> {
        ALL.iter().find(|descriptor| descriptor.id.as_str() == id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn first_batch_ids_are_unique_and_stable() {
        let descriptors = ChartIndicatorRegistry::all();
        let ids = descriptors
            .iter()
            .map(|descriptor| descriptor.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(descriptors.len(), 7);
        assert_eq!(ids.len(), descriptors.len());
        assert!(ids.contains("sma"));
        assert!(ids.contains("ema"));
        assert!(ids.contains("bollinger_bands"));
        assert!(ids.contains("vwap"));
        assert!(ids.contains("rsi"));
        assert!(ids.contains("macd"));
        assert!(ids.contains("atr"));
    }

    #[test]
    fn registry_finds_exact_stable_ids_only() {
        let macd = ChartIndicatorRegistry::find("macd");
        assert!(matches!(macd, Some(value) if value.id == ChartIndicatorId::Macd));
        assert!(ChartIndicatorRegistry::find("MACD").is_none());
        assert!(ChartIndicatorRegistry::find("unknown").is_none());
    }
}
