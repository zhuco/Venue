//! Stable discovery metadata for every study currently exposed by VenueFlow.

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ChartIndicatorId {
    Ma,
    Ema,
    Wma,
    Bollinger,
    Vwap,
    Avl,
    Trix,
    Sar,
    Supertrend,
    Volume,
    Macd,
    Rsi,
    Mfi,
    Kdj,
    Obv,
    Cci,
    StochRsi,
    WilliamsR,
    Dmi,
    Momentum,
    Emv,
    Atr,
}

impl ChartIndicatorId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ma => "ma",
            Self::Ema => "ema",
            Self::Wma => "wma",
            Self::Bollinger => "bollinger_bands",
            Self::Vwap => "vwap",
            Self::Avl => "avl",
            Self::Trix => "trix",
            Self::Sar => "parabolic_sar",
            Self::Supertrend => "supertrend",
            Self::Volume => "volume",
            Self::Macd => "macd",
            Self::Rsi => "rsi",
            Self::Mfi => "mfi",
            Self::Kdj => "kdj",
            Self::Obv => "obv",
            Self::Cci => "cci",
            Self::StochRsi => "stoch_rsi",
            Self::WilliamsR => "williams_r",
            Self::Dmi => "dmi",
            Self::Momentum => "momentum",
            Self::Emv => "emv",
            Self::Atr => "atr",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChartIndicatorPlacement {
    Overlay,
    Pane,
}

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

const PERIOD: &[ChartParameterDescriptor] = &[ChartParameterDescriptor {
    key: "period",
    name_zh_cn: "周期",
    name_en: "Period",
    default: "14",
    minimum: Some("1"),
    maximum: Some("100000"),
    unit: "bars",
}];

const THREE_PERIODS: &[ChartParameterDescriptor] = &[
    ChartParameterDescriptor {
        key: "first_period",
        name_zh_cn: "周期1",
        name_en: "Period 1",
        default: "7",
        minimum: Some("1"),
        maximum: Some("100000"),
        unit: "bars",
    },
    ChartParameterDescriptor {
        key: "second_period",
        name_zh_cn: "周期2",
        name_en: "Period 2",
        default: "25",
        minimum: Some("1"),
        maximum: Some("100000"),
        unit: "bars",
    },
    ChartParameterDescriptor {
        key: "third_period",
        name_zh_cn: "周期3",
        name_en: "Period 3",
        default: "99",
        minimum: Some("1"),
        maximum: Some("100000"),
        unit: "bars",
    },
];

const MACD: &[ChartParameterDescriptor] = &[
    ChartParameterDescriptor {
        key: "fast_period",
        name_zh_cn: "快线周期",
        name_en: "Fast period",
        default: "12",
        minimum: Some("1"),
        maximum: Some("99999"),
        unit: "bars",
    },
    ChartParameterDescriptor {
        key: "slow_period",
        name_zh_cn: "慢线周期",
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

macro_rules! study {
    ($id:ident, $zh:literal, $en:literal, $place:ident, $input:literal, $output:literal, $params:expr, $version:literal) => {
        ChartIndicatorDescriptor {
            id: ChartIndicatorId::$id,
            name_zh_cn: $zh,
            name_en: $en,
            placement: ChartIndicatorPlacement::$place,
            input: $input,
            output: $output,
            parameters: $params,
            warmup: "parameter-dependent closed bars",
            algorithm_version: $version,
        }
    };
}

const ALL: &[ChartIndicatorDescriptor] = &[
    study!(
        Ma,
        "移动平均线",
        "Moving Average",
        Overlay,
        "closed_bar.close",
        "three lines",
        THREE_PERIODS,
        "ma-v2"
    ),
    study!(
        Ema,
        "指数移动平均线",
        "Exponential Moving Average",
        Overlay,
        "closed_bar.close",
        "three lines",
        THREE_PERIODS,
        "ema-v2"
    ),
    study!(
        Wma,
        "加权移动平均线",
        "Weighted Moving Average",
        Overlay,
        "closed_bar.close",
        "three lines",
        THREE_PERIODS,
        "wma-v1"
    ),
    study!(
        Bollinger,
        "布林带",
        "Bollinger Bands",
        Overlay,
        "closed_bar.ohlc",
        "upper,middle,lower",
        PERIOD,
        "bollinger-v1"
    ),
    study!(
        Vwap,
        "成交量加权均价",
        "VWAP",
        Overlay,
        "closed_bar.ohlcv",
        "line",
        &[],
        "vwap-v1"
    ),
    study!(
        Avl,
        "均价线",
        "Average Value Line",
        Overlay,
        "closed_bar.base_quote_volume",
        "line",
        &[],
        "avl-v1"
    ),
    study!(
        Trix,
        "三重指数平滑",
        "TRIX",
        Overlay,
        "closed_bar.close",
        "line,rate",
        PERIOD,
        "trix-v1"
    ),
    study!(
        Sar,
        "抛物线转向",
        "Parabolic SAR",
        Overlay,
        "closed_bar.ohlc",
        "value,direction",
        &[],
        "sar-v1"
    ),
    study!(
        Supertrend,
        "超级趋势",
        "SuperTrend",
        Overlay,
        "closed_bar.ohlc",
        "value,direction",
        PERIOD,
        "supertrend-v1"
    ),
    study!(
        Volume,
        "成交量",
        "Volume",
        Pane,
        "closed_bar.volume",
        "histogram",
        &[],
        "volume-v1"
    ),
    study!(
        Macd,
        "指数平滑异同移动平均",
        "MACD",
        Pane,
        "closed_bar.close",
        "macd,signal,histogram",
        MACD,
        "macd-v1"
    ),
    study!(
        Rsi,
        "相对强弱指标",
        "RSI",
        Pane,
        "closed_bar.close",
        "line",
        PERIOD,
        "rsi-v1"
    ),
    study!(
        Mfi,
        "资金流量指标",
        "MFI",
        Pane,
        "closed_bar.ohlcv",
        "line",
        PERIOD,
        "mfi-v1"
    ),
    study!(
        Kdj,
        "随机指标",
        "KDJ",
        Pane,
        "closed_bar.ohlc",
        "k,d,j",
        PERIOD,
        "kdj-v1"
    ),
    study!(
        Obv,
        "能量潮",
        "OBV",
        Pane,
        "closed_bar.close_volume",
        "line",
        &[],
        "obv-v1"
    ),
    study!(
        Cci,
        "顺势指标",
        "CCI",
        Pane,
        "closed_bar.ohlc",
        "line",
        PERIOD,
        "cci-v1"
    ),
    study!(
        StochRsi,
        "随机相对强弱",
        "Stochastic RSI",
        Pane,
        "closed_bar.close",
        "k,d",
        PERIOD,
        "stoch-rsi-v1"
    ),
    study!(
        WilliamsR,
        "威廉指标",
        "Williams %R",
        Pane,
        "closed_bar.ohlc",
        "line",
        PERIOD,
        "williams-r-v1"
    ),
    study!(
        Dmi,
        "趋向指标",
        "DMI",
        Pane,
        "closed_bar.ohlc",
        "plus_di,minus_di,adx",
        PERIOD,
        "dmi-v1"
    ),
    study!(
        Momentum,
        "动量",
        "Momentum",
        Pane,
        "closed_bar.close",
        "line",
        PERIOD,
        "momentum-v1"
    ),
    study!(
        Emv,
        "简易波动指标",
        "Ease of Movement",
        Pane,
        "closed_bar.ohlcv",
        "line",
        PERIOD,
        "emv-v1"
    ),
    study!(
        Atr,
        "平均真实波幅",
        "ATR",
        Pane,
        "closed_bar.ohlc",
        "line",
        PERIOD,
        "atr-v1"
    ),
];

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

    use super::{ChartIndicatorId, ChartIndicatorRegistry};

    #[test]
    fn commercial_ui_indicator_ids_are_complete_and_unique() {
        let descriptors = ChartIndicatorRegistry::all();
        let ids = descriptors
            .iter()
            .map(|value| value.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(descriptors.len(), 22);
        assert_eq!(ids.len(), descriptors.len());
        assert!(ids.contains("supertrend"));
        assert!(ids.contains("stoch_rsi"));
    }

    #[test]
    fn registry_find_is_exact_and_case_sensitive() {
        assert!(
            matches!(ChartIndicatorRegistry::find("macd"), Some(value) if value.id == ChartIndicatorId::Macd)
        );
        assert!(ChartIndicatorRegistry::find("MACD").is_none());
    }
}
