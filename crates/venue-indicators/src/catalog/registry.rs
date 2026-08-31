#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndicatorCategory {
    Trend,
    Momentum,
    Volatility,
    Volume,
    PriceChannel,
    Statistics,
    TradeFlow,
    BookMicrostructure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndicatorInput {
    ClosedBar,
    ScalarPair,
    PublicTrade,
    PublicBook,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndicatorDescriptor {
    pub id: &'static str,
    pub name_zh_cn: &'static str,
    pub name_en: &'static str,
    pub category: IndicatorCategory,
    pub input: IndicatorInput,
    pub algorithm_version: &'static str,
}

macro_rules! descriptor {
    ($id:literal, $zh:literal, $en:literal, $category:ident, $input:ident) => {
        IndicatorDescriptor {
            id: $id,
            name_zh_cn: $zh,
            name_en: $en,
            category: IndicatorCategory::$category,
            input: IndicatorInput::$input,
            algorithm_version: concat!($id, "-venue-pulse-v1"),
        }
    };
}

const ALL: &[IndicatorDescriptor] = &[
    descriptor!(
        "sma",
        "简单移动平均",
        "Simple Moving Average",
        Trend,
        ClosedBar
    ),
    descriptor!(
        "ema",
        "指数移动平均",
        "Exponential Moving Average",
        Trend,
        ClosedBar
    ),
    descriptor!(
        "rma",
        "威尔德移动平均",
        "Wilder Moving Average",
        Trend,
        ClosedBar
    ),
    descriptor!(
        "wma",
        "加权移动平均",
        "Weighted Moving Average",
        Trend,
        ClosedBar
    ),
    descriptor!(
        "dema",
        "双指数移动平均",
        "Double Exponential Moving Average",
        Trend,
        ClosedBar
    ),
    descriptor!(
        "tema",
        "三指数移动平均",
        "Triple Exponential Moving Average",
        Trend,
        ClosedBar
    ),
    descriptor!(
        "hma",
        "赫尔移动平均",
        "Hull Moving Average",
        Trend,
        ClosedBar
    ),
    descriptor!(
        "kama",
        "考夫曼自适应均线",
        "Kaufman Adaptive Moving Average",
        Trend,
        ClosedBar
    ),
    descriptor!("zlema", "零延迟指数均线", "Zero Lag EMA", Trend, ClosedBar),
    descriptor!("aroon", "阿隆指标", "Aroon", Trend, ClosedBar),
    descriptor!(
        "dmi",
        "趋向指标",
        "Directional Movement Index",
        Trend,
        ClosedBar
    ),
    descriptor!("supertrend", "超级趋势", "SuperTrend", Trend, ClosedBar),
    descriptor!("trix", "三重指数平滑指标", "TRIX", Trend, ClosedBar),
    descriptor!(
        "parabolic_sar",
        "抛物线转向",
        "Parabolic SAR",
        Trend,
        ClosedBar
    ),
    descriptor!("momentum", "动量", "Momentum", Momentum, ClosedBar),
    descriptor!("roc", "变动率", "Rate of Change", Momentum, ClosedBar),
    descriptor!(
        "rsi",
        "相对强弱指数",
        "Relative Strength Index",
        Momentum,
        ClosedBar
    ),
    descriptor!("stochastic", "随机指标", "Stochastic", Momentum, ClosedBar),
    descriptor!(
        "stoch_rsi",
        "随机相对强弱",
        "Stochastic RSI",
        Momentum,
        ClosedBar
    ),
    descriptor!(
        "cci",
        "顺势指标",
        "Commodity Channel Index",
        Momentum,
        ClosedBar
    ),
    descriptor!("williams_r", "威廉指标", "Williams %R", Momentum, ClosedBar),
    descriptor!(
        "mfi",
        "资金流量指标",
        "Money Flow Index",
        Momentum,
        ClosedBar
    ),
    descriptor!(
        "tsi",
        "真实强弱指数",
        "True Strength Index",
        Momentum,
        ClosedBar
    ),
    descriptor!("macd", "指数平滑异同移动平均", "MACD", Momentum, ClosedBar),
    descriptor!(
        "awesome_oscillator",
        "动量震荡指标",
        "Awesome Oscillator",
        Momentum,
        ClosedBar
    ),
    descriptor!(
        "true_range",
        "真实波幅",
        "True Range",
        Volatility,
        ClosedBar
    ),
    descriptor!(
        "atr",
        "平均真实波幅",
        "Average True Range",
        Volatility,
        ClosedBar
    ),
    descriptor!(
        "natr",
        "归一化平均真实波幅",
        "Normalized ATR",
        Volatility,
        ClosedBar
    ),
    descriptor!(
        "rolling_stddev",
        "滚动标准差",
        "Rolling Standard Deviation",
        Volatility,
        ClosedBar
    ),
    descriptor!(
        "historical_volatility",
        "历史波动率",
        "Historical Volatility",
        Volatility,
        ClosedBar
    ),
    descriptor!(
        "bollinger_bands",
        "布林带",
        "Bollinger Bands",
        Volatility,
        ClosedBar
    ),
    descriptor!(
        "bollinger_bandwidth",
        "布林带宽",
        "Bollinger Bandwidth",
        Volatility,
        ClosedBar
    ),
    descriptor!(
        "keltner_channel",
        "肯特纳通道",
        "Keltner Channel",
        Volatility,
        ClosedBar
    ),
    descriptor!(
        "choppiness_index",
        "震荡指数",
        "Choppiness Index",
        Volatility,
        ClosedBar
    ),
    descriptor!("obv", "能量潮", "On Balance Volume", Volume, ClosedBar),
    descriptor!(
        "adl",
        "累积派发线",
        "Accumulation Distribution Line",
        Volume,
        ClosedBar
    ),
    descriptor!("cmf", "蔡金资金流", "Chaikin Money Flow", Volume, ClosedBar),
    descriptor!(
        "chaikin_oscillator",
        "蔡金震荡指标",
        "Chaikin Oscillator",
        Volume,
        ClosedBar
    ),
    descriptor!("pvt", "价量趋势", "Price Volume Trend", Volume, ClosedBar),
    descriptor!("vwap", "成交量加权平均价", "VWAP", Volume, ClosedBar),
    descriptor!("vwma", "成交量加权移动平均", "VWMA", Volume, ClosedBar),
    descriptor!("force_index", "强力指数", "Force Index", Volume, ClosedBar),
    descriptor!(
        "volume_oscillator",
        "成交量震荡指标",
        "Volume Oscillator",
        Volume,
        ClosedBar
    ),
    descriptor!(
        "average_value_line",
        "均价线",
        "Average Value Line",
        Volume,
        ClosedBar
    ),
    descriptor!(
        "ease_of_movement",
        "简易波动指标",
        "Ease of Movement",
        Volume,
        ClosedBar
    ),
    descriptor!(
        "typical_price",
        "典型价格",
        "Typical Price",
        PriceChannel,
        ClosedBar
    ),
    descriptor!(
        "median_price",
        "中间价格",
        "Median Price",
        PriceChannel,
        ClosedBar
    ),
    descriptor!(
        "weighted_close",
        "加权收盘价",
        "Weighted Close",
        PriceChannel,
        ClosedBar
    ),
    descriptor!(
        "highest_high",
        "滚动最高价",
        "Highest High",
        PriceChannel,
        ClosedBar
    ),
    descriptor!(
        "lowest_low",
        "滚动最低价",
        "Lowest Low",
        PriceChannel,
        ClosedBar
    ),
    descriptor!("midpoint", "通道中点", "Midpoint", PriceChannel, ClosedBar),
    descriptor!(
        "donchian_channel",
        "唐奇安通道",
        "Donchian Channel",
        PriceChannel,
        ClosedBar
    ),
    descriptor!(
        "pivot_points",
        "枢轴点",
        "Pivot Points",
        PriceChannel,
        ClosedBar
    ),
    descriptor!("zscore", "标准分数", "Z-Score", Statistics, ClosedBar),
    descriptor!(
        "linear_regression",
        "线性回归",
        "Linear Regression",
        Statistics,
        ClosedBar
    ),
    descriptor!(
        "pearson_correlation",
        "皮尔逊相关系数",
        "Pearson Correlation",
        Statistics,
        ScalarPair
    ),
    descriptor!(
        "efficiency_ratio",
        "效率比率",
        "Efficiency Ratio",
        Statistics,
        ClosedBar
    ),
    descriptor!(
        "mean_absolute_deviation",
        "平均绝对偏差",
        "Mean Absolute Deviation",
        Statistics,
        ClosedBar
    ),
    descriptor!(
        "coefficient_of_variation",
        "变异系数",
        "Coefficient of Variation",
        Statistics,
        ClosedBar
    ),
    descriptor!(
        "autocorrelation",
        "自相关",
        "Autocorrelation",
        Statistics,
        ClosedBar
    ),
    descriptor!(
        "volume_delta",
        "成交量差",
        "Volume Delta",
        TradeFlow,
        PublicTrade
    ),
    descriptor!(
        "cumulative_volume_delta",
        "累计成交量差",
        "Cumulative Volume Delta",
        TradeFlow,
        PublicTrade
    ),
    descriptor!(
        "aggressor_ratio",
        "主动买入比",
        "Aggressor Ratio",
        TradeFlow,
        PublicTrade
    ),
    descriptor!(
        "trade_imbalance",
        "成交不平衡",
        "Trade Imbalance",
        TradeFlow,
        PublicTrade
    ),
    descriptor!(
        "trade_intensity",
        "成交强度",
        "Trade Intensity",
        TradeFlow,
        PublicTrade
    ),
    descriptor!(
        "average_trade_size",
        "平均成交量",
        "Average Trade Size",
        TradeFlow,
        PublicTrade
    ),
    descriptor!(
        "large_trade_ratio",
        "大单占比",
        "Large Trade Ratio",
        TradeFlow,
        PublicTrade
    ),
    descriptor!(
        "signed_notional",
        "主动成交净额",
        "Signed Notional",
        TradeFlow,
        PublicTrade
    ),
    descriptor!(
        "spread",
        "买卖价差",
        "Spread",
        BookMicrostructure,
        PublicBook
    ),
    descriptor!(
        "mid_price",
        "盘口中间价",
        "Mid Price",
        BookMicrostructure,
        PublicBook
    ),
    descriptor!(
        "weighted_mid",
        "加权中间价",
        "Weighted Mid",
        BookMicrostructure,
        PublicBook
    ),
    descriptor!(
        "microprice",
        "微观价格",
        "Microprice",
        BookMicrostructure,
        PublicBook
    ),
    descriptor!(
        "book_imbalance",
        "订单簿不平衡",
        "Book Imbalance",
        BookMicrostructure,
        PublicBook
    ),
    descriptor!(
        "depth_weighted_imbalance",
        "深度加权不平衡",
        "Depth Weighted Imbalance",
        BookMicrostructure,
        PublicBook
    ),
    descriptor!(
        "depth_slope",
        "深度斜率",
        "Depth Slope",
        BookMicrostructure,
        PublicBook
    ),
    descriptor!(
        "book_order_flow_imbalance",
        "盘口订单流不平衡",
        "Book Order Flow Imbalance",
        BookMicrostructure,
        PublicBook
    ),
];

pub struct IndicatorCatalog;

impl IndicatorCatalog {
    #[must_use]
    pub const fn all() -> &'static [IndicatorDescriptor] {
        ALL
    }

    #[must_use]
    pub fn find(id: &str) -> Option<&'static IndicatorDescriptor> {
        ALL.iter().find(|descriptor| descriptor.id == id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::IndicatorCatalog;

    #[test]
    fn migrated_catalog_has_all_unique_venue_pulse_indicators() {
        let descriptors = IndicatorCatalog::all();
        let unique = descriptors
            .iter()
            .map(|descriptor| descriptor.id)
            .collect::<BTreeSet<_>>();
        assert_eq!(descriptors.len(), 76);
        assert_eq!(unique.len(), descriptors.len());
    }
}
