use crate::i18n::Language;

macro_rules! resources {
    ($($key:ident => $en:literal, $zh:literal;)+) => {
        #[derive(Clone, Copy)]
        pub(super) enum Key { $($key,)+ }
        pub(super) fn text(language: Language, key: Key) -> &'static str {
            match (language, key) {
                $((Language::English, Key::$key) => $en,)+
                $((Language::SimplifiedChinese, Key::$key) => $zh,)+
            }
        }
    }
}

resources! {
    Title => "EMA + ADX + MACD · ATR", "EMA＋ADX＋MACD · ATR归一化";
    Enabled => "Enable this study", "启用此指标";
    Implementation => "Rust translation · AIScript source reference (not a script interpreter)", "Rust 移植版 · 附 AIScript 原稿（非脚本解释器）";
    Boundary => "Local public bars only. Virtual signals do not place orders or read account positions.", "仅计算本地公共 K 线；虚拟信号不下单，也不读取账户持仓。";
    Compatibility => "Preserves raw-signal cooldown. Positive cooldown can suppress entries; START is not ENTRY. AiCoin bar-by-bar parity is not yet verified.", "保留原稿的原始信号冷却；正冷却可能阻止开仓，启动不等于开仓。尚未经 AiCoin 逐根校准。";
    Trend => "Trend parameters", "趋势参数";
    Fast => "Fast EMA", "EMA 快线";
    Mid => "Middle EMA", "EMA 中线";
    Slow => "Slow EMA", "EMA 慢线";
    Di => "DI period", "DI 周期";
    Adx => "ADX smoothing", "ADX 平滑周期";
    Minimum => "Minimum ADX", "最低趋势 ADX";
    MacdFast => "MACD fast", "MACD 快线";
    MacdSlow => "MACD slow", "MACD 慢线";
    MacdSignal => "MACD signal", "MACD 信号线";
    Atr => "ATR period (SMA of TR)", "ATR 周期（TR 简单均值）";
    Filters => "Filters and cooldown", "过滤与冷却";
    Range => "Bar range / ATR", "单K振幅 / ATR";
    Histogram => "MACD threshold / ATR", "MACD 阈值 / ATR";
    Exit => "Exit buffer / ATR", "平仓缓冲 / ATR";
    Breakout => "Breakout buffer / ATR", "突破缓冲 / ATR";
    Lookback => "Breakout lookback", "突破回看K线数";
    Distance => "Cooldown distance / ATR", "冷却价格距离 / ATR";
    Bars => "Minimum bars between", "开仓最少间隔K线数";
    VolumeFilter => "Volume filter", "启用成交量过滤";
    VolumePeriod => "Volume average period", "成交量均线周期";
    VolumeMultiple => "Volume multiple", "成交量放大倍数";
    Display => "Lines and signal labels", "线条与信号标签";
    BullColor => "Fast / bullish", "快线／多头颜色";
    BearColor => "Middle / bearish", "中线／空头颜色";
    NeutralColor => "Neutral slow EMA", "慢线中性颜色";
    ConfirmedOnly => "Only confirmed signal labels (lines preview live)", "只显示收盘确认标签（线条仍实时预览）";
    Source => "View original AIScript", "查看 AIScript 原稿";
    Long => "LONG", "开多";
    Short => "SHORT", "开空";
    LongExit => "EXIT LONG", "多平";
    ShortExit => "EXIT SHORT", "空平";
    BullStart => "BULL START", "多启动";
    BearStart => "BEAR START", "空启动";
    Preview => "preview", "预览";
    Confirmed => "confirmed", "已确认";
    Warmup => "warming up", "预热中";
    VirtualPosition => "virtual position", "虚拟状态";
    Restore => "Reset this custom study", "恢复此指标默认参数";
    WebUnavailable => "Local indicator calculation is available in the native desktop only.", "本地指标计算目前仅在原生桌面提供。";
}

pub(super) const SIGNAL_KEYS: [Key; 6] = [
    Key::Long,
    Key::Short,
    Key::LongExit,
    Key::ShortExit,
    Key::BullStart,
    Key::BearStart,
];
