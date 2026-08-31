use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    English,
    #[default]
    SimplifiedChinese,
}

impl Language {
    pub const ALL: [Self; 2] = [Self::SimplifiedChinese, Self::English];

    pub const fn label(self) -> &'static str {
        match self {
            Self::English => "English",
            Self::SimplifiedChinese => "简体中文",
        }
    }
}

macro_rules! resources {
    ($($key:ident => $english:literal, $chinese:literal;)+) => {
        #[allow(dead_code)]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum TextKey { $($key,)+ }

        pub const fn text(language: Language, key: TextKey) -> &'static str {
            match (language, key) {
                $((Language::English, TextKey::$key) => $english,)+
                $((Language::SimplifiedChinese, TextKey::$key) => $chinese,)+
            }
        }
    };
}

resources! {
    Trading => "Trading", "交易";
    Operations => "Operations", "运营";
    MultiChart => "Multi-chart", "多图表";
    MarketWatch => "Market watch", "市场观察";
    ChartIndicators => "Chart & indicators", "K线与指标";
    OrderBook => "Order book", "订单簿";
    TradeTape => "Trade tape", "逐笔成交";
    RecentTrades => "Recent trades", "最新成交";
    Accounts => "Accounts", "账户";
    Strategies => "Strategies", "策略";
    CopyRelations => "Copy relations", "跟单关系";
    ReceiptLedger => "Receipt ledger", "回执账本";
    LifecycleControl => "Lifecycle control", "生命周期控制";
    Diagnostics => "Diagnostics", "诊断";
    Settings => "Settings", "设置";
    TradingSettings => "Trading settings", "交易设置";
    OrderParameters => "Order parameters", "订单参数";
    TradingHotkeys => "Hotkeys", "快捷键";
    OrderType => "Order type", "订单类型";
    LimitOrder => "Limit", "限价";
    Enable => "Enabled", "启用";
    EnableHotkeys => "Enable trading hotkeys", "启用交易快捷键";
    SizeUnit => "Quote asset", "金额单位";
    SizePresets => "Order size presets", "下单金额预设";
    InvalidSizePreset => "Every preset must be greater than zero", "每档下单金额必须大于零";
    PriceValidity => "Price validity", "选价有效期";
    PriceValidityHint => "1–300 seconds after selecting a chart or order-book price. Expiry requires a new selection; submitted GTC orders are unchanged. Editing this value clears the current price.", "图表或盘口选价后 1–300 秒有效，到期须重新选价；不影响已提交的 GTC 订单。修改有效期会清空当前选价。";
    Preset => "Preset", "档位";
    PresetHint => "Select an amount from the order panel or its assigned hotkey.", "在下单面板或使用对应快捷键选择金额。";
    OrderSettingsHint => "Limit / GTC only. Post Only rejects orders that would immediately take liquidity.", "当前仅支持限价 / GTC。Post Only 表示只做挂单，避免立即吃单。";
    SettingsImmediate => "Changes apply immediately · saved locally", "修改即时生效 · 自动保存到本机";
    Done => "Done", "完成";
    MarginShort => "Margin", "保证金";
    NoAccountShort => "No trading account selected", "未选择交易帐户";
    DisplayCadence => "Market display", "行情显示";
    DisplayCadenceHint => "Display sampling only; market ingestion, order receipts and pointer interaction remain immediate.", "仅调整画面更新间隔；行情持续接收，交易回执与鼠标交互不节流。";
    CandleCadence => "Candles / indicators", "K线 / 指标";
    MarketDelay => "Market lag", "行情延迟";
    MarketDelayHint => "Exchange event to local receipt, not HTTP RTT. Includes clock skew. A dash means no fresh measurement.", "交易所事件时间至本机接收时间，并非 HTTP 往返耗时，受时钟偏差影响；无新鲜测量时显示横线。";
    TradeConnection => "Trading node", "交易节点";
    NodeStatusHint => "Selected account projection only. Healthy/private/writer generations do not prove that an order can execute; the server must revalidate risk, WAL and writer.", "仅表示所选账户的节点投影；健康及私流/writer 代际不代表能下单，仍须服务器校验 risk、WAL 与 writer。";
    AvailableMargin => "Available margin", "可用保证金";
    ValuationCurrencyMissing => "Valuation currency unspecified", "估值币种待提供";
    FundsHint => "Selected account, server valuation. Currency is not supplied by the current protocol: do not interpret this as the chart's quote asset. Stale values are historical, not spendable balance.", "所选账户的服务器估值；当前协议未提供估值币种，不能当作图表交易对的报价币。过期值仅供参考，不代表可下单余额。";
    AwaitingNode => "Awaiting node", "等待节点";
    NoExecutionAccount => "No trading account selected", "未选择交易帐户";
    TradingUnavailable => "Trading unavailable", "交易未就绪";
    Modules => "Modules", "模块";
    ResetLayout => "Reset layout", "重置布局";
    MarketServer => "Market server", "行情服务器";
    BinancePublic => "Binance public", "Binance 公共行情";
    ExecutionAccount => "Execution account", "执行账户";
    SelectExecutionAccount => "Select execution account", "选择执行账户";
    LoginAccount => "Log in account", "登录账户";
    Dismiss => "Close", "关闭";
    ExecutionAccountDescription => "After logging in, load or bind an exchange API key for the execution account. Market-server selection never requires login.", "登录账户后，可为执行账户加载或绑定交易所 API Key；行情服务器选择无需登录。";
    AccountReadiness => "Execution account readiness", "执行账户准入状态";
    LoginSession => "VenueFlow account login", "VenueFlow 账户登录";
    GatewayConnected => "Gateway connected", "网关已连接";
    ApiKeyBound => "API key bound to this account", "API Key 已绑定此账户";
    DualPositionVerified => "Hedge / dual-position mode verified", "双向持仓账户模式已验证";
    AccountNodeVerificationRequired => "The account node must return all three gateway proofs before switching can be armed.", "必须由账户节点返回网关、API Key 绑定和双向持仓三项证据后，才能启用切换。";
    ProjectedAccounts => "Server-projected accounts", "服务端投影账户";
    NoProjectedAccounts => "No account projection is available.", "暂无账户投影。";
    SelectedScope => "UI selected scope:", "UI 当前选择作用域：";
    SwitchExecutionAccount => "Switch execution account", "切换执行账户";
    SwitchWaitsForVerification => "Switching remains disabled until the account node verifies the gateway session, API-key binding, and Hedge mode.", "账户节点完成网关会话、API Key 绑定和双向持仓模式验证前，切换保持禁用。";
    Ready => "ready", "就绪";
    Pending => "pending", "待确认";
    Healthy => "healthy", "健康";
    Recovering => "recovering", "恢复中";
    NeedsAttention => "needs attention", "需关注";
    Stopped => "stopped", "已停止";
    Unknown => "unknown", "未知";
    SearchSymbol => "Search symbol", "搜索交易对";
    NoSymbols => "No matching symbols", "没有匹配的交易对";
    NoSnapshot => "no snapshot", "无快照";
    ControlError => "Control error", "控制服务错误";
    ControlBoundary => "EXEC=CONTROL · no credentials in UI · no direct mutation", "执行=控制服务器 · UI无凭证 · 不直连交易写入";
    ConfirmAction => "Confirm high-risk control action", "确认高风险控制操作";
    IntentWarning => "This request is only an intent. The service and account node will revalidate it.", "该请求只是语义意图，服务端和账户节点会再次验证。";
    TypeConfirmation => "Type the exact confirmation:", "请输入完全一致的确认文本：";
    Cancel => "Cancel", "取消";
    SubmitIntent => "Submit intent", "提交意图";
    SettingsTitle => "VenueFlow settings", "VenueFlow 设置";
    Language => "Language", "语言";
    ControlUrl => "Control API base URL", "Control API 地址";
    WebSameOrigin => "Web builds may leave this empty to use the current origin.", "Web 版本可留空并使用当前站点地址。";
    Reconnect => "Reconnect", "重新连接";
    LocalSymbol => "Local Binance USD-M symbol (canonical BASE/USDT or BASE/USDC)", "本地 Binance USD-M 交易对（规范格式 BASE/USDT 或 BASE/USDC）";
    NativePublicOnly => "Native only · fixed Binance LIVE public endpoints · no API key.", "仅桌面端 · 固定 Binance LIVE 公共端点 · 无需 API Key。";
    WebControlOnly => "Web builds remain Control-only and do not connect to exchanges.", "Web 版本仅连接 Control，不直连交易所。";
    UiScale => "UI scale", "界面缩放";
    ShowStatus => "Show status bar", "显示状态栏";
    FixedEndpointHint => "The configurable URL above is only for Venue Control; local public market endpoints are fixed in native code.", "上方可配置地址仅用于 Venue Control；本地公共行情端点固定在桌面程序中。";
    WorkspaceModules => "Workspace modules", "工作区模块";
    Connecting => "CONNECTING", "连接中";
    LoadingHistory => "LOADING HISTORY", "加载历史";
    Resyncing => "RESYNCING", "重新同步";
    Stale => "STALE", "数据过期";
    LiveData => "LIVE DATA", "实时数据";
    Degraded => "DEGRADED", "降级";
    Offline => "OFFLINE", "离线";
    Markets => "Markets", "交易对";
    MarketSource => "Binance public catalog · local prices", "Binance 公共目录 · 本地行情";
    Symbol => "Symbol", "交易对";
    Last => "Last", "最新价";
    DataSource => "DATA=BINANCE LIVE · local public REST/WS", "数据=BINANCE LIVE · 本地公共 REST/WS";
    Source => "Source", "来源";
    ControlFallback => "Control projection fallback", "Control 投影回退";
    NoMarket => "No market projection is available for this symbol.", "该交易对暂无行情投影。";
    Fit => "Fit", "适配";
    Follow => "Follow", "跟随";
    Bars => "bars", "根K线";
    Live => "LIVE", "实时";
    History => "HISTORY", "历史";
    NoCandles => "No candles", "暂无K线";
    Open => "Open", "开";
    High => "High", "高";
    Low => "Low", "低";
    Close => "Close", "收";
    Change => "Change", "涨跌幅";
    Amplitude => "Amplitude", "振幅";
    Volume => "Volume", "成交量";
    NoBook => "No order-book data. Waiting for the public WebSocket.", "暂无订单簿，正在等待公共 WebSocket。";
    Side => "Side", "方向";
    Price => "Price", "价格";
    Quantity => "Quantity", "数量";
    Total => "Total", "累计";
    PricePrecision => "Price precision", "价格精度";
    Ask => "ASK", "卖";
    Bid => "BID", "买";
    NoTrades => "No trades. Waiting for the public WebSocket.", "暂无逐笔成交，正在等待公共 WebSocket。";
    Time => "Time", "时间";
    AccountsSource => "secret-free server projections", "无敏感信息的服务端投影";
    WaitingControl => "Waiting for the Control API.", "正在等待 Control API。";
    Venue => "Venue", "交易所";
    Mode => "Mode", "模式";
    Account => "Account", "账户";
    Health => "Health", "健康";
    Equity => "Equity", "权益";
    Available => "Available", "可用";
    UnrealizedPnl => "uPnL", "未实现盈亏";
    PrivateGeneration => "Private gen", "私流代际";
    WriterGeneration => "Writer gen", "写入者代际";
    ReconciledAge => "Reconciled age", "对账时效";
    Instance => "Instance", "实例";
    Kind => "Kind", "类型";
    State => "State", "状态";
    Orders => "Orders", "订单";
    Long => "Long", "多头";
    Short => "Short", "空头";
    Pnl => "PnL", "盈亏";
    Epoch => "Epoch", "配置代际";
    StrategiesSubtitle => "Grid · Scalping · Copy", "网格 · 剥头皮 · 跟单";
    NoStrategies => "No strategy instances were returned.", "服务端未返回策略实例。";
    CopySubtitle => "target exposure and durable drift", "目标敞口与持久偏差";
    NoCopy => "Waiting for copy projections.", "正在等待跟单投影。";
    Leader => "Leader", "带单账户";
    Follower => "Follower", "跟单账户";
    Target => "Target", "目标";
    Actual => "Actual", "实际";
    Drift => "Drift", "偏差";
    LedgerSubtitle => "read-only execution and control receipts", "只读执行与控制回执";
    NoLedger => "Waiting for ledger projections.", "正在等待回执投影。";
    Observed => "Observed", "观察时间";
    Action => "Action", "操作";
    Receipt => "Receipt", "回执";
    Detail => "Detail", "详情";
    ControlSubtitle => "semantic intents; server revalidation required", "仅提交语义意图；必须由服务端复核";
    NoControl => "No controllable strategy projection is available.", "暂无可控制的策略投影。";
    SelectInstance => "Select instance", "选择实例";
    Pause => "Pause", "暂停";
    Resume => "Resume", "恢复";
    Stop => "Stop", "停止";
    Flatten => "Flatten", "平仓";
    DiagnosticsSubtitle => "control-plane and local public-market state", "控制平面与本地公共行情状态";
    AwaitingSnapshot => "awaiting snapshot", "等待快照";
    Online => "online", "在线";
    None => "none", "无";
    LocalPublicData => "Local public market data", "本地公共行情";
    AuthorityCoverage => "Authority coverage in Control v2", "Control v2 权限证据覆盖";
    LiveProjection => "LIVE, account, strategy, private generation, writer generation: projected", "LIVE、账户、策略、私流代际、写入者代际：已投影";
    ReceiptProjection => "Command Accepted/Applied/Rejected/Unknown receipts: projected", "命令 Accepted/Applied/Rejected/Unknown 回执：已投影";
    WalNotProjected => "WAL state: not projected", "WAL 状态：未投影";
    UnknownNotProjected => "Runtime Unknown fence: not projected", "运行时 Unknown 栅栏：未投影";
    CapabilityNotProjected => "Capability evidence freshness: not projected", "能力证据时效：未投影";
    RuntimeProjection => "Runtime projection", "运行时投影";
    Endpoint => "Endpoint", "端点";
    SnapshotPolling => "Snapshot polling", "快照轮询";
    EventStream => "SSE stream", "SSE 事件流";
    LastEventId => "Last event ID", "最近事件 ID";
    Schema => "Schema", "协议版本";
    Generated => "Generated", "生成时间";
    LedgerRows => "Ledger rows", "账本行数";
    LocalVenueLive => "Venue: Binance USD-M · mode: LIVE · identity: none", "交易所：Binance USD-M · 模式：LIVE · 无账户身份";
    Subscriptions => "Subscriptions", "订阅";
    Generation => "generation", "代际";
    CatalogSymbols => "Catalog symbols", "目录交易对";
    FixedEndpoints => "Fixed endpoints: fapi.binance.com + fstream.binance.com", "固定端点：fapi.binance.com + fstream.binance.com";
    PublicBoundary => "The native UI may read Binance public market endpoints. It cannot read credentials, private streams, PostgreSQL, WAL, checkpoints, artifacts, or any exchange mutation path.", "桌面 UI 仅可读取 Binance 公共行情端点，不能读取凭证、私有流、PostgreSQL、WAL、检查点或工件，也不包含任何交易所写入路径。";
    ProxyEnabled => "WebSocket proxy: enabled", "WebSocket 代理：已启用";
    ProxyDisabled => "WebSocket proxy: direct", "WebSocket 代理：直连";
    RecentNotices => "Recent notices", "最近通知";
    Indicators => "Local indicators", "本地指标";
    IndicatorPending => "Local indicators are not connected yet; Control indicators are not mixed into Binance local data.", "本地指标尚未接入；不会把 Control 指标静默混入 Binance 本地行情。";
    StopSemantics => "Stop cancels only the selected instance's owned orders and preserves residual custody. Flatten additionally requests signed zero-position convergence.", "停止只撤销所选实例拥有的订单并保留残余仓位托管；平仓还会请求带签名的零仓位收敛。";
    ConfirmationSemantics => "Pause, Stop, and Flatten require an exact typed scope confirmation. The account node remains authoritative and must independently revalidate every intent.", "暂停、停止与平仓需要输入精确的作用域确认；账户节点始终保持权威并独立复核每个意图。";
    SessionReceipts => "This session's command receipts", "本次会话的命令回执";
    AccountProjectionCaveat => "WAL state · runtime Unknown fence · capability freshness: not projected by Control v2", "WAL 状态 · 运行时 Unknown 栅栏 · 能力证据时效：Control v2 尚未投影";
    AccountAuthorityCaveat => "VenueFlow shows writer/private generations and reconciliation age only; it does not infer missing authority from health or ledger text.", "VenueFlow 只显示写入者/私流代际与对账时效，不会根据健康状态或账本文本推断缺失的权威事实。";
}

macro_rules! indicator_resources {
    ($($key:ident => $english:literal, $chinese:literal;)+) => {
        #[allow(dead_code)]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum IndicatorTextKey { $($key,)+ }

        pub const fn indicator_text(language: Language, key: IndicatorTextKey) -> &'static str {
            match (language, key) {
                $((Language::English, IndicatorTextKey::$key) => $english,)+
                $((Language::SimplifiedChinese, IndicatorTextKey::$key) => $chinese,)+
            }
        }
    };
}

indicator_resources! {
    MainTab => "Main", "主图";
    SubTab => "Sub-chart", "副图";
    CustomTab => "Custom", "自定义";
    BacktestTab => "Backtest", "回测测试";
    GeneralTab => "General", "通用";
    MainGroup => "Main", "主图";
    SubGroup => "Sub", "副图";
    ClosePrice => "Close", "收盘价";
    Deviation => "Deviation", "标准差倍数";
    Middle => "Middle", "中轨";
    OuterBands => "Upper / lower bands", "上轨 / 下轨";
    BandFill => "Band fill", "通道填充";
    FillOpacity => "Fill opacity", "填充不透明度";
    PositiveHistogram => "Positive histogram", "正值柱";
    NegativeHistogram => "Negative histogram", "负值柱";
    Step => "Step", "加速因子";
    Maximum => "Maximum", "最大加速";
    Multiplier => "Multiplier", "乘数";
    Fast => "Fast", "快线";
    Slow => "Slow", "慢线";
    Signal => "Signal", "信号线";
    Period => "Period", "周期";
    Smoothing => "Smoothing", "平滑";
    RsiPeriod => "RSI", "RSI周期";
    StochasticPeriod => "Stochastic", "随机周期";
    Line => "Line", "线条";
    RisingLine => "Rising line", "上涨线";
    FallingLine => "Falling line", "下跌线";
    RisingBackground => "Uptrend fill to candle midpoint", "上涨区域填充（至K线实体中点）";
    FallingBackground => "Downtrend fill to candle midpoint", "下跌区域填充（至K线实体中点）";
    Save => "Save", "保存";
    RestoreDefaults => "Restore defaults", "恢复默认";
    LiveRedraw => "Changes redraw the chart live", "修改将实时重绘图表";
    FeatureUnavailable => "This feature is not available yet", "该功能尚未开放";
    RecalculationFailed => "Indicator recalculation failed", "指标重算失败";
    MaTitle => "MA - Moving Average", "MA - 移动平均线";
    EmaTitle => "EMA - Exponential Moving Average", "EMA - 指数移动平均线";
    WmaTitle => "WMA - Weighted Moving Average", "WMA - 加权移动平均线";
    BollTitle => "BOLL - Bollinger Bands", "BOLL - 布林带";
    VwapTitle => "VWAP - Volume Weighted Average Price", "VWAP - 成交量加权均价";
    AvlTitle => "AVL - Average Value Line", "AVL - 均价线";
    TrixTitle => "TRIX - Triple Exponential Average", "TRIX - 三重指数平滑";
    SarTitle => "SAR - Parabolic Stop and Reverse", "SAR - 抛物线转向";
    SuperTitle => "SUPER - SUPERTREND", "SUPER - SUPERTREND";
    VolTitle => "VOL - Volume", "VOL - 成交量";
    MacdTitle => "MACD - Moving Average Convergence Divergence", "MACD - 指数平滑异同移动平均";
    RsiTitle => "RSI - Relative Strength Index", "RSI - 相对强弱指标";
    MfiTitle => "MFI - Money Flow Index", "MFI - 资金流量指标";
    KdjTitle => "KDJ - Stochastic", "KDJ - 随机指标";
    ObvTitle => "OBV - On Balance Volume", "OBV - 能量潮";
    CciTitle => "CCI - Commodity Channel Index", "CCI - 顺势指标";
    StochRsiTitle => "StochRSI - Stochastic RSI", "StochRSI - 随机相对强弱";
    WilliamsRTitle => "WR - Williams %R", "WR - 威廉指标";
    DmiTitle => "DMI - Directional Movement Index", "DMI - 趋向指标";
    MomentumTitle => "MTM - Momentum", "MTM - 动量";
    EmvTitle => "EMV - Ease of Movement", "EMV - 简易波动指标";
    AtrTitle => "ATR - Average True Range", "ATR - 平均真实波幅";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_resources_are_stable_and_distinct() {
        assert_eq!(text(Language::English, TextKey::Settings), "Settings");
        assert_eq!(text(Language::SimplifiedChinese, TextKey::Settings), "设置");
        assert_eq!(text(Language::English, TextKey::Amplitude), "Amplitude");
        assert_eq!(text(Language::SimplifiedChinese, TextKey::Change), "涨跌幅");
        assert_eq!(text(Language::SimplifiedChinese, TextKey::Total), "累计");
        assert_eq!(
            indicator_text(Language::SimplifiedChinese, IndicatorTextKey::LiveRedraw),
            "修改将实时重绘图表"
        );
        assert_eq!(
            indicator_text(Language::English, IndicatorTextKey::SarTitle),
            "SAR - Parabolic Stop and Reverse"
        );
    }
}
