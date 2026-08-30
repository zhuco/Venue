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
    Modules => "Modules", "模块";
    ResetLayout => "Reset layout", "重置布局";
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
    CopyRelationDetails => "Selected relation", "已选跟单关系";
    CopyRelationBinding => "Exact follower binding", "精确跟单账户绑定";
    CopyRelationConfiguration => "Copy configuration", "跟单配置";
    LeaderBindingUnavailable => "Leader account binding is not projected by Control v2.", "Control v2 未投影带单账户绑定。";
    CopyConfigurationUnavailable => "Capital allocation, multiplier, and risk policy are not projected by Control v2.", "Control v2 未投影资本分配、倍率和风险策略。";
    CopyEditingUnavailable => "Create and edit are unavailable until Control exposes an authenticated relation configuration endpoint.", "Control 提供经认证的关系配置端点前，不能创建或编辑关系。";
    LastAppliedJob => "Last applied job", "最近应用任务";
    NoAppliedJob => "No applied job", "暂无已应用任务";
    SelectCopyRelation => "Select a relation to inspect its precise follower scope.", "选择一条关系以查看其精确跟单范围。";
    FollowerInstance => "Follower instance", "跟单实例";
    RelationStatus => "Relation status", "关系状态";
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
    FailureReason => "Failure reason", "失败原因";
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
    ConfirmationSemantics => "Pause, Resume, Stop, and Flatten require an exact typed scope confirmation. The account node remains authoritative and must independently revalidate every intent.", "暂停、恢复、停止与平仓需要输入精确的作用域确认；账户节点始终保持权威并独立复核每个意图。";
    SessionReceipts => "This session's command receipts", "本次会话的命令回执";
    AccountProjectionCaveat => "WAL state · runtime Unknown fence · capability freshness: not projected by Control v2", "WAL 状态 · 运行时 Unknown 栅栏 · 能力证据时效：Control v2 尚未投影";
    AccountAuthorityCaveat => "VenueFlow shows writer/private generations and reconciliation age only; it does not infer missing authority from health or ledger text.", "VenueFlow 只显示写入者/私流代际与对账时效，不会根据健康状态或账本文本推断缺失的权威事实。";
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
    }
}
