use super::{Series, Snapshot};
use crate::chart::ChartInterval;
use eframe::egui;
use futures_util::{
    StreamExt,
    future::{Either, select},
};

#[derive(Debug)]
pub(crate) struct BrowserMarket {
    pub snapshot: Option<Snapshot>,
    pub status: String,
    wanted: Vec<(String, ChartInterval)>,
    sender: crossbeam_channel::Sender<Result<Snapshot, String>>,
    receiver: crossbeam_channel::Receiver<Result<Snapshot, String>>,
    pending: bool,
    next_ms: u64,
    received_ms: u64,
}

impl Default for BrowserMarket {
    fn default() -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        Self {
            snapshot: None,
            status: "Connecting public market…".into(),
            wanted: Vec::new(),
            sender,
            receiver,
            pending: false,
            next_ms: 0,
            received_ms: 0,
        }
    }
}
impl BrowserMarket {
    pub fn series(&self, symbol: &str, interval: Option<ChartInterval>) -> Option<&Series> {
        self.snapshot.as_ref()?.series.iter().find(|series| {
            series.symbol == symbol && interval.is_none_or(|value| value == series.interval)
        })
    }
    pub fn poll(&mut self, mut wanted: Vec<(String, ChartInterval)>, context: &egui::Context) {
        wanted.sort();
        wanted.dedup();
        wanted.truncate(8);
        let now = crate::account_center::now_ms();
        if self.wanted != wanted {
            self.wanted = wanted;
            self.snapshot = None;
            self.next_ms = 0;
            self.status = "Loading selection…".into();
        }
        while let Ok(result) = self.receiver.try_recv() {
            self.pending = false;
            match result {
                Ok(snapshot) if snapshot.selections == self.wanted => {
                    self.status = snapshot
                        .series
                        .iter()
                        .map(|item| format!("{}: {}", item.symbol, item.status))
                        .collect::<Vec<_>>()
                        .join(" · ");
                    self.snapshot = Some(snapshot);
                    self.received_ms = now;
                }
                Ok(_) => {
                    self.next_ms = 0;
                }
                Err(error) => {
                    self.status = error;
                    self.snapshot = None;
                }
            }
        }
        if self.received_ms > 0 && now.saturating_sub(self.received_ms) > 5_000 {
            self.snapshot = None;
            self.status = "Public market delayed; waiting for a fresh snapshot".into();
        }
        if self.pending || now < self.next_ms || self.wanted.is_empty() {
            return;
        }
        self.pending = true;
        self.next_ms = now.saturating_add(500);
        let wanted = self.wanted.clone();
        let sender = self.sender.clone();
        let context = context.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let result = match select(Box::pin(fetch(wanted)), Box::pin(timer())).await {
                Either::Left((result, _)) => result,
                Either::Right(_) => Err("Public market request timed out".into()),
            };
            let _ = sender.try_send(result);
            context.request_repaint();
        });
    }
}

async fn fetch(wanted: Vec<(String, ChartInterval)>) -> Result<Snapshot, String> {
    let origin = web_sys::window()
        .and_then(|window| window.location().origin().ok())
        .ok_or("browser origin unavailable")?;
    let mut url = reqwest::Url::parse(&format!("{origin}/preview/markets"))
        .map_err(|_| "invalid preview origin")?;
    url.query_pairs_mut().append_pair(
        "selections",
        &serde_json::to_string(&wanted).map_err(|_| "invalid selection")?,
    );
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .map_err(|_| "Public market connection unavailable")?;
    if !response.status().is_success() {
        return Err("Waiting for public market connection".into());
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "Public market response interrupted")?;
        if bytes.len().saturating_add(chunk.len()) > 8 * 1024 * 1024 {
            return Err("Public market response too large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let snapshot: Snapshot =
        serde_json::from_slice(&bytes).map_err(|_| "Invalid public market response")?;
    if snapshot.selections != wanted
        || snapshot.series.len() > 8
        || snapshot.series.iter().any(|series| {
            !wanted.contains(&(series.symbol.clone(), series.interval))
                || series.bars.len() > 10_000
        })
    {
        return Err("Public market selection mismatch".into());
    }
    Ok(snapshot)
}

async fn timer() {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, 5_000);
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

pub(crate) async fn load_font() -> Option<Vec<u8>> {
    let origin = web_sys::window()?.location().origin().ok()?;
    let request = async {
        let response = reqwest::get(format!("{origin}/preview-font.ttc"))
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let bytes = response.bytes().await.ok()?;
        (bytes.len() <= 32 * 1024 * 1024).then(|| bytes.to_vec())
    };
    match select(Box::pin(request), Box::pin(timer())).await {
        Either::Left((font, _)) => font,
        Either::Right(_) => None,
    }
}
