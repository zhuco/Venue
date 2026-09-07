use super::*;

#[derive(Clone, Debug)]
pub(super) struct MarketSender {
    sender: Sender<LocalMarketClientEvent>,
    context: Option<egui::Context>,
}

impl From<Sender<LocalMarketClientEvent>> for MarketSender {
    fn from(sender: Sender<LocalMarketClientEvent>) -> Self {
        Self {
            sender,
            context: None,
        }
    }
}

impl MarketSender {
    pub(super) fn new(
        sender: Sender<LocalMarketClientEvent>,
        context: Option<egui::Context>,
    ) -> Self {
        Self { sender, context }
    }

    fn wake(&self) {
        if let Some(context) = &self.context {
            // Schedule from the producer: a queued repaint cannot wake a sleeping UI.
            context.request_repaint_after(Duration::from_millis(16));
        }
    }

    pub(super) fn try_send(
        &self,
        event: LocalMarketClientEvent,
    ) -> Result<(), TrySendError<LocalMarketClientEvent>> {
        self.sender.try_send(event)?;
        self.wake();
        Ok(())
    }

    pub(super) fn send_timeout(
        &self,
        event: LocalMarketClientEvent,
        timeout: Duration,
    ) -> Result<(), crossbeam_channel::SendTimeoutError<LocalMarketClientEvent>> {
        self.sender.send_timeout(event, timeout)?;
        self.wake();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_requests_a_frame_without_ui_draining_the_queue()
    -> Result<(), Box<dyn std::error::Error>> {
        let context = egui::Context::default();
        let wakeups = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = wakeups.clone();
        context.set_request_repaint_callback(move |_| {
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let (tx, rx) = bounded(8);
        let sender = MarketSender::new(tx, Some(context));
        sender.try_send(LocalMarketClientEvent::ProxyDetected(false))?;
        assert_eq!(rx.len(), 1);
        assert!(wakeups.load(std::sync::atomic::Ordering::SeqCst) > 0);
        Ok(())
    }
}
