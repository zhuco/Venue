use venue_control_protocol::{
    CommandReceipt, CommandState, ControlCommandRequest, GatewayMode, UiEventEnvelope, VenueId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountNodeBinding {
    pub venue: VenueId,
    pub mode: GatewayMode,
    pub trading_account_id: String,
}

impl AccountNodeBinding {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.mode != GatewayMode::Live {
            return Err("account node binding mode must be exactly LIVE");
        }
        if !venue_domain::is_canonical_trading_account_id(&self.trading_account_id) {
            return Err("account node binding trading account id is not canonical");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedCommand {
    pub command: ControlCommandRequest,
    pub consumer_id: String,
    pub claimed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedCommandReceipt {
    pub command: ControlCommandRequest,
    pub consumer_id: String,
    pub receipt: CommandReceipt,
}

impl ScopedCommandReceipt {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        self.command
            .validate()
            .map_err(|_| "receipt command scope is invalid")?;
        self.receipt
            .validate()
            .map_err(|_| "command receipt is invalid")?;
        if self.consumer_id.trim().is_empty() {
            return Err("command receipt consumer id is missing");
        }
        if self.receipt.request_id != self.command.request_id {
            return Err("command receipt request id does not match its scope");
        }
        if self.receipt.state == CommandState::Accepted {
            return Err("account nodes cannot settle a command as accepted");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    pub sequence: i64,
    pub event: UiEventEnvelope,
}
