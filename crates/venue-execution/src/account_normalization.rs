use super::*;
use venue_domain::LimitTimeInForce;

/// Automatic strategies supply quote exposure; the adapter derives a fresh post-only price.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountLimitNormalizationIntent {
    pub command_id: CommandId,
    pub client_order_id: CommandId,
    pub owner: OrderOwner,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub quote_delta: Decimal,
    pub reduce_only: bool,
}

impl AccountLimitNormalizationIntent {
    pub fn validate(&self) -> Result<(), AccountHostValidationError> {
        if self.quote_delta <= Decimal::ZERO || Asset::new(self.owner.symbol.quote()).is_err() {
            return Err(AccountHostValidationError::Command);
        }
        self.owner
            .validate()
            .map_err(|_| AccountHostValidationError::Command)
    }
}

/// User-selected price is immutable. A close carries the caller's signed/UI base-quantity cap;
/// the normal account risk and position checks still apply before physical dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountPricedLimitIntent {
    pub intent: AccountLimitNormalizationIntent,
    pub limit_price: Price,
    pub time_in_force: LimitTimeInForce,
    pub maximum_quantity: Option<Decimal>,
}

impl AccountPricedLimitIntent {
    pub fn validate(&self) -> Result<(), AccountHostValidationError> {
        self.intent.validate()?;
        if self.limit_price.value() <= Decimal::ZERO
            || self
                .maximum_quantity
                .is_some_and(|quantity| quantity <= Decimal::ZERO)
            || (self.intent.reduce_only && self.maximum_quantity.is_none())
        {
            return Err(AccountHostValidationError::Command);
        }
        Ok(())
    }

    /// Quantity before native lot rounding. No minimum-notional rounding may increase this cap.
    pub fn quantity_cap(&self) -> Result<Decimal, AccountHostValidationError> {
        self.validate()?;
        let quantity = self
            .intent
            .quote_delta
            .checked_div(self.limit_price.value())
            .ok_or(AccountHostValidationError::Command)?;
        let quantity = self
            .maximum_quantity
            .map_or(quantity, |cap| quantity.min(cap));
        if quantity <= Decimal::ZERO {
            return Err(AccountHostValidationError::Command);
        }
        Ok(quantity)
    }
}

impl<G: AccountPhysicalGateway> AccountMutationHost<G> {
    /// Automatic BBO normalization has no side effects; WAL/entry checks occur at preparation.
    pub fn normalize_limit_intent(
        &mut self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostError<G::Error>> {
        self.validate_normalization_scope(intent)?;
        let command = self
            .gateway
            .normalize_limit_intent(intent)
            .map_err(AccountHostError::Validation)?;
        if !matches!(&command, ExecutionCommand::PlaceLimit(place) if place.time_in_force.is_post_only())
            || command.command_id() != &intent.command_id
            || command.native_client_id() != Some(&intent.client_order_id)
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::Command,
            ));
        }
        validate_command_scope(&command, &self.binding, &self.configured_symbols)
            .map_err(AccountHostError::Validation)?;
        Ok(command)
    }

    pub fn normalize_priced_limit_intent(
        &mut self,
        intent: &AccountPricedLimitIntent,
    ) -> Result<ExecutionCommand, AccountHostError<G::Error>> {
        intent.validate().map_err(AccountHostError::Validation)?;
        self.validate_normalization_scope(&intent.intent)?;
        let command = self
            .gateway
            .normalize_priced_limit_intent(intent)
            .map_err(AccountHostError::Validation)?;
        validate_priced_command(intent, &command).map_err(AccountHostError::Validation)?;
        validate_command_scope(&command, &self.binding, &self.configured_symbols)
            .map_err(AccountHostError::Validation)?;
        Ok(command)
    }

    fn validate_normalization_scope(
        &self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<(), AccountHostError<G::Error>> {
        intent.validate().map_err(AccountHostError::Validation)?;
        if intent.owner.exchange != self.binding.venue.as_str()
            || intent.owner.account != self.binding.trading_account_id
            || !self.configured_symbols.contains(&intent.owner.symbol)
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::Scope,
            ));
        }
        Ok(())
    }
}

fn validate_priced_command(
    intent: &AccountPricedLimitIntent,
    command: &ExecutionCommand,
) -> Result<(), AccountHostValidationError> {
    let ExecutionCommand::PlaceLimit(place) = command else {
        return Err(AccountHostValidationError::Command);
    };
    let expected = &intent.intent;
    let notional = place
        .quantity
        .checked_mul(place.limit_price.value())
        .ok_or(AccountHostValidationError::Command)?;
    if place.command_id != expected.command_id
        || place.client_order_id != expected.client_order_id
        || place.owner != expected.owner
        || place.side != expected.side
        || place.position_side != expected.position_side
        || place.reduce_only != expected.reduce_only
        || place.limit_price != intent.limit_price
        || place.time_in_force != intent.time_in_force
        || place.quantity <= Decimal::ZERO
        || place.quantity > intent.quantity_cap()?
        || notional > expected.quote_delta
    {
        return Err(AccountHostValidationError::Command);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::{OrderCommand, OrderPurpose};
    use venue_gateway_api::{GatewayMode, VenueId};

    fn intent() -> Result<AccountPricedLimitIntent, Box<dyn std::error::Error>> {
        Ok(AccountPricedLimitIntent {
            intent: AccountLimitNormalizationIntent {
                command_id: CommandId::new("manual-limit")?,
                client_order_id: CommandId::new("manual-client")?,
                owner: OrderOwner {
                    strategy_instance_id: "actor1".to_owned(),
                    run_id: "run1".to_owned(),
                    exchange: "bybit".to_owned(),
                    account: "00000000-0000-4000-8000-000000000081".to_owned(),
                    symbol: "DOGE/USDT".parse()?,
                    purpose: OrderPurpose::Entry,
                },
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                quote_delta: Decimal::TEN,
                reduce_only: false,
            },
            limit_price: Price::new(Decimal::new(1, 1))?,
            time_in_force: LimitTimeInForce::Gtc,
            maximum_quantity: Some(Decimal::from(40)),
        })
    }

    fn command(intent: &AccountPricedLimitIntent) -> OrderCommand {
        OrderCommand {
            command_id: intent.intent.command_id.clone(),
            client_order_id: intent.intent.client_order_id.clone(),
            owner: intent.intent.owner.clone(),
            side: intent.intent.side,
            position_side: intent.intent.position_side,
            quantity: Decimal::from(40),
            limit_price: intent.limit_price,
            time_in_force: intent.time_in_force,
            reduce_only: intent.intent.reduce_only,
        }
    }

    #[test]
    fn priced_limit_preserves_selected_semantics_and_caps() -> Result<(), Box<dyn std::error::Error>>
    {
        let intent = intent()?;
        let original = command(&intent);
        assert!(
            validate_priced_command(&intent, &ExecutionCommand::PlaceLimit(original.clone()))
                .is_ok()
        );
        let other_price = Price::new(Decimal::new(11, 2))?;
        let mut invalid = Vec::new();
        let mut row = original.clone();
        row.limit_price = other_price;
        invalid.push(row);
        let mut row = original.clone();
        row.time_in_force = LimitTimeInForce::PostOnly;
        invalid.push(row);
        let mut row = original.clone();
        row.quantity = Decimal::from(41);
        invalid.push(row);
        let mut row = original.clone();
        row.owner.strategy_instance_id = "other-actor".to_owned();
        invalid.push(row);
        let mut row = original.clone();
        row.side = OrderSide::Sell;
        invalid.push(row);
        let mut row = original.clone();
        row.position_side = PositionSide::Short;
        invalid.push(row);
        let mut row = original.clone();
        row.reduce_only = true;
        invalid.push(row);
        let mut row = original.clone();
        row.command_id = CommandId::new("other-command")?;
        invalid.push(row);
        let mut row = original;
        row.client_order_id = CommandId::new("other-client")?;
        invalid.push(row);
        for row in invalid {
            assert!(validate_priced_command(&intent, &ExecutionCommand::PlaceLimit(row)).is_err());
        }
        Ok(())
    }

    #[test]
    fn priced_limit_close_needs_a_positive_quantity_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut intent = intent()?;
        intent.intent.reduce_only = true;
        intent.intent.owner.purpose = OrderPurpose::Reduce;
        intent.intent.side = OrderSide::Sell;
        assert_eq!(intent.quantity_cap()?, Decimal::from(40));
        intent.maximum_quantity = Some(Decimal::from(200));
        assert_eq!(intent.quantity_cap()?, Decimal::from(100));
        intent.maximum_quantity = None;
        assert!(intent.quantity_cap().is_err());
        intent.maximum_quantity = Some(Decimal::ZERO);
        assert!(intent.quantity_cap().is_err());
        Ok(())
    }

    #[test]
    fn priced_limit_arithmetic_overflow_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut intent = intent()?;
        intent.limit_price = Price::new(Decimal::new(1, 28))?;
        intent.maximum_quantity = None;
        assert_eq!(
            intent.quantity_cap(),
            Err(AccountHostValidationError::Command)
        );

        intent.limit_price = Price::new(Decimal::from(2))?;
        let mut overflowing = command(&intent);
        overflowing.quantity = Decimal::MAX;
        assert_eq!(
            validate_priced_command(&intent, &ExecutionCommand::PlaceLimit(overflowing)),
            Err(AccountHostValidationError::Command)
        );
        Ok(())
    }

    struct PricedGateway {
        binding: GatewayBinding,
        result: Option<ExecutionCommand>,
        dispatches: usize,
    }

    impl AccountPhysicalGateway for PricedGateway {
        type Error = std::io::Error;
        fn binding(&self) -> &GatewayBinding {
            &self.binding
        }
        fn reconcile(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<AccountRecoveryReport, Self::Error> {
            AccountRecoveryReport::new(request.binding().clone(), 1, Vec::new())
                .map_err(std::io::Error::other)
        }
        fn normalize_priced_limit_intent(
            &mut self,
            _: &AccountPricedLimitIntent,
        ) -> Result<ExecutionCommand, AccountHostValidationError> {
            self.result
                .clone()
                .ok_or(AccountHostValidationError::Command)
        }
        fn dispatch(&mut self, _: AccountDispatchPermit) -> AccountGatewayResult {
            self.dispatches += 1;
            AccountGatewayResult::Unknown
        }
    }

    #[test]
    fn priced_limit_host_is_read_only_and_has_no_automatic_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let intent = intent()?;
        let command = ExecutionCommand::PlaceLimit(command(&intent));
        let binding = GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            intent.intent.owner.account.clone(),
            intent.intent.owner.symbol.clone(),
        )?;
        let temp = tempfile::tempdir()?;
        let gateway = PricedGateway {
            binding: binding.clone(),
            result: Some(command.clone()),
            dispatches: 0,
        };
        let root = temp
            .path()
            .join("bybit")
            .join("LIVE")
            .join(&intent.intent.owner.account);
        let mut host = AccountMutationHost::open(root, binding, Decimal::TEN, gateway)?;
        assert_eq!(host.normalize_priced_limit_intent(&intent)?, command);
        assert!(host.command_status(command.command_id())?.is_none());
        assert_eq!(host.gateway.dispatches, 0);
        host.gateway.result = None;
        assert!(host.normalize_priced_limit_intent(&intent).is_err());
        assert!(host.command_status(command.command_id())?.is_none());
        assert_eq!(host.gateway.dispatches, 0);
        Ok(())
    }
}
