use super::*;

impl<G: AccountPhysicalGateway> AccountMutationHost<G> {
    pub(super) fn enrich_signed_order_owners(&self, snapshot: &mut SignedAccountSnapshot) {
        for fact in snapshot.open_orders_mut() {
            fact.owner = None;
            fact.external = true;
            let Some(command) = self.command_for_signed_order(fact) else {
                continue;
            };
            let (Some(owner), Some(client_id)) = (command.owner(), command.native_client_id())
            else {
                continue;
            };
            // Only an exact adapter identity comparison allows the signed wire ID to become the
            // canonical WAL ID consumed by Runtime and UI. A matching venue ID alone is not enough.
            fact.client_order_id = client_id.as_str().to_owned();
            fact.owner = Some(owner.clone());
            fact.external = false;
        }
    }

    fn command_for_signed_order(&self, fact: &SignedAccountOrderFact) -> Option<&ExecutionCommand> {
        if let Some(client_id) = fact
            .venue_order_id
            .as_deref()
            .and_then(|id| self.journal.client_id_by_venue_order_id(id))
        {
            let command_id = self.journal.command_id_by_client_id(client_id)?;
            let receipt = self.journal.receipt(command_id)?;
            if !matches!(&receipt.state, CommandState::Accepted { venue_order_id }
                if fact.venue_order_id.as_deref() == Some(venue_order_id.as_str()))
            {
                return None;
            }
            return self
                .signed_order_matches_command(&receipt.command, fact)
                .then_some(&receipt.command);
        }

        let direct_id = CommandId::new(fact.client_order_id.clone())
            .ok()
            .and_then(|id| self.journal.command_id_by_client_id(&id));
        let mut matched = None;
        if let Some(receipt) = direct_id.and_then(|id| self.journal.receipt(id)) {
            let attributable = match receipt.state {
                CommandState::Accepted { .. } => fact.venue_order_id.is_none(),
                CommandState::Submitted | CommandState::Unknown { .. } => true,
                CommandState::Prepared | CommandState::Rejected { .. } => false,
            };
            if attributable && self.signed_order_matches_command(&receipt.command, fact) {
                matched = Some(&receipt.command);
            }
        }

        // Encoded client IDs cannot be reversed. Before signed acceptance establishes the native
        // index, inspect only unresolved WAL commands, not historical orders or a second journal.
        for command_id in self.journal.unresolved_command_ids() {
            if Some(&command_id) == direct_id {
                continue;
            }
            let receipt = self.journal.receipt(&command_id)?;
            if matches!(
                receipt.state,
                CommandState::Submitted | CommandState::Unknown { .. }
            ) && self.signed_order_matches_command(&receipt.command, fact)
            {
                if matched.is_some() {
                    return None;
                }
                matched = Some(&receipt.command);
            }
        }
        matched
    }

    fn signed_order_matches_command(
        &self,
        command: &ExecutionCommand,
        fact: &SignedAccountOrderFact,
    ) -> bool {
        command.native_client_id().is_some_and(|client_id| {
            self.gateway
                .signed_client_order_id_matches(client_id, &fact.client_order_id)
        }) && command_matches_signed_order(command, fact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::{LimitTimeInForce, OrderCommand, OrderPurpose, OrderState};
    use venue_gateway_api::{GatewayMode, VenueId};

    struct EncodedGateway {
        binding: GatewayBinding,
    }

    impl AccountPhysicalGateway for EncodedGateway {
        type Error = std::io::Error;

        fn binding(&self) -> &GatewayBinding {
            &self.binding
        }

        fn reconcile(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<AccountRecoveryReport, Self::Error> {
            AccountRecoveryReport::new(
                self.binding.clone(),
                1,
                request
                    .unresolved()
                    .iter()
                    .map(|command| {
                        AccountRecoveryOutcome::still_unknown(command.command_id().clone())
                    })
                    .collect(),
            )
            .map_err(std::io::Error::other)
        }

        fn signed_client_order_id_matches(&self, canonical: &CommandId, signed: &str) -> bool {
            signed == format!("encoded-{}", canonical.as_str())
        }

        fn dispatch(&mut self, _: AccountDispatchPermit) -> AccountGatewayResult {
            AccountGatewayResult::Unknown
        }
    }

    fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000091",
            "DOGE/USDT".parse()?,
        )?)
    }

    fn command(binding: &GatewayBinding) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            command_id: CommandId::new("identity-command")?,
            client_order_id: CommandId::new("identity-client")?,
            owner: OrderOwner {
                strategy_instance_id: "actor".to_owned(),
                run_id: "run".to_owned(),
                exchange: binding.venue.as_str().to_owned(),
                account: binding.trading_account_id.clone(),
                symbol: binding.symbol.clone(),
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::from(10),
            limit_price: Price::new(Decimal::new(1, 1))?,
            time_in_force: LimitTimeInForce::Gtc,
            reduce_only: false,
        }))
    }

    fn fact(
        command: &ExecutionCommand,
    ) -> Result<SignedAccountOrderFact, Box<dyn std::error::Error>> {
        let ExecutionCommand::PlaceLimit(order) = command else {
            return Err("limit command required".into());
        };
        Ok(SignedAccountOrderFact {
            client_order_id: format!("encoded-{}", order.client_order_id.as_str()),
            venue_order_id: Some("venue-order-91".to_owned()),
            symbol: order.owner.symbol.clone(),
            family: NativeOrderFamily::UmOrder,
            side: order.side,
            position_side: order.position_side,
            quantity: order.quantity,
            limit_price: Some(order.limit_price.value()),
            time_in_force: Some(order.time_in_force),
            created_at_ms: Some(1),
            reduce_only: order.reduce_only,
            owner: None,
            external: true,
            state: Some(OrderState::PartiallyFilled),
            filled_quantity: Some(Decimal::ONE),
        })
    }

    fn open(
        temp: &tempfile::TempDir,
        binding: &GatewayBinding,
    ) -> Result<AccountMutationHost<EncodedGateway>, Box<dyn std::error::Error>> {
        Ok(AccountMutationHost::open(
            temp.path()
                .join("okx")
                .join("LIVE")
                .join(&binding.trading_account_id),
            binding.clone(),
            Decimal::TEN,
            EncodedGateway {
                binding: binding.clone(),
            },
        )?)
    }

    #[test]
    fn encoded_signed_identity_restores_canonical_owner_after_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let command = command(&binding)?;
        let signed = fact(&command)?;
        let temp = tempfile::tempdir()?;
        let mut host = open(&temp, &binding)?;
        host.journal.prepare(command.clone())?;
        host.journal
            .transition(command.command_id(), CommandState::Submitted)?;
        host.journal.transition(
            command.command_id(),
            CommandState::Accepted {
                venue_order_id: "venue-order-91".to_owned(),
            },
        )?;
        assert_eq!(host.command_for_signed_order(&signed), Some(&command));
        drop(host);
        let host = open(&temp, &binding)?;
        let mut snapshot = SignedAccountSnapshot::complete(
            binding,
            2,
            1,
            1,
            1,
            SignedAccountPositionMode::Hedge,
            vec![signed],
            vec![],
            "cursor".to_owned(),
            vec![],
        )?;
        host.enrich_signed_order_owners(&mut snapshot);
        let normalized = &snapshot.open_orders()[0];
        assert_eq!(normalized.client_order_id, "identity-client");
        assert_eq!(normalized.owner.as_ref(), command.owner());
        assert!(!normalized.external);
        assert_eq!(normalized.filled_quantity, Some(Decimal::ONE));
        Ok(())
    }

    #[test]
    fn encoded_signed_identity_never_ignores_client_or_order_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let command = command(&binding)?;
        let original = fact(&command)?;
        let temp = tempfile::tempdir()?;
        let mut host = open(&temp, &binding)?;
        host.journal.prepare(command.clone())?;
        host.journal
            .transition(command.command_id(), CommandState::Submitted)?;
        host.journal.transition(
            command.command_id(),
            CommandState::Accepted {
                venue_order_id: "venue-order-91".to_owned(),
            },
        )?;
        let mut wrong = original.clone();
        wrong.client_order_id = "encoded-unrelated-client".to_owned();
        assert!(host.command_for_signed_order(&wrong).is_none());
        wrong.client_order_id = "identity-client".to_owned();
        assert!(host.command_for_signed_order(&wrong).is_none());
        wrong = original.clone();
        wrong.venue_order_id = Some("unmapped-native-order".to_owned());
        assert!(host.command_for_signed_order(&wrong).is_none());
        wrong = original.clone();
        wrong.quantity -= Decimal::ONE;
        assert!(host.command_for_signed_order(&wrong).is_none());
        wrong = original.clone();
        wrong.time_in_force = Some(LimitTimeInForce::PostOnly);
        assert!(host.command_for_signed_order(&wrong).is_none());
        wrong = original;
        wrong.family = NativeOrderFamily::UmConditional;
        assert!(host.command_for_signed_order(&wrong).is_none());
        Ok(())
    }

    #[test]
    fn encoded_signed_identity_can_attribute_unknown_without_settling_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let command = command(&binding)?;
        let signed = fact(&command)?;
        let temp = tempfile::tempdir()?;
        let mut host = open(&temp, &binding)?;
        host.journal.prepare(command.clone())?;
        assert!(host.command_for_signed_order(&signed).is_none());
        host.journal
            .transition(command.command_id(), CommandState::Submitted)?;
        host.journal.transition(
            command.command_id(),
            CommandState::Unknown {
                reason: "ambiguous transport outcome".to_owned(),
            },
        )?;
        assert_eq!(host.command_for_signed_order(&signed), Some(&command));
        assert!(matches!(
            host.journal
                .receipt(command.command_id())
                .map(|receipt| &receipt.state),
            Some(CommandState::Unknown { .. })
        ));
        host.journal.transition(
            command.command_id(),
            CommandState::Rejected {
                reason: "signed terminal rejection".to_owned(),
            },
        )?;
        assert!(host.command_for_signed_order(&signed).is_none());
        Ok(())
    }
}
