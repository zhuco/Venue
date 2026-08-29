use std::collections::BTreeMap;

use crate::{
    domain::Symbol,
    runtime::account::{
        AccountKey, InstanceLifecycle, StrategyBinding, StrategyInstanceKey,
        model::validate_config_digest,
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StrategyRegistration {
    pub binding: StrategyBinding,
    pub config_epoch: u64,
    pub lifecycle: InstanceLifecycle,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StopPlan {
    pub binding: StrategyBinding,
    pub cancel_owned_orders: bool,
    pub preserve_position: bool,
    pub reconcile_after_connection_generation: u64,
    pub reconcile_after_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlattenPlan {
    pub binding: StrategyBinding,
    pub cancel_owned_orders: bool,
    pub reduce_owned_position: bool,
    pub reconcile_after_connection_generation: u64,
    pub reconcile_after_generation: u64,
}

/// A signed generation may release a symbol only after this exact instance has no open orders.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedStopProof {
    target: StrategyInstanceKey,
    connection_generation: u64,
    private_generation: u64,
    owned_open_orders: usize,
}

impl SignedStopProof {
    pub(super) fn new(
        target: StrategyInstanceKey,
        connection_generation: u64,
        private_generation: u64,
    ) -> Self {
        Self {
            target,
            connection_generation,
            private_generation,
            owned_open_orders: 0,
        }
    }

    #[must_use]
    pub const fn target(&self) -> &StrategyInstanceKey {
        &self.target
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StrategyRegistry {
    account: AccountKey,
    instances: BTreeMap<String, StrategyRegistration>,
    symbol_owners: BTreeMap<Symbol, String>,
}

impl StrategyRegistry {
    #[must_use]
    pub fn new(account: AccountKey) -> Self {
        Self {
            account,
            instances: BTreeMap::new(),
            symbol_owners: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn account(&self) -> &AccountKey {
        &self.account
    }

    pub fn register(&mut self, binding: StrategyBinding) -> Result<(), RegistryError> {
        if binding.key.account != self.account {
            return Err(RegistryError::AccountMismatch);
        }
        if self.instances.contains_key(&binding.key.instance_id) {
            return Err(RegistryError::InstanceOccupied);
        }
        if self.symbol_owners.contains_key(&binding.key.symbol) {
            return Err(RegistryError::SymbolOccupied);
        }
        self.symbol_owners
            .insert(binding.key.symbol.clone(), binding.key.instance_id.clone());
        self.instances.insert(
            binding.key.instance_id.clone(),
            StrategyRegistration {
                binding,
                config_epoch: 1,
                lifecycle: InstanceLifecycle::Registered,
            },
        );
        Ok(())
    }

    pub(crate) fn restore_state(
        &mut self,
        binding: &StrategyBinding,
        config_epoch: u64,
        lifecycle: InstanceLifecycle,
    ) -> Result<(), RegistryError> {
        let registration = self.registration_mut(&binding.key)?;
        if registration.binding != *binding || config_epoch == 0 {
            return Err(RegistryError::RecoveryMismatch);
        }
        registration.config_epoch = config_epoch;
        registration.lifecycle = if lifecycle == InstanceLifecycle::Running {
            InstanceLifecycle::Recovering
        } else {
            lifecycle
        };
        Ok(())
    }

    #[must_use]
    pub fn registration(&self, key: &StrategyInstanceKey) -> Option<&StrategyRegistration> {
        self.instances
            .get(&key.instance_id)
            .filter(|registration| registration.binding.key == *key)
    }

    #[must_use]
    pub fn binding_by_instance(&self, instance_id: &str) -> Option<&StrategyBinding> {
        self.instances
            .get(instance_id)
            .map(|registration| &registration.binding)
    }

    #[must_use]
    pub fn binding_by_symbol(&self, symbol: &Symbol) -> Option<&StrategyBinding> {
        self.symbol_owners
            .get(symbol)
            .and_then(|instance_id| self.binding_by_instance(instance_id))
    }

    pub fn registrations(&self) -> impl Iterator<Item = &StrategyRegistration> {
        self.instances.values()
    }

    pub fn active_bindings(&self) -> impl Iterator<Item = &StrategyBinding> {
        self.instances
            .values()
            .map(|registration| &registration.binding)
    }

    pub fn begin_recovery_all(&mut self) -> Vec<StrategyInstanceKey> {
        self.instances
            .values_mut()
            .filter(|registration| {
                matches!(
                    registration.lifecycle,
                    InstanceLifecycle::Registered
                        | InstanceLifecycle::Recovering
                        | InstanceLifecycle::Running
                )
            })
            .map(|registration| {
                registration.lifecycle = InstanceLifecycle::Recovering;
                registration.binding.key.clone()
            })
            .collect()
    }

    pub fn mark_recovering(&mut self, key: &StrategyInstanceKey) -> Result<(), RegistryError> {
        let registration = self.registration_mut(key)?;
        if matches!(
            registration.lifecycle,
            InstanceLifecycle::Stopping
                | InstanceLifecycle::Faulted
                | InstanceLifecycle::NeedsAttention
        ) {
            return Err(RegistryError::Lifecycle);
        }
        registration.lifecycle = InstanceLifecycle::Recovering;
        Ok(())
    }

    pub fn mark_running(&mut self, key: &StrategyInstanceKey) -> Result<(), RegistryError> {
        let registration = self.registration_mut(key)?;
        if !matches!(
            registration.lifecycle,
            InstanceLifecycle::Registered | InstanceLifecycle::Recovering
        ) {
            return Err(RegistryError::Lifecycle);
        }
        registration.lifecycle = InstanceLifecycle::Running;
        Ok(())
    }

    pub fn pause(&mut self, key: &StrategyInstanceKey) -> Result<(), RegistryError> {
        let registration = self.registration_mut(key)?;
        if matches!(
            registration.lifecycle,
            InstanceLifecycle::Stopping
                | InstanceLifecycle::Faulted
                | InstanceLifecycle::NeedsAttention
        ) {
            return Err(RegistryError::Lifecycle);
        }
        registration.lifecycle = InstanceLifecycle::Paused;
        Ok(())
    }

    pub fn resume(&mut self, key: &StrategyInstanceKey) -> Result<(), RegistryError> {
        let registration = self.registration_mut(key)?;
        if registration.lifecycle != InstanceLifecycle::Paused {
            return Err(RegistryError::Lifecycle);
        }
        registration.lifecycle = InstanceLifecycle::Recovering;
        Ok(())
    }

    pub fn needs_attention(&mut self, key: &StrategyInstanceKey) -> Result<(), RegistryError> {
        let registration = self.registration_mut(key)?;
        if registration.lifecycle == InstanceLifecycle::Stopping {
            return Err(RegistryError::Lifecycle);
        }
        registration.lifecycle = InstanceLifecycle::NeedsAttention;
        Ok(())
    }

    pub fn fault(&mut self, key: &StrategyInstanceKey) -> Result<(), RegistryError> {
        let registration = self.registration_mut(key)?;
        if registration.lifecycle == InstanceLifecycle::Stopping {
            return Err(RegistryError::Lifecycle);
        }
        registration.lifecycle = InstanceLifecycle::Faulted;
        Ok(())
    }

    pub fn request_stop(
        &mut self,
        key: &StrategyInstanceKey,
        reconcile_after_connection_generation: u64,
        reconcile_after_generation: u64,
    ) -> Result<StopPlan, RegistryError> {
        let registration = self.registration_mut(key)?;
        registration.lifecycle = InstanceLifecycle::Stopping;
        Ok(StopPlan {
            binding: registration.binding.clone(),
            cancel_owned_orders: true,
            preserve_position: true,
            reconcile_after_connection_generation,
            reconcile_after_generation,
        })
    }

    pub fn request_flatten(
        &mut self,
        key: &StrategyInstanceKey,
        reconcile_after_connection_generation: u64,
        reconcile_after_generation: u64,
    ) -> Result<FlattenPlan, RegistryError> {
        let registration = self.registration_mut(key)?;
        registration.lifecycle = InstanceLifecycle::Stopping;
        Ok(FlattenPlan {
            binding: registration.binding.clone(),
            cancel_owned_orders: true,
            reduce_owned_position: true,
            reconcile_after_connection_generation,
            reconcile_after_generation,
        })
    }

    pub fn complete_stop(
        &mut self,
        key: &StrategyInstanceKey,
        proof: SignedStopProof,
    ) -> Result<StrategyBinding, RegistryError> {
        if proof.target != *key
            || proof.connection_generation == 0
            || proof.private_generation == 0
            || proof.owned_open_orders != 0
        {
            return Err(RegistryError::StopNotProven);
        }
        let registration = self.registration(key).ok_or(RegistryError::Missing)?;
        if registration.lifecycle != InstanceLifecycle::Stopping {
            return Err(RegistryError::Lifecycle);
        }
        let removed = self
            .instances
            .remove(&key.instance_id)
            .ok_or(RegistryError::Missing)?;
        self.symbol_owners.remove(&removed.binding.key.symbol);
        Ok(removed.binding)
    }

    pub fn replace_config_digest(
        &mut self,
        key: &StrategyInstanceKey,
        config_digest: String,
    ) -> Result<(), RegistryError> {
        if validate_config_digest(&config_digest).is_err() {
            return Err(RegistryError::ConfigDigest);
        }
        let registration = self.registration_mut(key)?;
        if matches!(
            registration.lifecycle,
            InstanceLifecycle::Stopping
                | InstanceLifecycle::Faulted
                | InstanceLifecycle::NeedsAttention
        ) {
            return Err(RegistryError::Lifecycle);
        }
        if registration.binding.config_digest == config_digest {
            return Ok(());
        }
        registration.config_epoch = registration
            .config_epoch
            .checked_add(1)
            .ok_or(RegistryError::ConfigEpoch)?;
        registration.binding.config_digest = config_digest;
        if registration.lifecycle != InstanceLifecycle::Paused {
            registration.lifecycle = InstanceLifecycle::Recovering;
        }
        Ok(())
    }

    fn registration_mut(
        &mut self,
        key: &StrategyInstanceKey,
    ) -> Result<&mut StrategyRegistration, RegistryError> {
        self.instances
            .get_mut(&key.instance_id)
            .filter(|registration| registration.binding.key == *key)
            .ok_or(RegistryError::Missing)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RegistryError {
    #[error("strategy binding belongs to another account runtime")]
    AccountMismatch,
    #[error("strategy instance id is already registered in this account")]
    InstanceOccupied,
    #[error("symbol is already owned by another strategy in this account")]
    SymbolOccupied,
    #[error("strategy instance is not registered")]
    Missing,
    #[error("strategy lifecycle transition is invalid")]
    Lifecycle,
    #[error("signed zero-owned-order proof is required before releasing a symbol")]
    StopNotProven,
    #[error("configuration digest is invalid")]
    ConfigDigest,
    #[error("configuration epoch is exhausted")]
    ConfigEpoch,
    #[error("durable strategy checkpoint does not match the configured binding")]
    RecoveryMismatch,
}
