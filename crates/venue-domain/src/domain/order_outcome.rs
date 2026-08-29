use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use crate::domain::{CommandId, ExecutionCommand, NativeOrderFamily, OrderOwner, OrderState};

/// Immutable identity of the mutation whose missing ACK is being reconciled.
///
/// The complete owner is retained so an account, symbol, run, strategy, or purpose cannot borrow
/// evidence produced for another physical binding. `native_client_id` is the family-specific
/// identity sent to the exchange, not an exchange-assigned order ID from a later response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OrderOutcomeBinding {
    command_id: CommandId,
    native_client_id: CommandId,
    native_order_family: NativeOrderFamily,
    owner: OrderOwner,
}

impl OrderOutcomeBinding {
    pub fn new(
        command_id: CommandId,
        native_client_id: CommandId,
        native_order_family: NativeOrderFamily,
        owner: OrderOwner,
    ) -> Result<Self, OrderOutcomeError> {
        owner
            .validate()
            .map_err(|_| OrderOutcomeError::InvalidBinding)?;
        if !valid_command_identity(command_id.as_str())
            || !valid_command_identity(native_client_id.as_str())
        {
            return Err(OrderOutcomeError::InvalidBinding);
        }
        Ok(Self {
            command_id,
            native_client_id,
            native_order_family,
            owner,
        })
    }

    /// Derives every identity directly from an order-creating command. Cancellation recovery must
    /// use [`Self::new`] because its target family is durable routing state, not part of
    /// [`ExecutionCommand::Cancel`].
    pub fn from_command(command: &ExecutionCommand) -> Result<Self, OrderOutcomeError> {
        let native_client_id = command
            .native_client_id()
            .cloned()
            .ok_or(OrderOutcomeError::MissingNativeIdentity)?;
        let native_order_family = command
            .native_order_family()
            .ok_or(OrderOutcomeError::MissingNativeIdentity)?;
        Self::new(
            command.command_id().clone(),
            native_client_id,
            native_order_family,
            command.mutation_owner().clone(),
        )
    }

    #[must_use]
    pub const fn command_id(&self) -> &CommandId {
        &self.command_id
    }

    #[must_use]
    pub const fn native_client_id(&self) -> &CommandId {
        &self.native_client_id
    }

    #[must_use]
    pub const fn native_order_family(&self) -> NativeOrderFamily {
        self.native_order_family
    }

    #[must_use]
    pub const fn owner(&self) -> &OrderOwner {
        &self.owner
    }
}

#[derive(Deserialize)]
struct OrderOutcomeBindingWire {
    command_id: CommandId,
    native_client_id: CommandId,
    native_order_family: NativeOrderFamily,
    owner: OrderOwner,
}

impl<'de> Deserialize<'de> for OrderOutcomeBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = OrderOutcomeBindingWire::deserialize(deserializer)?;
        Self::new(
            wire.command_id,
            wire.native_client_id,
            wire.native_order_family,
            wire.owner,
        )
        .map_err(D::Error::custom)
    }
}

/// Reconciliation contract frozen when an ACK-less mutation becomes UNKNOWN.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct UnknownOrderContract {
    binding: OrderOutcomeBinding,
    unknown_after_readback_generation: u64,
}

#[derive(Deserialize)]
struct UnknownOrderContractWire {
    binding: OrderOutcomeBinding,
    unknown_after_readback_generation: u64,
}

impl<'de> Deserialize<'de> for UnknownOrderContract {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = UnknownOrderContractWire::deserialize(deserializer)?;
        Self::new(wire.binding, wire.unknown_after_readback_generation).map_err(D::Error::custom)
    }
}

impl UnknownOrderContract {
    pub fn new(
        binding: OrderOutcomeBinding,
        unknown_after_readback_generation: u64,
    ) -> Result<Self, OrderOutcomeError> {
        if unknown_after_readback_generation == 0 {
            return Err(OrderOutcomeError::InvalidGeneration);
        }
        Ok(Self {
            binding,
            unknown_after_readback_generation,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &OrderOutcomeBinding {
        &self.binding
    }

    #[must_use]
    pub const fn unknown_after_readback_generation(&self) -> u64 {
        self.unknown_after_readback_generation
    }

    /// Classifies signed readback without converting a point lookup miss or partial empty page
    /// into authoritative absence.
    #[must_use]
    pub fn classify(&self, readback: SignedOrderReadback) -> AuthoritativeOrderOutcome {
        let status = if readback.binding != self.binding {
            OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::BindingMismatch)
        } else if readback.readback_generation <= self.unknown_after_readback_generation {
            OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::StaleReadbackGeneration)
        } else {
            match &readback.observation {
                OrderReadbackObservation::Found {
                    native_client_id,
                    state,
                    ..
                } => {
                    if native_client_id != self.binding.native_client_id() {
                        OrderOutcomeStatus::Unresolved(
                            UnresolvedOrderReason::NativeIdentityMismatch,
                        )
                    } else {
                        match state {
                            OrderState::New | OrderState::PartiallyFilled => {
                                OrderOutcomeStatus::Open
                            }
                            OrderState::Filled
                            | OrderState::Cancelled
                            | OrderState::Expired
                            | OrderState::Rejected => OrderOutcomeStatus::Terminal,
                            OrderState::Unknown => OrderOutcomeStatus::Unresolved(
                                UnresolvedOrderReason::UnknownOrderState,
                            ),
                        }
                    }
                }
                OrderReadbackObservation::NotFound => {
                    OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::PointLookupNotFound)
                }
                OrderReadbackObservation::EmptyCollection => match readback.coverage {
                    OrderReadbackCoverage::CompleteFamilyCollection { .. } => {
                        OrderOutcomeStatus::ProvenAbsent
                    }
                    OrderReadbackCoverage::ExactIdentity => {
                        OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::PointLookupNotFound)
                    }
                    OrderReadbackCoverage::IncompleteFamilyCollection { .. } => {
                        OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::IncompleteCollection)
                    }
                },
                OrderReadbackObservation::Indeterminate => {
                    OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::IndeterminateReadback)
                }
            }
        };
        AuthoritativeOrderOutcome {
            contract: self.clone(),
            readback,
            status,
        }
    }
}

/// Scope covered by one adapter-verified signed response.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderReadbackCoverage {
    /// One identity lookup. A returned matching order is useful, but a 404 is never absence proof.
    ExactIdentity,
    /// Every page of the bound native order family was signed and collected to its terminal cursor.
    CompleteFamilyCollection { page_count: u32 },
    /// At least one signed page exists, but terminal collection coverage was not established.
    IncompleteFamilyCollection { page_count: u32 },
}

/// Semantic observation extracted from signed exchange payloads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum OrderReadbackObservation {
    Found {
        native_client_id: CommandId,
        exchange_order_id: String,
        state: OrderState,
    },
    /// A family-specific point endpoint returned its not-found response (including HTTP 404).
    NotFound,
    /// The collected order rows were empty. Coverage decides whether this is authoritative.
    EmptyCollection,
    /// Parsing, pagination, signing, or exchange semantics could not establish a closed result.
    Indeterminate,
}

/// Adapter-verified signed readback bound to one UNKNOWN mutation identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SignedOrderReadback {
    binding: OrderOutcomeBinding,
    readback_generation: u64,
    coverage: OrderReadbackCoverage,
    observation: OrderReadbackObservation,
    signed_readback_sha256: [u8; 32],
}

#[derive(Deserialize)]
struct SignedOrderReadbackWire {
    binding: OrderOutcomeBinding,
    readback_generation: u64,
    coverage: OrderReadbackCoverage,
    observation: OrderReadbackObservation,
    signed_readback_sha256: [u8; 32],
}

impl<'de> Deserialize<'de> for SignedOrderReadback {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SignedOrderReadbackWire::deserialize(deserializer)?;
        Self::verified(
            wire.binding,
            wire.readback_generation,
            wire.coverage,
            wire.observation,
            wire.signed_readback_sha256,
        )
        .map_err(D::Error::custom)
    }
}

impl SignedOrderReadback {
    /// This constructor validates the canonical envelope, not an exchange signature. The adapter
    /// must call it only after verifying the signature, pagination terminal condition, family,
    /// native identity, and binding against the raw response collection.
    pub fn verified(
        binding: OrderOutcomeBinding,
        readback_generation: u64,
        coverage: OrderReadbackCoverage,
        observation: OrderReadbackObservation,
        signed_readback_sha256: [u8; 32],
    ) -> Result<Self, OrderOutcomeError> {
        let invalid_page_count = match coverage {
            OrderReadbackCoverage::ExactIdentity => false,
            OrderReadbackCoverage::CompleteFamilyCollection { page_count }
            | OrderReadbackCoverage::IncompleteFamilyCollection { page_count } => page_count == 0,
        };
        let invalid_found_identity = matches!(
            &observation,
            OrderReadbackObservation::Found {
                exchange_order_id,
                ..
            } if exchange_order_id.trim().is_empty()
        );
        if readback_generation == 0 {
            return Err(OrderOutcomeError::InvalidGeneration);
        }
        if invalid_page_count {
            return Err(OrderOutcomeError::InvalidCoverage);
        }
        if signed_readback_sha256.iter().all(|byte| *byte == 0) {
            return Err(OrderOutcomeError::UnsignedReadback);
        }
        if invalid_found_identity {
            return Err(OrderOutcomeError::InvalidObservation);
        }
        Ok(Self {
            binding,
            readback_generation,
            coverage,
            observation,
            signed_readback_sha256,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &OrderOutcomeBinding {
        &self.binding
    }

    #[must_use]
    pub const fn readback_generation(&self) -> u64 {
        self.readback_generation
    }

    #[must_use]
    pub const fn coverage(&self) -> OrderReadbackCoverage {
        self.coverage
    }

    #[must_use]
    pub const fn observation(&self) -> &OrderReadbackObservation {
        &self.observation
    }

    #[must_use]
    pub const fn signed_readback_sha256(&self) -> &[u8; 32] {
        &self.signed_readback_sha256
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome", content = "reason")]
pub enum OrderOutcomeStatus {
    Open,
    Terminal,
    ProvenAbsent,
    Unresolved(UnresolvedOrderReason),
}

/// Canonical outcome of an ACK-less/UNKNOWN order mutation. Private fields prevent callers from
/// manufacturing `ProvenAbsent` without passing through [`UnknownOrderContract::classify`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AuthoritativeOrderOutcome {
    contract: UnknownOrderContract,
    readback: SignedOrderReadback,
    status: OrderOutcomeStatus,
}

impl AuthoritativeOrderOutcome {
    #[must_use]
    pub const fn contract(&self) -> &UnknownOrderContract {
        &self.contract
    }

    #[must_use]
    pub const fn readback(&self) -> &SignedOrderReadback {
        &self.readback
    }

    #[must_use]
    pub const fn status(&self) -> OrderOutcomeStatus {
        self.status
    }

    /// Outcome facts close or retain an UNKNOWN fence. None authorizes replay of the original
    /// mutation; any later action requires a newly admitted command through the normal runtime.
    #[must_use]
    pub const fn grants_original_command_redispatch(&self) -> bool {
        false
    }
}

#[derive(Deserialize)]
struct AuthoritativeOrderOutcomeWire {
    contract: UnknownOrderContract,
    readback: SignedOrderReadback,
    status: OrderOutcomeStatus,
}

impl<'de> Deserialize<'de> for AuthoritativeOrderOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AuthoritativeOrderOutcomeWire::deserialize(deserializer)?;
        let classified = wire.contract.classify(wire.readback);
        if classified.status != wire.status {
            return Err(D::Error::custom(OrderOutcomeError::InvalidOutcome));
        }
        Ok(classified)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnresolvedOrderReason {
    BindingMismatch,
    NativeIdentityMismatch,
    StaleReadbackGeneration,
    PointLookupNotFound,
    IncompleteCollection,
    UnknownOrderState,
    IndeterminateReadback,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OrderOutcomeError {
    #[error("order outcome owner binding is invalid")]
    InvalidBinding,
    #[error("the command does not expose a native identity and order family")]
    MissingNativeIdentity,
    #[error("order readback generations must be non-zero")]
    InvalidGeneration,
    #[error("signed family collection coverage requires at least one page")]
    InvalidCoverage,
    #[error("signed order readback digest must be non-zero")]
    UnsignedReadback,
    #[error("a found order observation requires a non-empty exchange order identity")]
    InvalidObservation,
    #[error("serialized order outcome does not match its authoritative classification")]
    InvalidOutcome,
}

fn valid_command_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 36
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}
