use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    storage::{ScalpingRiskBinding, ScalpingRiskCursor},
    strategy::scalping::{RiskUnit, StrategyBinding},
};

use super::ScalpingOwnerRiskPage;

/// One Core-owned, append-only logical-risk page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingOwnerRiskInboxRecord {
    pub sequence: u64,
    pub content_sha256: String,
    pub page: ScalpingOwnerRiskPage,
}

/// The sole local append boundary for an already-valued Core page. It never reads an account,
/// derives risk, or lets the resident write its own input. A caller must serialize Core writes for
/// one artifact root; a retry of the exact page is idempotent, while a competing page for the
/// same requested cursor is rejected before it can make the reader ambiguous.
#[derive(Debug)]
pub struct ScalpingOwnerRiskInboxJournal {
    path: PathBuf,
    binding: StrategyBinding,
    risk_unit: RiskUnit,
    next_sequence: u64,
    records: Vec<ScalpingOwnerRiskInboxRecord>,
}

impl ScalpingOwnerRiskInboxJournal {
    pub fn open(
        path: impl Into<PathBuf>,
        binding: StrategyBinding,
        risk_unit: RiskUnit,
    ) -> Result<Self, ScalpingOwnerRiskInboxError> {
        binding
            .validate()
            .map_err(|_| ScalpingOwnerRiskInboxError::Binding)?;
        if risk_unit.as_str().is_empty() {
            return Err(ScalpingOwnerRiskInboxError::Binding);
        }
        let path = path.into();
        let records = recover(&path, &binding, &risk_unit)?;
        let next_sequence = records
            .last()
            .map(|record| {
                record
                    .sequence
                    .checked_add(1)
                    .ok_or(ScalpingOwnerRiskInboxError::Sequence)
            })
            .transpose()?
            .unwrap_or(1);
        Ok(Self {
            path,
            binding,
            risk_unit,
            next_sequence,
            records,
        })
    }

    /// Fsyncs an externally produced page before returning. This is not a risk producer: all
    /// valuation fields must already be complete in `page`.
    pub fn append(
        &mut self,
        page: ScalpingOwnerRiskPage,
    ) -> Result<ScalpingOwnerRiskInboxRecord, ScalpingOwnerRiskInboxError> {
        validate_page(&page, &self.binding, &self.risk_unit)?;
        let content_sha256 = scalping_owner_risk_page_digest(&page)?;
        if let Some(record) = self
            .records
            .iter()
            .find(|record| record.page == page && record.content_sha256 == content_sha256)
        {
            return Ok(record.clone());
        }
        if self
            .records
            .iter()
            .any(|record| record.page.requested_after == page.requested_after)
        {
            return Err(ScalpingOwnerRiskInboxError::RequestCursor);
        }
        let record = ScalpingOwnerRiskInboxRecord {
            sequence: self.next_sequence,
            content_sha256,
            page,
        };
        let encoded = serde_json::to_vec(&record).map_err(ScalpingOwnerRiskInboxError::Encode)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| ScalpingOwnerRiskInboxError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .map_err(|source| ScalpingOwnerRiskInboxError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.records.push(record.clone());
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ScalpingOwnerRiskInboxError::Sequence)?;
        Ok(record)
    }
}

/// Read-only view of a Core-owned page inbox. A missing inbox is an empty source, while any
/// incomplete or contradictory durable content is a fence-worthy error.
#[derive(Debug)]
pub struct ScalpingOwnerRiskInboxReader {
    path: PathBuf,
    binding: StrategyBinding,
    risk_unit: RiskUnit,
}

impl ScalpingOwnerRiskInboxReader {
    pub fn open(
        path: impl Into<PathBuf>,
        binding: StrategyBinding,
        risk_unit: RiskUnit,
    ) -> Result<Self, ScalpingOwnerRiskInboxError> {
        binding
            .validate()
            .map_err(|_| ScalpingOwnerRiskInboxError::Binding)?;
        if risk_unit.as_str().is_empty() {
            return Err(ScalpingOwnerRiskInboxError::Binding);
        }
        Ok(Self {
            path: path.into(),
            binding,
            risk_unit,
        })
    }

    /// Revalidates the complete append-only inbox on every turn, then returns at most one page
    /// whose request cursor exactly matches the producer's durable cursor. A higher valuation
    /// generation may explicitly restart from `None`; competing candidates are never guessed.
    pub fn next_page(
        &self,
        resume_after: Option<&ScalpingRiskCursor>,
    ) -> Result<Option<ScalpingOwnerRiskPage>, ScalpingOwnerRiskInboxError> {
        let records = recover(&self.path, &self.binding, &self.risk_unit)?;
        let mut candidate = None;
        for record in records {
            if requested_by(&record.page, resume_after) {
                match &candidate {
                    None => candidate = Some((record.content_sha256, record.page)),
                    Some((digest, page))
                        if *digest == record.content_sha256 && *page == record.page => {}
                    Some(_) => return Err(ScalpingOwnerRiskInboxError::RequestCursor),
                }
            }
        }
        Ok(candidate.map(|(_, page)| page))
    }
}

pub fn scalping_owner_risk_page_digest(
    page: &ScalpingOwnerRiskPage,
) -> Result<String, ScalpingOwnerRiskInboxError> {
    let bytes = serde_json::to_vec(page).map_err(ScalpingOwnerRiskInboxError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn recover(
    path: &Path,
    binding: &StrategyBinding,
    risk_unit: &RiskUnit,
) -> Result<Vec<ScalpingOwnerRiskInboxRecord>, ScalpingOwnerRiskInboxError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ScalpingOwnerRiskInboxError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(ScalpingOwnerRiskInboxError::Truncated);
    }
    let mut records = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: ScalpingOwnerRiskInboxRecord =
            serde_json::from_slice(line).map_err(ScalpingOwnerRiskInboxError::Decode)?;
        let expected = u64::try_from(records.len())
            .map_err(|_| ScalpingOwnerRiskInboxError::Sequence)?
            .checked_add(1)
            .ok_or(ScalpingOwnerRiskInboxError::Sequence)?;
        if record.sequence != expected {
            return Err(ScalpingOwnerRiskInboxError::Sequence);
        }
        if record.content_sha256 != scalping_owner_risk_page_digest(&record.page)? {
            return Err(ScalpingOwnerRiskInboxError::Hash);
        }
        validate_page(&record.page, binding, risk_unit)?;
        records.push(record);
    }
    Ok(records)
}

fn requested_by(page: &ScalpingOwnerRiskPage, resume_after: Option<&ScalpingRiskCursor>) -> bool {
    match resume_after {
        None => page.requested_after.is_none(),
        Some(resume_after) => {
            page.requested_after.as_ref() == Some(resume_after)
                || (page.requested_after.is_none()
                    && page.cursor.binding.valuation_generation
                        > resume_after.binding.valuation_generation)
        }
    }
}

fn validate_page(
    page: &ScalpingOwnerRiskPage,
    binding: &StrategyBinding,
    risk_unit: &RiskUnit,
) -> Result<(), ScalpingOwnerRiskInboxError> {
    validate_cursor(&page.cursor, binding, risk_unit)?;
    if let Some(requested_after) = &page.requested_after {
        validate_cursor(requested_after, binding, risk_unit)?;
    }
    if page.facts.len() > super::MAX_RISK_FACTS_PER_PAGE {
        return Err(ScalpingOwnerRiskInboxError::Page);
    }
    let supplied_ids = page
        .facts
        .iter()
        .map(|fact| fact.fact.fact_id.as_str())
        .collect::<BTreeSet<_>>();
    if supplied_ids.len() != page.facts.len()
        || !supplied_ids.iter().all(|fact_id| {
            page.cursor
                .source_fact_ids
                .iter()
                .any(|cursor_id| cursor_id == fact_id)
        })
    {
        return Err(ScalpingOwnerRiskInboxError::Page);
    }
    for fact in &page.facts {
        if fact.binding != page.cursor.binding
            || fact.fact.fact_id.trim().is_empty()
            || fact.fact.event_time_ms == 0
            || fact.fact.event_time_ms < page.cursor.complete_from_ms
            || fact.fact.event_time_ms > page.cursor.observed_through_ms
            || fact.fact.valuation_generation != page.cursor.binding.valuation_generation
            || fact.fact.risk_unit != page.cursor.binding.risk_unit
        {
            return Err(ScalpingOwnerRiskInboxError::Page);
        }
    }
    Ok(())
}

fn validate_cursor(
    cursor: &ScalpingRiskCursor,
    binding: &StrategyBinding,
    risk_unit: &RiskUnit,
) -> Result<(), ScalpingOwnerRiskInboxError> {
    validate_binding(&cursor.binding, binding, risk_unit)?;
    if cursor.cursor_id.trim().is_empty()
        || cursor.observed_through_ms == 0
        || cursor.complete_from_ms > cursor.observed_through_ms
        || cursor.source_fact_ids.iter().any(|id| id.trim().is_empty())
        || cursor.source_fact_ids.iter().collect::<BTreeSet<_>>().len()
            != cursor.source_fact_ids.len()
    {
        return Err(ScalpingOwnerRiskInboxError::Page);
    }
    Ok(())
}

fn validate_binding(
    actual: &ScalpingRiskBinding,
    expected: &StrategyBinding,
    risk_unit: &RiskUnit,
) -> Result<(), ScalpingOwnerRiskInboxError> {
    if actual.exchange != expected.exchange
        || actual.account != expected.account
        || actual.owner_scope != expected.owner_scope
        || actual.strategy_instance_id != expected.strategy_instance_id
        || actual.run_id != expected.run_id
        || actual.parameter_release_id != expected.parameter_release_id
        || actual.symbol != expected.symbol
        || actual.risk_unit != *risk_unit
        || actual.valuation_generation == 0
    {
        return Err(ScalpingOwnerRiskInboxError::Binding);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingOwnerRiskInboxError {
    #[error("owner-risk inbox binding or logical risk unit is invalid")]
    Binding,
    #[error("owner-risk inbox page is incomplete or cross-bound")]
    Page,
    #[error("owner-risk inbox has no unambiguous page for the durable request cursor")]
    RequestCursor,
    #[error("owner-risk inbox has a truncated tail")]
    Truncated,
    #[error("owner-risk inbox sequence is invalid or exhausted")]
    Sequence,
    #[error("owner-risk inbox record content hash does not match")]
    Hash,
    #[error("owner-risk inbox I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("owner-risk inbox encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("owner-risk inbox JSON is invalid: {0}")]
    Decode(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::{
        domain::{Amount, Asset},
        storage::ScalpingRiskFact,
        strategy::scalping::{RiskFact, StrategyKind},
    };

    use super::*;

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding {
            strategy_kind: StrategyKind::Scalping,
            strategy_instance_id: "owner-risk-inbox".to_owned(),
            run_id: "shadow-1".to_owned(),
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
            parameter_release_id: "scalping-shadow-v1".to_owned(),
            owner_scope: "owner-risk-inbox:shadow-1".to_owned(),
            risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
        })
    }

    fn page(
        binding: &StrategyBinding,
        requested_after: Option<ScalpingRiskCursor>,
        cursor_id: &str,
        generation: u64,
        event_time_ms: u64,
    ) -> Result<ScalpingOwnerRiskPage, Box<dyn std::error::Error>> {
        let risk_unit = RiskUnit::new("risk")?;
        let risk_binding = ScalpingRiskBinding {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            owner_scope: binding.owner_scope.clone(),
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            parameter_release_id: binding.parameter_release_id.clone(),
            symbol: binding.symbol.clone(),
            risk_unit: risk_unit.clone(),
            valuation_generation: generation,
        };
        let fact_id = format!("core-risk-{cursor_id}");
        Ok(ScalpingOwnerRiskPage {
            requested_after,
            facts: vec![ScalpingRiskFact {
                binding: risk_binding.clone(),
                fact: RiskFact {
                    fact_id: fact_id.clone(),
                    event_time_ms,
                    valuation_generation: generation,
                    risk_unit,
                    realized_pnl: Decimal::ONE,
                },
            }],
            cursor: ScalpingRiskCursor {
                cursor_id: cursor_id.to_owned(),
                binding: risk_binding,
                source_sequence: event_time_ms,
                complete_from_ms: event_time_ms,
                observed_through_ms: event_time_ms,
                has_more: false,
                source_fact_ids: vec![fact_id],
            },
        })
    }

    fn append(
        path: &Path,
        sequence: u64,
        page: ScalpingOwnerRiskPage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let record = ScalpingOwnerRiskInboxRecord {
            sequence,
            content_sha256: scalping_owner_risk_page_digest(&page)?,
            page,
        };
        let mut bytes = if path.exists() {
            std::fs::read(path)?
        } else {
            Vec::new()
        };
        bytes.extend(serde_json::to_vec(&record)?);
        bytes.push(b'\n');
        std::fs::write(path, bytes)?;
        Ok(())
    }

    #[test]
    fn missing_inbox_is_empty_and_reopen_resumes_from_exact_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("owner-risk-pages.jsonl");
        let binding = binding()?;
        let reader =
            ScalpingOwnerRiskInboxReader::open(&path, binding.clone(), RiskUnit::new("risk")?)?;
        assert!(reader.next_page(None)?.is_none());

        let first = page(&binding, None, "cursor-1", 1, 100)?;
        let second = page(&binding, Some(first.cursor.clone()), "cursor-2", 1, 200)?;
        let second_cursor = second.cursor.clone();
        append(&path, 1, first.clone())?;
        append(&path, 2, second.clone())?;
        assert_eq!(reader.next_page(None)?, Some(first.clone()));

        let reopened = ScalpingOwnerRiskInboxReader::open(&path, binding, RiskUnit::new("risk")?)?;
        assert_eq!(reopened.next_page(Some(&first.cursor))?, Some(second));
        assert!(reopened.next_page(Some(&second_cursor))?.is_none());
        Ok(())
    }

    #[test]
    fn core_journal_fsyncs_one_page_retries_exactly_and_rejects_a_competing_page()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("owner-risk-pages.jsonl");
        let binding = binding()?;
        let first = page(&binding, None, "cursor-1", 1, 100)?;
        let competing = page(&binding, None, "cursor-other", 1, 101)?;
        let mut journal =
            ScalpingOwnerRiskInboxJournal::open(&path, binding.clone(), RiskUnit::new("risk")?)?;

        let committed = journal.append(first.clone())?;
        assert_eq!(committed.sequence, 1);
        assert_eq!(journal.append(first)?.sequence, 1);
        assert!(matches!(
            journal.append(competing),
            Err(ScalpingOwnerRiskInboxError::RequestCursor)
        ));
        let reader = ScalpingOwnerRiskInboxReader::open(&path, binding, RiskUnit::new("risk")?)?;
        assert_eq!(reader.next_page(None)?, Some(committed.page));
        Ok(())
    }

    #[test]
    fn duplicate_core_page_for_one_request_is_idempotently_selected()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("owner-risk-pages.jsonl");
        let binding = binding()?;
        let reader =
            ScalpingOwnerRiskInboxReader::open(&path, binding.clone(), RiskUnit::new("risk")?)?;
        let first = page(&binding, None, "cursor-1", 1, 100)?;
        append(&path, 1, first.clone())?;
        append(&path, 2, first.clone())?;

        assert_eq!(reader.next_page(None)?, Some(first));
        Ok(())
    }

    #[test]
    fn corrupt_truncated_or_unrequested_pages_never_make_a_fact()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let binding = binding()?;
        let path = directory.path().join("owner-risk-pages.jsonl");
        let reader =
            ScalpingOwnerRiskInboxReader::open(&path, binding.clone(), RiskUnit::new("risk")?)?;
        let first = page(&binding, None, "cursor-1", 1, 100)?;
        let second = page(&binding, Some(first.cursor.clone()), "cursor-2", 1, 200)?;
        let conflicting_second = page(&binding, Some(first.cursor.clone()), "cursor-3", 1, 300)?;
        append(&path, 1, first.clone())?;
        append(&path, 2, second)?;
        append(&path, 3, conflicting_second)?;
        assert!(matches!(
            reader.next_page(Some(&first.cursor)),
            Err(ScalpingOwnerRiskInboxError::RequestCursor)
        ));

        let corrupt = directory.path().join("corrupt.jsonl");
        std::fs::write(&corrupt, b"{\n")?;
        let corrupt_reader =
            ScalpingOwnerRiskInboxReader::open(&corrupt, binding.clone(), RiskUnit::new("risk")?)?;
        assert!(matches!(
            corrupt_reader.next_page(None),
            Err(ScalpingOwnerRiskInboxError::Decode(_))
        ));

        let truncated = directory.path().join("truncated.jsonl");
        std::fs::write(&truncated, b"{")?;
        let truncated_reader =
            ScalpingOwnerRiskInboxReader::open(&truncated, binding, RiskUnit::new("risk")?)?;
        assert!(matches!(
            truncated_reader.next_page(None),
            Err(ScalpingOwnerRiskInboxError::Truncated)
        ));
        Ok(())
    }
}
