//! Opt-in checksummed segmented outbox backend with write-ahead manifest,
//! torn-tail recovery, ack journaling, and safe segment compaction.
//!
//! Keeps `SqliteOutbox` (v1) as the default and fallback backend.

use super::outbox::{
    EnqueueReport, OutboxCapacity, OutboxEntry, OutboxError, DEFAULT_CLAIM_LEASE_MS,
};
use crate::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CompletionStatus, ContentChecksum, EffectId,
    EffectOutcome, Generation, RuleId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SEGMENT_MAGIC: &[u8; 4] = b"RKOB";
const CHECKSUM_STR_LEN: usize = 71;
const FRAME_HEADER_LEN: usize = 4 + 4 + CHECKSUM_STR_LEN;
static NEXT_SEGMENT_NONCE: AtomicU64 = AtomicU64::new(1);

/// Selects the outbox backend; `SqliteV1` remains the default.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OutboxBackendKind {
    #[default]
    SqliteV1,
    Segmented(SegmentedOutboxConfig),
}

/// Configuration for [`SegmentedOutbox`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentedOutboxConfig {
    pub root_dir: PathBuf,
    pub max_segment_bytes: u64,
    pub capacity: OutboxCapacity,
    pub sync_on_write: bool,
}

impl SegmentedOutboxConfig {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            max_segment_bytes: 16 * 1024 * 1024,
            capacity: OutboxCapacity::default(),
            sync_on_write: true,
        }
    }
}

/// Deterministic crash-injection points for transaction oracle / recovery tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentedCrashStep {
    AfterSegmentAppendBeforeManifest,
    AfterManifestWriteBeforeSwap,
    AfterAckJournalBeforeManifest,
    AfterCompactionManifestBeforeDeleteOld,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentFramePayload {
    effect_id: EffectId,
    rule_id: RuleId,
    generation: Generation,
    entry: OutboxEntry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SegmentRef {
    segment_id: u64,
    offset: u64,
    len: u32,
    checksum: ContentChecksum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestEntry {
    effect_id: EffectId,
    rule_id: RuleId,
    action_id: ActionId,
    order_key: Option<String>,
    payload_bytes: usize,
    segment_ref: SegmentRef,
    claimed_by: Option<String>,
    claim_token: Option<String>,
    claimed_until_ms: Option<i64>,
    claim_seq: u64,
    terminal_outcome: Option<EffectOutcome>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct OutboxManifest {
    version: u32,
    active_segment_id: u64,
    next_segment_id: u64,
    latest_checkpoints: BTreeMap<String, CheckpointEnvelope>,
    entries: Vec<ManifestEntry>,
    #[serde(default)]
    settled_effect_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AckJournalRecord {
    effect_id: EffectId,
    #[serde(default)]
    claim_token: String,
    outcome: EffectOutcome,
}

/// Exclusive OS-backed directory lock guard released automatically on drop or process exit.
#[derive(Debug)]
struct WriterLockGuard {
    _file: File,
}

impl WriterLockGuard {
    fn acquire(root_dir: &Path) -> Result<Self, OutboxError> {
        let path = root_dir.join("writer.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| {
                OutboxError::Storage(format!(
                    "failed to open exclusive writer lock at {}: {e}",
                    path.display()
                ))
            })?;
        file.try_lock().map_err(|e| {
            OutboxError::Storage(format!(
                "failed to acquire exclusive writer lock at {}: {e}",
                path.display()
            ))
        })?;
        Ok(Self { _file: file })
    }
}

/// Checksummed segmented outbox implementing the same logical operations as
/// [`super::outbox::SqliteOutbox`].
#[derive(Debug)]
pub struct SegmentedOutbox {
    config: SegmentedOutboxConfig,
    manifest: OutboxManifest,
    _lock: WriterLockGuard,
}

impl SegmentedOutbox {
    /// Opens (or initializes) a segmented outbox in `config.root_dir`,
    /// repairing any trailing torn tail on segments/ack journal and replaying
    /// committed ack journal entries.
    pub fn open(config: SegmentedOutboxConfig) -> Result<Self, OutboxError> {
        fs::create_dir_all(config.root_dir.join("segments"))
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        let lock = WriterLockGuard::acquire(&config.root_dir)?;

        let manifest_path = config.root_dir.join("manifest.json");
        let mut manifest = if manifest_path.exists() {
            let raw = fs::read(&manifest_path).map_err(|e| OutboxError::Storage(e.to_string()))?;
            serde_json::from_slice::<OutboxManifest>(&raw)
                .map_err(|e| OutboxError::Storage(format!("corrupt outbox manifest: {e}")))?
        } else {
            OutboxManifest {
                version: 1,
                active_segment_id: 1,
                next_segment_id: 2,
                latest_checkpoints: BTreeMap::new(),
                entries: Vec::new(),
                settled_effect_ids: BTreeSet::new(),
            }
        };

        // Repair torn tails on all existing segment files, then verify every
        // referenced entry still has a valid segment frame on disk.
        let seg_dir = config.root_dir.join("segments");
        for entry in fs::read_dir(&seg_dir).map_err(|e| OutboxError::Storage(e.to_string()))? {
            let entry = entry.map_err(|e| OutboxError::Storage(e.to_string()))?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                repair_segment_torn_tail(&path, config.sync_on_write)?;
            }
        }

        for m in &manifest.entries {
            let seg_path = segment_path(&config.root_dir, m.segment_ref.segment_id);
            if !seg_path.exists() {
                return Err(OutboxError::Storage(format!(
                    "missing payload segment {} for effect {:?}",
                    m.segment_ref.segment_id, m.effect_id
                )));
            }
            let _ = read_and_verify_frame(&seg_path, &m.segment_ref)?;
        }

        // Replay valid ack journal records (truncating any torn journal tail).
        let journal_path = config.root_dir.join("ack_journal.log");
        if journal_path.exists() {
            let acks = replay_ack_journal(&journal_path, config.sync_on_write)?;
            for ack in acks {
                if ack.claim_token.is_empty() {
                    continue;
                }
                if let Some(m) = manifest.entries.iter_mut().find(|e| {
                    e.effect_id == ack.effect_id
                        && e.terminal_outcome.is_none()
                        && e.claim_token.as_deref() == Some(ack.claim_token.as_str())
                }) {
                    if ack.outcome.is_terminal() {
                        m.terminal_outcome = Some(ack.outcome);
                    }
                    m.claimed_by = None;
                    m.claim_token = None;
                    m.claimed_until_ms = None;
                }
            }
        }

        if !manifest_path.exists() {
            persist_manifest(&config.root_dir, &manifest, config.sync_on_write)?;
        }

        Ok(Self {
            config,
            manifest,
            _lock: lock,
        })
    }

    pub fn config(&self) -> &SegmentedOutboxConfig {
        &self.config
    }

    /// Removes unreferenced `seg_*.log` files (orphans left behind if a crash
    /// occurred after segment append or compaction before manifest swap).
    pub fn reclaim_orphan_segments(&self) -> Result<usize, OutboxError> {
        let mut referenced: BTreeSet<u64> = self
            .manifest
            .entries
            .iter()
            .map(|e| e.segment_ref.segment_id)
            .collect();
        referenced.insert(self.manifest.active_segment_id);

        let mut removed = 0;
        let seg_dir = self.config.root_dir.join("segments");
        for entry in fs::read_dir(&seg_dir).map_err(|e| OutboxError::Storage(e.to_string()))? {
            let entry = entry.map_err(|e| OutboxError::Storage(e.to_string()))?;
            let path = entry.path();
            if let Some(seg_id) = parse_segment_id(&path) {
                if !referenced.contains(&seg_id) {
                    fs::remove_file(&path).map_err(|e| OutboxError::Storage(e.to_string()))?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    /// Commits checkpoint state and outbox entries using write-ahead segment
    /// persistence followed by atomic manifest commit.
    pub fn commit_state_and_enqueue(
        &mut self,
        envelope: CheckpointEnvelope,
        entries: Vec<OutboxEntry>,
    ) -> Result<EnqueueReport, OutboxError> {
        self.commit_state_and_enqueue_with_crash(envelope, entries, None)
    }

    /// Same as [`SegmentedOutbox::commit_state_and_enqueue`], with an optional
    /// deterministic crash injection point for recovery oracle testing.
    pub fn commit_state_and_enqueue_with_crash(
        &mut self,
        envelope: CheckpointEnvelope,
        entries: Vec<OutboxEntry>,
        crash_at: Option<SegmentedCrashStep>,
    ) -> Result<EnqueueReport, OutboxError> {
        let rule_key = envelope.metadata.identity.rule_id.as_str();
        let incoming_gen = envelope.metadata.identity.generation;
        if let Some(prev) = self.manifest.latest_checkpoints.get(rule_key) {
            let expected_gen = prev.metadata.identity.generation;
            if incoming_gen < expected_gen {
                return Err(OutboxError::Checkpoint(
                    CheckpointError::GenerationMismatch {
                        expected: expected_gen,
                        actual: incoming_gen,
                    },
                ));
            }
        }
        for entry in &entries {
            if entry.effect.rule_id != envelope.metadata.identity.rule_id
                || entry.effect.generation != incoming_gen
            {
                return Err(OutboxError::Storage(format!(
                    "outbox effect {} ({}@{}) does not match checkpoint envelope ({}@{})",
                    entry.effect.effect_id.as_str(),
                    entry.effect.rule_id.as_str(),
                    entry.effect.generation.0,
                    rule_key,
                    incoming_gen.0
                )));
            }
        }

        let existing_ids: BTreeSet<&str> = self
            .manifest
            .entries
            .iter()
            .map(|e| e.effect_id.as_str())
            .collect();
        let mut seen_new = BTreeSet::new();
        let mut to_insert = Vec::new();
        let mut deduplicated = 0usize;
        let mut incoming_bytes = 0usize;

        for entry in entries {
            let eid = entry.effect.effect_id.as_str();
            if existing_ids.contains(eid)
                || self.manifest.settled_effect_ids.contains(eid)
                || !seen_new.insert(eid.to_owned())
            {
                deduplicated += 1;
                continue;
            }
            incoming_bytes = incoming_bytes.saturating_add(entry.payload.len());
            to_insert.push(entry);
        }

        let current_items = self
            .manifest
            .entries
            .iter()
            .filter(|e| e.terminal_outcome.is_none())
            .count();
        let current_bytes: usize = self
            .manifest
            .entries
            .iter()
            .filter(|e| e.terminal_outcome.is_none())
            .map(|e| e.payload_bytes)
            .sum();

        if current_items.saturating_add(to_insert.len()) > self.config.capacity.max_items {
            return Err(OutboxError::CapacityExceeded {
                resource: "items",
                current: current_items,
                incoming: to_insert.len(),
                limit: self.config.capacity.max_items,
            });
        }
        if current_bytes.saturating_add(incoming_bytes) > self.config.capacity.max_bytes {
            return Err(OutboxError::CapacityExceeded {
                resource: "bytes",
                current: current_bytes,
                incoming: incoming_bytes,
                limit: self.config.capacity.max_bytes,
            });
        }

        // Phase 1: Append and fsync segment payloads BEFORE updating manifest.
        let mut new_manifest_entries = Vec::with_capacity(to_insert.len());
        let inserted = to_insert.len();
        for entry in to_insert {
            let active_path = segment_path(&self.config.root_dir, self.manifest.active_segment_id);
            let current_len = fs::metadata(&active_path).map(|m| m.len()).unwrap_or(0);
            if current_len >= self.config.max_segment_bytes && current_len > 0 {
                self.manifest.active_segment_id = self.manifest.next_segment_id;
                self.manifest.next_segment_id = self.manifest.next_segment_id.saturating_add(1);
            }
            let seg_id = self.manifest.active_segment_id;
            let seg_path = segment_path(&self.config.root_dir, seg_id);
            let payload_struct = SegmentFramePayload {
                effect_id: entry.effect.effect_id.clone(),
                rule_id: entry.effect.rule_id.clone(),
                generation: entry.effect.generation,
                entry: entry.clone(),
            };
            let encoded = serde_json::to_vec(&payload_struct)
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
            let seg_ref = append_frame(&seg_path, seg_id, &encoded, self.config.sync_on_write)?;
            new_manifest_entries.push(ManifestEntry {
                effect_id: entry.effect.effect_id,
                rule_id: entry.effect.rule_id,
                action_id: entry.effect.destination.action_id,
                order_key: entry.effect.order_key,
                payload_bytes: entry.payload.len(),
                segment_ref: seg_ref,
                claimed_by: None,
                claim_token: None,
                claimed_until_ms: None,
                claim_seq: 0,
                terminal_outcome: None,
            });
        }

        if crash_at == Some(SegmentedCrashStep::AfterSegmentAppendBeforeManifest) {
            return Err(OutboxError::Storage(
                "injected crash after segment append before manifest commit".to_owned(),
            ));
        }

        // Phase 2: Atomically commit checkpoint + segment references in manifest.
        let mut next_manifest = self.manifest.clone();
        next_manifest
            .latest_checkpoints
            .insert(envelope.metadata.identity.rule_id.to_string(), envelope);
        next_manifest.entries.extend(new_manifest_entries);

        if crash_at == Some(SegmentedCrashStep::AfterManifestWriteBeforeSwap) {
            let tmp_path = self.config.root_dir.join("manifest.json.tmp");
            let raw = serde_json::to_vec_pretty(&next_manifest)
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
            fs::write(&tmp_path, raw).map_err(|e| OutboxError::Storage(e.to_string()))?;
            return Err(OutboxError::Storage(
                "injected crash after manifest temp write before atomic swap".to_owned(),
            ));
        }

        persist_manifest(
            &self.config.root_dir,
            &next_manifest,
            self.config.sync_on_write,
        )?;
        self.manifest = next_manifest;

        Ok(EnqueueReport {
            inserted,
            deduplicated,
        })
    }

    pub fn latest_checkpoint(&self, rule_id: &RuleId) -> Option<&CheckpointEnvelope> {
        self.manifest.latest_checkpoints.get(rule_id.as_str())
    }

    pub fn claim_pending_with_lease(
        &mut self,
        rule_id: &RuleId,
        action_id: &ActionId,
        worker_id: &str,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, OutboxError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now_ms = current_unix_millis();
        let lease_until_ms = now_ms.saturating_add(i64::try_from(lease_ms).unwrap_or(i64::MAX));
        let mut active_order_keys: BTreeSet<String> = BTreeSet::new();

        for m in &self.manifest.entries {
            if m.rule_id == *rule_id
                && m.action_id == *action_id
                && m.terminal_outcome.is_none()
                && m.claimed_by.is_some()
                && m.claimed_until_ms.is_some_and(|until| until > now_ms)
            {
                if let Some(ref ok) = m.order_key {
                    active_order_keys.insert(ok.clone());
                }
            }
        }

        let mut next_manifest = self.manifest.clone();
        let mut claimed = Vec::new();
        for m in &mut next_manifest.entries {
            if claimed.len() >= limit {
                break;
            }
            if m.rule_id != *rule_id || m.action_id != *action_id || m.terminal_outcome.is_some() {
                continue;
            }
            let lease_active =
                m.claimed_by.is_some() && m.claimed_until_ms.is_some_and(|until| until > now_ms);
            if lease_active {
                continue;
            }
            if let Some(ref ok) = m.order_key {
                if !active_order_keys.insert(ok.clone()) {
                    continue;
                }
            }

            let seg_path = segment_path(&self.config.root_dir, m.segment_ref.segment_id);
            if !seg_path.exists() {
                return Err(OutboxError::Storage(format!(
                    "missing payload segment {} for effect {:?}",
                    m.segment_ref.segment_id, m.effect_id
                )));
            }
            let frame_bytes = read_and_verify_frame(&seg_path, &m.segment_ref)?;
            let decoded: SegmentFramePayload = serde_json::from_slice(&frame_bytes)
                .map_err(|e| OutboxError::Storage(e.to_string()))?;

            m.claim_seq = m.claim_seq.saturating_add(1);
            let nonce = NEXT_SEGMENT_NONCE.fetch_add(1, Ordering::Relaxed);
            let token = format!("seg:{}:{nonce}:{worker_id}", m.claim_seq);
            m.claimed_by = Some(worker_id.to_owned());
            m.claim_token = Some(token.clone());
            m.claimed_until_ms = Some(lease_until_ms);

            let mut out = decoded.entry;
            out.claim_token = token;
            claimed.push(out);
        }

        if !claimed.is_empty() {
            persist_manifest(
                &self.config.root_dir,
                &next_manifest,
                self.config.sync_on_write,
            )?;
            self.manifest = next_manifest;
        }
        Ok(claimed)
    }

    pub fn claim_pending(
        &mut self,
        rule_id: &RuleId,
        action_id: &ActionId,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, OutboxError> {
        self.claim_pending_with_lease(rule_id, action_id, worker_id, DEFAULT_CLAIM_LEASE_MS, limit)
    }

    pub fn ack(
        &mut self,
        effect_id: &EffectId,
        claim_token: &str,
        outcome: EffectOutcome,
    ) -> Result<Option<CompletionStatus>, OutboxError> {
        self.ack_with_crash(effect_id, claim_token, outcome, None)
    }

    pub fn ack_with_crash(
        &mut self,
        effect_id: &EffectId,
        claim_token: &str,
        outcome: EffectOutcome,
        crash_at: Option<SegmentedCrashStep>,
    ) -> Result<Option<CompletionStatus>, OutboxError> {
        let Some(entry) = self
            .manifest
            .entries
            .iter()
            .find(|e| e.effect_id == *effect_id)
        else {
            return Ok(None);
        };

        if let Some(ref existing) = entry.terminal_outcome {
            if *existing == outcome {
                return Ok(Some(CompletionStatus::Applied));
            }
            return Ok(Some(CompletionStatus::DuplicateIgnored));
        }

        if claim_token.is_empty() || entry.claim_token.as_deref() != Some(claim_token) {
            return Ok(Some(CompletionStatus::DuplicateIgnored));
        }

        // Write-ahead ack journal entry before updating manifest.
        let journal_path = self.config.root_dir.join("ack_journal.log");
        let rec = AckJournalRecord {
            effect_id: effect_id.clone(),
            claim_token: claim_token.to_owned(),
            outcome: outcome.clone(),
        };
        let encoded = serde_json::to_vec(&rec).map_err(|e| OutboxError::Storage(e.to_string()))?;
        append_journal_record(&journal_path, &encoded, self.config.sync_on_write)?;

        if crash_at == Some(SegmentedCrashStep::AfterAckJournalBeforeManifest) {
            return Err(OutboxError::Storage(
                "injected crash after ack journal before manifest commit".to_owned(),
            ));
        }

        let mut next_manifest = self.manifest.clone();
        if let Some(m) = next_manifest
            .entries
            .iter_mut()
            .find(|e| e.effect_id == *effect_id)
        {
            if outcome.is_terminal() {
                m.terminal_outcome = Some(outcome);
            }
            m.claimed_by = None;
            m.claim_token = None;
            m.claimed_until_ms = None;
        }
        persist_manifest(
            &self.config.root_dir,
            &next_manifest,
            self.config.sync_on_write,
        )?;
        self.manifest = next_manifest;

        Ok(Some(CompletionStatus::Applied))
    }

    /// Compacts live (non-terminal) entries into a new segment, atomically
    /// commits the updated manifest, and then deletes superseded old segments.
    pub fn compact_with_crash(
        &mut self,
        crash_at: Option<SegmentedCrashStep>,
    ) -> Result<usize, OutboxError> {
        let old_segments: BTreeSet<u64> = self
            .manifest
            .entries
            .iter()
            .map(|e| e.segment_ref.segment_id)
            .chain(std::iter::once(self.manifest.active_segment_id))
            .collect();

        let new_seg_id = self.manifest.next_segment_id;
        let mut next_manifest = OutboxManifest {
            version: self.manifest.version,
            active_segment_id: new_seg_id,
            next_segment_id: new_seg_id.saturating_add(1),
            latest_checkpoints: self.manifest.latest_checkpoints.clone(),
            entries: Vec::new(),
            settled_effect_ids: self.manifest.settled_effect_ids.clone(),
        };

        let new_seg_path = segment_path(&self.config.root_dir, new_seg_id);
        let mut live_count = 0usize;
        for m in &self.manifest.entries {
            if m.terminal_outcome.is_some() {
                next_manifest
                    .settled_effect_ids
                    .insert(m.effect_id.as_str().to_owned());
                continue;
            }
            let old_path = segment_path(&self.config.root_dir, m.segment_ref.segment_id);
            if !old_path.exists() {
                return Err(OutboxError::Storage(format!(
                    "missing payload segment {} during compaction",
                    m.segment_ref.segment_id
                )));
            }
            let frame_bytes = read_and_verify_frame(&old_path, &m.segment_ref)?;
            let new_ref = append_frame(
                &new_seg_path,
                new_seg_id,
                &frame_bytes,
                self.config.sync_on_write,
            )?;
            let mut updated = m.clone();
            updated.segment_ref = new_ref;
            next_manifest.entries.push(updated);
            live_count += 1;
        }

        persist_manifest(
            &self.config.root_dir,
            &next_manifest,
            self.config.sync_on_write,
        )?;
        self.manifest = next_manifest;

        // Clear ack journal now that terminal outcomes are compacted out.
        let journal_path = self.config.root_dir.join("ack_journal.log");
        if journal_path.exists() {
            let _ = fs::remove_file(&journal_path);
        }

        if crash_at == Some(SegmentedCrashStep::AfterCompactionManifestBeforeDeleteOld) {
            return Err(OutboxError::Storage(
                "injected crash after compaction manifest before deleting old segments".to_owned(),
            ));
        }

        for old_id in old_segments {
            if old_id != new_seg_id {
                let p = segment_path(&self.config.root_dir, old_id);
                if p.exists() {
                    fs::remove_file(&p).map_err(|e| OutboxError::Storage(e.to_string()))?;
                }
            }
        }
        Ok(live_count)
    }
}

fn segment_path(root: &Path, seg_id: u64) -> PathBuf {
    root.join("segments").join(format!("seg_{seg_id:08}.log"))
}

fn parse_segment_id(path: &Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    let num = stem.strip_prefix("seg_")?;
    num.parse::<u64>().ok()
}

fn append_frame(
    path: &Path,
    seg_id: u64,
    payload: &[u8],
    sync: bool,
) -> Result<SegmentRef, OutboxError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
    let offset = file
        .metadata()
        .map_err(|e| OutboxError::Storage(e.to_string()))?
        .len();
    let len = u32::try_from(payload.len())
        .map_err(|_| OutboxError::Storage("segment frame exceeds u32".to_owned()))?;
    let checksum = ContentChecksum::sha256_bytes(payload);

    file.write_all(SEGMENT_MAGIC)
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
    file.write_all(&len.to_le_bytes())
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
    file.write_all(checksum.as_str().as_bytes())
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
    file.write_all(payload)
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
    if sync {
        file.sync_all()
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
    }

    Ok(SegmentRef {
        segment_id: seg_id,
        offset,
        len,
        checksum,
    })
}

fn read_and_verify_frame(path: &Path, seg_ref: &SegmentRef) -> Result<Vec<u8>, OutboxError> {
    let mut file = File::open(path).map_err(|e| OutboxError::Storage(e.to_string()))?;
    file.seek(SeekFrom::Start(seg_ref.offset))
        .map_err(|e| OutboxError::Storage(e.to_string()))?;

    let mut header = [0u8; FRAME_HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|e| OutboxError::Storage(format!("corrupt segment frame header: {e}")))?;
    if &header[..4] != SEGMENT_MAGIC {
        return Err(OutboxError::Storage(
            "invalid segment frame magic".to_owned(),
        ));
    }
    let len = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let checksum_str = std::str::from_utf8(&header[8..FRAME_HEADER_LEN])
        .map_err(|e| OutboxError::Storage(format!("corrupt segment checksum header: {e}")))?;
    if len != seg_ref.len || checksum_str != seg_ref.checksum.as_str() {
        return Err(OutboxError::Storage(
            "segment frame header mismatch".to_owned(),
        ));
    }

    let mut payload = vec![0u8; usize::try_from(len).unwrap_or(0)];
    file.read_exact(&mut payload)
        .map_err(|e| OutboxError::Storage(format!("truncated segment payload: {e}")))?;
    let actual = ContentChecksum::sha256_bytes(&payload);
    if actual != seg_ref.checksum {
        return Err(OutboxError::Storage(format!(
            "segment payload checksum mismatch at offset {}",
            seg_ref.offset
        )));
    }
    Ok(payload)
}

fn repair_segment_torn_tail(path: &Path, sync: bool) -> Result<(), OutboxError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
    let total_len = file
        .metadata()
        .map_err(|e| OutboxError::Storage(e.to_string()))?
        .len();
    let mut valid_offset = 0u64;

    loop {
        if valid_offset == total_len {
            break;
        }
        file.seek(SeekFrom::Start(valid_offset))
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        let mut header = [0u8; FRAME_HEADER_LEN];
        if file.read_exact(&mut header).is_err() {
            break;
        }
        if &header[..4] != SEGMENT_MAGIC {
            break;
        }
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let Ok(checksum_str) = std::str::from_utf8(&header[8..FRAME_HEADER_LEN]) else {
            break;
        };
        let mut payload = vec![0u8; usize::try_from(len).unwrap_or(0)];
        if file.read_exact(&mut payload).is_err() {
            break;
        }
        if ContentChecksum::sha256_bytes(&payload).as_str() != checksum_str {
            break;
        }
        valid_offset = valid_offset.saturating_add(FRAME_HEADER_LEN as u64 + u64::from(len));
    }

    if valid_offset < total_len {
        file.set_len(valid_offset)
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        if sync {
            file.sync_all()
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
        }
    }
    Ok(())
}

fn append_journal_record(path: &Path, payload: &[u8], sync: bool) -> Result<(), OutboxError> {
    let _ = append_frame(path, 0, payload, sync)?;
    Ok(())
}

fn replay_ack_journal(path: &Path, sync: bool) -> Result<Vec<AckJournalRecord>, OutboxError> {
    repair_segment_torn_tail(path, sync)?;
    let mut file = File::open(path).map_err(|e| OutboxError::Storage(e.to_string()))?;
    let total_len = file
        .metadata()
        .map_err(|e| OutboxError::Storage(e.to_string()))?
        .len();
    let mut offset = 0u64;
    let mut records = Vec::new();
    while offset < total_len {
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        let mut header = [0u8; FRAME_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let mut payload = vec![0u8; usize::try_from(len).unwrap_or(0)];
        file.read_exact(&mut payload)
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        let rec: AckJournalRecord =
            serde_json::from_slice(&payload).map_err(|e| OutboxError::Storage(e.to_string()))?;
        records.push(rec);
        offset = offset.saturating_add(FRAME_HEADER_LEN as u64 + u64::from(len));
    }
    Ok(records)
}

fn persist_manifest(root: &Path, manifest: &OutboxManifest, sync: bool) -> Result<(), OutboxError> {
    let tmp_path = root.join("manifest.json.tmp");
    let final_path = root.join("manifest.json");
    let encoded =
        serde_json::to_vec_pretty(manifest).map_err(|e| OutboxError::Storage(e.to_string()))?;
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        file.write_all(&encoded)
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        if sync {
            file.sync_all()
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
        }
    }
    fs::rename(&tmp_path, &final_path).map_err(|e| OutboxError::Storage(e.to_string()))?;
    Ok(())
}

fn current_unix_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    i64::try_from(millis).unwrap_or(i64::MAX)
}
