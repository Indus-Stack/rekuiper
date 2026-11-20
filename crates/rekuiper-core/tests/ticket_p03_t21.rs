use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointId, CheckpointMetadata, CompletionStatus,
    ContentChecksum, EffectDestination, EffectId, EffectOutcome, EffectRecord, ErrorCode,
    ErrorObject, ErrorStage, Generation, OutboxBoundary, PlanIdentity, RuleId, StructuredError,
};
use rekuiper_core::reliability::{
    OutboxBackendKind, OutboxCapacity, OutboxEntry, OutboxError, SegmentedCrashStep,
    SegmentedOutbox, SegmentedOutboxConfig,
};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rekuiper_p03_t21_{name}_{}_{}",
        std::process::id(),
        i64::try_from(jiff::Timestamp::now().as_nanosecond()).unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn envelope(rule: &str, cp_id: u64) -> CheckpointEnvelope {
    CheckpointEnvelope {
        metadata: CheckpointMetadata {
            format_version: 1,
            identity: PlanIdentity {
                rule_id: RuleId::try_from(rule).unwrap(),
                generation: Generation(1),
                plan_fingerprint: ContentChecksum::sha256_bytes(b"plan_seg"),
                schema_fingerprints: BTreeMap::new(),
                extension_digests: BTreeMap::new(),
            },
            checkpoint_id: CheckpointId(cp_id),
            source_positions: vec![],
            outbox: OutboxBoundary {
                last_effect_id: None,
                pending_effect_count: 0,
            },
            participants: BTreeMap::new(),
            created_wall_millis: 1000,
        },
        payloads: BTreeMap::new(),
    }
}

fn entry(rule: &str, id: &str, payload: &[u8]) -> OutboxEntry {
    OutboxEntry::new(
        EffectRecord {
            effect_id: EffectId::try_from(id).unwrap(),
            rule_id: RuleId::try_from(rule).unwrap(),
            generation: Generation(1),
            destination: EffectDestination {
                action_id: ActionId::try_from("act_1").unwrap(),
                connection_id: None,
            },
            order_key: Some("k1".to_owned()),
            attempt: 0,
            terminal_outcome: None,
        },
        payload.to_vec(),
    )
}

#[test]
fn p03_t21_sqlite_v1_remains_default_and_crash_oracle_recovers_cleanly() {
    assert_eq!(OutboxBackendKind::default(), OutboxBackendKind::SqliteV1);

    let dir = temp_dir("crash_oracle");
    let cfg = SegmentedOutboxConfig::new(&dir);
    let rule = RuleId::try_from("r_seg").unwrap();
    let act = ActionId::try_from("act_1").unwrap();

    // 1. Crash after segment append before manifest commit -> orphan segment frame,
    //    not visible on reopen, and orphan segment file can be reclaimed.
    {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        let err = outbox
            .commit_state_and_enqueue_with_crash(
                envelope("r_seg", 1),
                vec![entry("r_seg", "eff_orphan", b"orphan")],
                Some(SegmentedCrashStep::AfterSegmentAppendBeforeManifest),
            )
            .expect_err("injected crash must abort commit");
        assert!(matches!(err, OutboxError::Storage(_)));
    }
    {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        assert!(outbox.latest_checkpoint(&rule).is_none());
        assert!(outbox
            .claim_pending(&rule, &act, "w1", 10)
            .unwrap()
            .is_empty());

        // 2. Normal commit succeeds; simulate torn tail appended to segment.
        outbox
            .commit_state_and_enqueue(
                envelope("r_seg", 2),
                vec![
                    entry("r_seg", "eff_1", b"hello-1"),
                    entry("r_seg", "eff_2", b"hello-2"),
                ],
            )
            .unwrap();
    }

    let seg1 = dir.join("segments").join("seg_00000001.log");
    {
        let mut f = OpenOptions::new().append(true).open(&seg1).unwrap();
        f.write_all(b"RKOB\x10\x00\x00\x00corrupt_torn_tail")
            .unwrap();
    }

    // Reopen repairs torn tail, preserves committed entries, and replays ack journal after crash.
    {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        let claimed = outbox.claim_pending(&rule, &act, "w1", 1).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].effect.effect_id.as_str(), "eff_1");
        assert_eq!(claimed[0].payload, b"hello-1");

        // Crash after ack journal before manifest update -> ack still replays on reopen!
        let ack_err = outbox
            .ack_with_crash(
                &claimed[0].effect.effect_id,
                claimed[0].claim_token(),
                EffectOutcome::Delivered,
                Some(SegmentedCrashStep::AfterAckJournalBeforeManifest),
            )
            .expect_err("injected crash after ack journal");
        assert!(matches!(ack_err, OutboxError::Storage(_)));
    }

    {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        // eff_1 was replayed from ack_journal, so eff_2 is now claimable!
        let claimed2 = outbox.claim_pending(&rule, &act, "w1", 10).unwrap();
        assert_eq!(claimed2.len(), 1);
        assert_eq!(claimed2[0].effect.effect_id.as_str(), "eff_2");

        // Crash during compaction after manifest swap before deleting old segment:
        // live eff_2 is preserved on reopen and orphan old segment is reclaimed.
        let compact_err = outbox
            .compact_with_crash(Some(
                SegmentedCrashStep::AfterCompactionManifestBeforeDeleteOld,
            ))
            .expect_err("injected crash before deleting old segment");
        assert!(matches!(compact_err, OutboxError::Storage(_)));
    }

    {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        let reclaimed = outbox.reclaim_orphan_segments().unwrap();
        assert!(reclaimed >= 1);
        let ack_res = outbox
            .ack(
                &EffectId::try_from("eff_2").unwrap(),
                "stale_token",
                EffectOutcome::PermanentFailure {
                    error: StructuredError {
                        code: ErrorCode::InvalidData,
                        stage: ErrorStage::Action,
                        object: ErrorObject::Action(act.clone()),
                        retriable: false,
                        position: None,
                    },
                },
            )
            .unwrap();
        assert_eq!(ack_res, Some(CompletionStatus::DuplicateIgnored));
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn p03_t21_missing_payload_segment_fails_visibly_and_capacity_enforced() {
    let dir = temp_dir("missing_segment");
    let mut cfg = SegmentedOutboxConfig::new(&dir);
    cfg.capacity = OutboxCapacity {
        max_items: 1,
        max_bytes: 1024,
    };

    {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        outbox
            .commit_state_and_enqueue(
                envelope("r_miss", 1),
                vec![entry("r_miss", "eff_1", b"payload")],
            )
            .unwrap();

        let cap_err = outbox
            .commit_state_and_enqueue(
                envelope("r_miss", 2),
                vec![entry("r_miss", "eff_2", b"overflow")],
            )
            .expect_err("capacity must be enforced");
        assert!(matches!(cap_err, OutboxError::CapacityExceeded { .. }));
    }

    // Delete referenced segment file -> reopen must fail visibly.
    std::fs::remove_file(dir.join("segments").join("seg_00000001.log")).unwrap();
    let err =
        SegmentedOutbox::open(cfg).expect_err("missing payload segment must fail visibly on open");
    assert!(matches!(err, OutboxError::Storage(_)));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn p03_t21_retryable_and_ambiguous_effects_remain_claimable_across_reopen() {
    let dir = temp_dir("retryable_stays_claimable");
    let cfg = SegmentedOutboxConfig::new(&dir);
    let rule = RuleId::try_from("r_retry").unwrap();
    let act = ActionId::try_from("act_1").unwrap();
    let eff = EffectId::try_from("eff_retry").unwrap();

    {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        outbox
            .commit_state_and_enqueue(
                envelope("r_retry", 1),
                vec![entry("r_retry", "eff_retry", b"p")],
            )
            .unwrap();

        let claim1 = outbox
            .claim_pending(&rule, &act, "worker-a", 10)
            .unwrap()
            .into_iter()
            .next()
            .expect("first claim");
        let ack = outbox
            .ack(
                &eff,
                &claim1.claim_token,
                EffectOutcome::Retryable {
                    delay_hint_millis: Some(100),
                },
            )
            .unwrap();
        assert_eq!(ack, Some(CompletionStatus::Applied));
    }

    // Reopen: Retryable effect must still be pending and claimable.
    {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        let claim2 = outbox
            .claim_pending(&rule, &act, "worker-b", 10)
            .unwrap()
            .into_iter()
            .next()
            .expect("retryable effect must remain claimable after reopen");
        assert_eq!(claim2.effect.effect_id, eff);

        // Simulate crash after writing Ambiguous to ack journal before manifest commit.
        let crash_err = outbox
            .ack_with_crash(
                &eff,
                &claim2.claim_token,
                EffectOutcome::Ambiguous {
                    error: StructuredError {
                        code: ErrorCode::InvalidData,
                        stage: ErrorStage::Action,
                        object: ErrorObject::Action(act.clone()),
                        retriable: true,
                        position: None,
                    },
                },
                Some(SegmentedCrashStep::AfterAckJournalBeforeManifest),
            )
            .expect_err("injected crash");
        assert!(matches!(crash_err, OutboxError::Storage(_)));
    }

    // Reopen again: journal replay of Ambiguous must also leave it pending and claimable.
    {
        let mut outbox = SegmentedOutbox::open(cfg).unwrap();
        let claim3 = outbox
            .claim_pending(&rule, &act, "worker-c", 10)
            .unwrap()
            .into_iter()
            .next()
            .expect("ambiguous effect must remain claimable after journal replay");
        assert_eq!(claim3.effect.effect_id, eff);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn p03_t21_second_writer_open_on_same_dir_fails_visibly() {
    let dir = temp_dir("exclusive_writer");
    let cfg = SegmentedOutboxConfig::new(&dir);

    {
        let mut outbox1 = SegmentedOutbox::open(cfg.clone()).unwrap();
        outbox1
            .commit_state_and_enqueue(envelope("r_1", 1), vec![])
            .unwrap();

        let err = SegmentedOutbox::open(cfg.clone())
            .expect_err("second concurrent writer on same directory must fail visibly");
        assert!(matches!(err, OutboxError::Storage(_)));
    }

    // Once outbox1 is dropped, reopening succeeds and preserves checkpoint.
    let outbox2 = SegmentedOutbox::open(cfg).unwrap();
    assert_eq!(
        outbox2
            .latest_checkpoint(&RuleId::try_from("r_1").unwrap())
            .map(|c| c.metadata.identity.generation.0),
        Some(1)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn p03_t21_reopen_succeeds_when_stale_lock_file_left_on_disk() {
    let dir = temp_dir("abrupt_exit_lock");
    let cfg = SegmentedOutboxConfig::new(&dir);

    {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        outbox
            .commit_state_and_enqueue(envelope("r_abrupt", 1), vec![])
            .unwrap();
    }

    // Simulate an unlinked/abruptly exited writer that left writer.lock behind with no OS lock held.
    std::fs::write(dir.join("writer.lock"), b"").unwrap();
    let reopened = SegmentedOutbox::open(cfg.clone())
        .expect("OS-backed lock must allow reopen when no live process holds the lock");
    assert_eq!(
        reopened
            .latest_checkpoint(&RuleId::try_from("r_abrupt").unwrap())
            .map(|c| c.metadata.identity.generation.0),
        Some(1)
    );

    // While reopened is live, a second concurrent open still fails.
    let err =
        SegmentedOutbox::open(cfg).expect_err("second open while OS lock is held must still fail");
    assert!(matches!(err, OutboxError::Storage(_)));

    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn p03_t21_stale_ack_journal_does_not_clear_newer_claim_on_reopen() {
    let dir = temp_dir("stale_journal_claim");
    let cfg = SegmentedOutboxConfig::new(&dir);
    let rule = RuleId::try_from("r_claim").unwrap();
    let act = ActionId::try_from("act_1").unwrap();
    let eff = EffectId::try_from("eff_claim").unwrap();

    let claim_b_token = {
        let mut outbox = SegmentedOutbox::open(cfg.clone()).unwrap();
        outbox
            .commit_state_and_enqueue(
                envelope("r_claim", 1),
                vec![entry("r_claim", "eff_claim", b"p")],
            )
            .unwrap();

        // Worker A claims and records a Retryable ack.
        let claim_a = outbox
            .claim_pending(&rule, &act, "worker-a", 10_000)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        outbox
            .ack(
                &eff,
                &claim_a.claim_token,
                EffectOutcome::Retryable {
                    delay_hint_millis: None,
                },
            )
            .unwrap();

        // Worker B reclaims the effect with a long lease.
        let claim_b = outbox
            .claim_pending(&rule, &act, "worker-b", 60_000)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        claim_b.claim_token
    };

    // Reopen: replaying worker A's older ack journal entry must NOT clear worker B's active claim.
    let mut reopened = SegmentedOutbox::open(cfg).unwrap();
    assert!(
        reopened
            .claim_pending(&rule, &act, "worker-c", 10_000)
            .unwrap()
            .is_empty(),
        "worker C must not steal worker B's active lease after reopen"
    );
    assert_eq!(
        reopened
            .ack(&eff, &claim_b_token, EffectOutcome::Delivered)
            .unwrap(),
        Some(CompletionStatus::Applied)
    );

    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}
