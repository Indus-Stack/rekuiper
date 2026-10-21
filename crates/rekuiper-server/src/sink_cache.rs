//! Sink offline cache and resend, following eKuiper's sink cache options
//! (`enableCache`, `memoryCacheThreshold`, `maxDiskCache`, `bufferPageSize`,
//! `resendInterval`, `cleanCacheAtStop`, `resendPriority`,
//! `resendIndicatorField`, `resendDestination`).
//!
//! Records whose send failed recoverably are kept in FIFO order in three
//! tiers: an in-memory head (oldest, resent first), disk pages, and an
//! in-memory write page (newest) that is written to disk once it holds
//! `bufferPageSize` records. When the disk budget is exhausted the oldest
//! records are dropped and counted, never silently. Pages survive a restart
//! and are replayed first unless `cleanCacheAtStop` is set.
//!
//! Durability: a page is written with one write per page and no fsync; a
//! crash loses the unwritten write page (as eKuiper documents).

use rekuiper_core::model::StreamRecord;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Which data goes first while cached data exists (`resendPriority`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResendPriority {
    /// `-1`: live data first; resend only while no live data is flowing.
    LiveFirst,
    /// `0`: live and cached data interleave.
    Equal,
    /// `1`: cached data first; live data queues behind it (strict order).
    CacheFirst,
}

/// Parsed sink cache options for one action.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheConfig {
    pub memory_threshold: usize,
    pub max_disk: usize,
    pub page_size: usize,
    pub resend_interval: Duration,
    pub clean_at_stop: bool,
    pub priority: ResendPriority,
    pub indicator_field: Option<String>,
    pub destination: Option<String>,
}

impl Default for CacheConfig {
    /// eKuiper `etc/kuiper.yaml` sink defaults.
    fn default() -> Self {
        Self {
            memory_threshold: 1024,
            max_disk: 1_024_000,
            page_size: 256,
            resend_interval: Duration::ZERO,
            clean_at_stop: false,
            priority: ResendPriority::Equal,
            indicator_field: None,
            destination: None,
        }
    }
}

fn opt_u64(opts: &HashMap<String, Value>, key: &str) -> Option<u64> {
    match opts.get(key)? {
        Value::Number(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f.max(0.0) as u64)),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn opt_i64(opts: &HashMap<String, Value>, key: &str) -> Option<i64> {
    match opts.get(key)? {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn opt_bool(opts: &HashMap<String, Value>, key: &str) -> Option<bool> {
    match opts.get(key)? {
        Value::Bool(b) => Some(*b),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn opt_str(opts: &HashMap<String, Value>, key: &str) -> Option<String> {
    opts.get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

impl CacheConfig {
    /// Cache options of one action, or `None` when `enableCache` is not true.
    pub fn from_action(opts: &HashMap<String, Value>) -> Option<Self> {
        if !opt_bool(opts, "enableCache").unwrap_or(false) {
            return None;
        }
        let defaults = Self::default();
        Some(Self {
            memory_threshold: opt_u64(opts, "memoryCacheThreshold")
                .map(|n| n as usize)
                .unwrap_or(defaults.memory_threshold)
                .max(1),
            max_disk: opt_u64(opts, "maxDiskCache")
                .map(|n| n as usize)
                .unwrap_or(defaults.max_disk),
            page_size: opt_u64(opts, "bufferPageSize")
                .map(|n| n as usize)
                .unwrap_or(defaults.page_size)
                .max(1),
            resend_interval: opt_u64(opts, "resendInterval")
                .map(Duration::from_millis)
                .unwrap_or(defaults.resend_interval),
            clean_at_stop: opt_bool(opts, "cleanCacheAtStop").unwrap_or(defaults.clean_at_stop),
            priority: match opt_i64(opts, "resendPriority").unwrap_or(0) {
                p if p < 0 => ResendPriority::LiveFirst,
                0 => ResendPriority::Equal,
                _ => ResendPriority::CacheFirst,
            },
            indicator_field: opt_str(opts, "resendIndicatorField"),
            destination: opt_str(opts, "resendDestination"),
        })
    }
}

/// One persisted page file: `page-<seq>.jsonl`, one record per line.
#[derive(Debug)]
struct Page {
    seq: u64,
    count: usize,
}

/// First sequence number for pages written after open; pages that must sort
/// before existing ones (the memory head at stop) count down from below.
const SEQ_BASE: u64 = 1 << 40;

/// Two-tier FIFO cache for one sink action.
pub struct SinkCache {
    cfg: CacheConfig,
    dir: PathBuf,
    head: VecDeque<StreamRecord>,
    pages: VecDeque<Page>,
    tail: Vec<StreamRecord>,
    disk_records: usize,
    next_seq: u64,
    /// Records dropped because memory and disk were full.
    pub dropped: u64,
}

impl SinkCache {
    /// Opens the cache directory, adopting pages left by a previous run.
    pub fn open(cfg: CacheConfig, dir: PathBuf) -> Self {
        let mut cache = Self {
            cfg,
            dir,
            head: VecDeque::new(),
            pages: VecDeque::new(),
            tail: Vec::new(),
            disk_records: 0,
            next_seq: SEQ_BASE,
            dropped: 0,
        };
        cache.adopt_pages();
        cache
    }

    pub fn config(&self) -> &CacheConfig {
        &self.cfg
    }

    pub fn len(&self) -> usize {
        self.head.len() + self.disk_records + self.tail.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn disk_enabled(&self) -> bool {
        self.cfg.max_disk >= self.cfg.page_size
    }

    fn page_path(&self, seq: u64) -> PathBuf {
        self.dir.join(format!("page-{:020}.jsonl", seq))
    }

    fn adopt_pages(&mut self) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        let mut found: Vec<u64> = entries
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.strip_prefix("page-")?
                    .strip_suffix(".jsonl")?
                    .parse::<u64>()
                    .ok()
            })
            .collect();
        found.sort_unstable();
        for seq in found {
            let count = std::fs::File::open(self.page_path(seq))
                .map(|f| std::io::BufReader::new(f).lines().count())
                .unwrap_or(0);
            if count == 0 {
                let _ = std::fs::remove_file(self.page_path(seq));
                continue;
            }
            self.next_seq = self.next_seq.max(seq + 1);
            self.disk_records += count;
            self.pages.push_back(Page { seq, count });
        }
        while self.disk_records > self.cfg.max_disk {
            if !self.drop_oldest_page() {
                break;
            }
        }
    }

    /// Appends a record whose send failed.
    pub fn push(&mut self, record: StreamRecord) {
        if !self.disk_enabled() {
            if self.head.len() >= self.cfg.memory_threshold {
                self.head.pop_front();
                self.dropped += 1;
            }
            self.head.push_back(record);
            return;
        }
        if self.pages.is_empty()
            && self.tail.is_empty()
            && self.head.len() < self.cfg.memory_threshold
        {
            self.head.push_back(record);
            return;
        }
        self.tail.push(record);
        if self.tail.len() >= self.cfg.page_size {
            self.spill_tail();
        }
    }

    /// Writes the tail as a page, dropping the oldest data if disk is full.
    fn spill_tail(&mut self) {
        if self.tail.is_empty() {
            return;
        }
        while self.disk_records + self.tail.len() > self.cfg.max_disk {
            // eKuiper rotation: the oldest in-memory records are discarded and
            // the oldest disk page moves into memory, freeing its disk budget.
            let n = self.cfg.page_size.min(self.head.len());
            self.head.drain(..n);
            self.dropped += n as u64;
            if !self.load_oldest_page() {
                if n == 0 {
                    // Nothing older left to drop: discard the oldest tail records.
                    let excess = (self.disk_records + self.tail.len())
                        .saturating_sub(self.cfg.max_disk)
                        .min(self.tail.len());
                    self.tail.drain(..excess);
                    self.dropped += excess as u64;
                }
                break;
            }
        }
        if self.tail.is_empty() {
            return;
        }
        let seq = self.next_seq;
        let records = std::mem::take(&mut self.tail);
        match self.write_page(seq, &records) {
            Ok(()) => {
                self.next_seq += 1;
                self.disk_records += records.len();
                self.pages.push_back(Page {
                    seq,
                    count: records.len(),
                });
            }
            Err(e) => {
                // Disk unavailable: keep the records in memory rather than
                // lose them; the memory head may exceed its threshold.
                tracing::warn!("sink cache page write to {:?} failed: {}", self.dir, e);
                self.head.extend(records);
            }
        }
    }

    fn write_page(&self, seq: u64, records: &[StreamRecord]) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        let mut buf = Vec::with_capacity(records.len() * 128);
        for record in records {
            serde_json::to_writer(&mut buf, record).map_err(std::io::Error::other)?;
            buf.push(b'\n');
        }
        let tmp = self.dir.join(format!("page-{:020}.tmp", seq));
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&buf)?;
        drop(file);
        std::fs::rename(tmp, self.page_path(seq))
    }

    fn drop_oldest_page(&mut self) -> bool {
        let Some(page) = self.pages.pop_front() else {
            return false;
        };
        let _ = std::fs::remove_file(self.page_path(page.seq));
        self.disk_records -= page.count;
        self.dropped += page.count as u64;
        true
    }

    /// Moves the oldest disk page to the back of the memory head.
    fn load_oldest_page(&mut self) -> bool {
        let Some(page) = self.pages.pop_front() else {
            return false;
        };
        let path = self.page_path(page.seq);
        self.disk_records -= page.count;
        let mut loaded = 0usize;
        if let Ok(file) = std::fs::File::open(&path) {
            for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
                match serde_json::from_str::<StreamRecord>(&line) {
                    Ok(record) => {
                        self.head.push_back(record);
                        loaded += 1;
                    }
                    Err(e) => tracing::warn!("sink cache: skipping corrupt record: {}", e),
                }
            }
        }
        self.dropped += page.count.saturating_sub(loaded) as u64;
        let _ = std::fs::remove_file(path);
        true
    }

    /// The oldest cached record (refilling memory from disk if needed).
    pub fn front(&mut self) -> Option<&StreamRecord> {
        if self.head.is_empty() && !self.load_oldest_page() && !self.tail.is_empty() {
            self.head.extend(self.tail.drain(..));
        }
        self.head.front()
    }

    /// Removes the oldest record after a successful resend.
    pub fn pop_front(&mut self) -> Option<StreamRecord> {
        self.front()?;
        self.head.pop_front()
    }

    /// Rule stop: persist everything (or delete it with `cleanCacheAtStop`).
    pub fn close(mut self) {
        if self.cfg.clean_at_stop {
            let _ = std::fs::remove_dir_all(&self.dir);
            return;
        }
        self.spill_tail();
        if self.head.is_empty() {
            return;
        }
        // The head is older than every page: write it below the first seq.
        let head: Vec<StreamRecord> = self.head.drain(..).collect();
        let first = self.pages.front().map(|p| p.seq).unwrap_or(self.next_seq);
        let chunks: Vec<&[StreamRecord]> = head.chunks(self.cfg.page_size).collect();
        let base = first.saturating_sub(chunks.len() as u64);
        for (i, chunk) in chunks.iter().enumerate() {
            if let Err(e) = self.write_page(base + i as u64, chunk) {
                tracing::warn!(
                    "sink cache: {} records lost at stop ({:?}): {}",
                    chunk.len(),
                    self.dir,
                    e
                );
            }
        }
    }
}

/// The record as resent: the indicator field set to `true` when configured.
pub fn resend_copy(cfg: &CacheConfig, record: &StreamRecord) -> StreamRecord {
    let mut copy = record.clone();
    if let Some(field) = &cfg.indicator_field {
        copy.data.insert(field.clone(), Value::Bool(true));
    }
    copy
}

/// Cache directory for one rule action.
pub fn cache_dir(root: &Path, rule_id: &str, action_index: usize) -> PathBuf {
    let safe: String = rule_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    root.join(safe).join(action_index.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rekuiper-sinkcache-{}-{}-{}",
            name,
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn rec(i: u64) -> StreamRecord {
        let mut data = HashMap::new();
        data.insert("seq".to_string(), json!(i));
        StreamRecord::with_timestamp(data, i as i64)
    }

    fn cfg(memory: usize, disk: usize, page: usize) -> CacheConfig {
        CacheConfig {
            memory_threshold: memory,
            max_disk: disk,
            page_size: page,
            ..CacheConfig::default()
        }
    }

    fn drain(cache: &mut SinkCache) -> Vec<u64> {
        let mut out = Vec::new();
        while let Some(r) = cache.pop_front() {
            out.push(r.data["seq"].as_u64().unwrap());
        }
        out
    }

    #[test]
    fn parses_ekuiper_action_options() {
        let opts: HashMap<String, Value> = serde_json::from_value(json!({
            "enableCache": true,
            "memoryCacheThreshold": 10,
            "maxDiskCache": "500",
            "bufferPageSize": 5,
            "resendInterval": 20,
            "cleanCacheAtStop": "true",
            "resendPriority": -1,
            "resendIndicatorField": "resent",
            "resendDestination": "vehicles/replay"
        }))
        .unwrap();
        let c = CacheConfig::from_action(&opts).unwrap();
        assert_eq!(c.memory_threshold, 10);
        assert_eq!(c.max_disk, 500);
        assert_eq!(c.page_size, 5);
        assert_eq!(c.resend_interval, Duration::from_millis(20));
        assert!(c.clean_at_stop);
        assert_eq!(c.priority, ResendPriority::LiveFirst);
        assert_eq!(c.indicator_field.as_deref(), Some("resent"));
        assert_eq!(c.destination.as_deref(), Some("vehicles/replay"));
        let off: HashMap<String, Value> = serde_json::from_value(json!({"url": "x"})).unwrap();
        assert!(CacheConfig::from_action(&off).is_none());
    }

    #[test]
    fn spills_to_disk_and_resends_in_order() {
        let dir = temp_dir("order");
        let mut cache = SinkCache::open(cfg(4, 1000, 3), dir.clone());
        for i in 0..20 {
            cache.push(rec(i));
        }
        assert_eq!(cache.len(), 20);
        assert!(cache.pages.len() >= 5, "records spilled to disk pages");
        // Interleave a few pushes during resend: FIFO holds overall.
        let mut out = Vec::new();
        for _ in 0..6 {
            out.push(cache.pop_front().unwrap().data["seq"].as_u64().unwrap());
        }
        for i in 20..25 {
            cache.push(rec(i));
        }
        out.extend(drain(&mut cache));
        assert_eq!(out, (0..25).collect::<Vec<_>>());
        assert!(cache.is_empty());
        assert_eq!(cache.dropped, 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn full_cache_drops_oldest_and_counts() {
        let dir = temp_dir("full");
        let mut cache = SinkCache::open(cfg(4, 6, 3), dir.clone());
        for i in 0..30 {
            cache.push(rec(i));
        }
        let out = drain(&mut cache);
        assert_eq!(
            out.len() as u64 + cache.dropped,
            30,
            "every record delivered or counted"
        );
        assert!(cache.dropped > 0);
        assert!(
            out.windows(2).all(|w| w[0] < w[1]),
            "survivors stay in order: {out:?}"
        );
        assert_eq!(*out.last().unwrap(), 29, "newest data survives");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn memory_only_when_disk_budget_below_a_page() {
        let dir = temp_dir("memonly");
        let mut cache = SinkCache::open(cfg(3, 0, 256), dir.clone());
        for i in 0..5 {
            cache.push(rec(i));
        }
        assert_eq!(drain(&mut cache), vec![2, 3, 4]);
        assert_eq!(cache.dropped, 2);
        assert!(!dir.exists(), "no disk writes");
    }

    #[test]
    fn close_persists_and_reopen_replays_in_order() {
        let dir = temp_dir("restart");
        let mut cache = SinkCache::open(cfg(4, 1000, 3), dir.clone());
        for i in 0..11 {
            cache.push(rec(i));
        }
        assert_eq!(cache.pop_front().unwrap().data["seq"], json!(0));
        cache.close();
        let mut reopened = SinkCache::open(cfg(4, 1000, 3), dir.clone());
        assert_eq!(reopened.len(), 10);
        assert_eq!(drain(&mut reopened), (1..11).collect::<Vec<_>>());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn clean_at_stop_removes_cache() {
        let dir = temp_dir("clean");
        let mut c = cfg(1, 1000, 2);
        c.clean_at_stop = true;
        let mut cache = SinkCache::open(c.clone(), dir.clone());
        for i in 0..7 {
            cache.push(rec(i));
        }
        assert!(dir.exists());
        cache.close();
        assert!(!dir.exists());
        assert!(SinkCache::open(c, dir).is_empty());
    }

    #[test]
    fn resend_copy_sets_indicator() {
        let mut c = CacheConfig::default();
        let r = rec(1);
        assert!(!resend_copy(&c, &r).data.contains_key("resent"));
        c.indicator_field = Some("resent".to_string());
        assert_eq!(resend_copy(&c, &r).data["resent"], json!(true));
    }
}
