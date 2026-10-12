use anyhow::Result;
use base64::Engine;
use chrono::Utc;
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::watch;

/// Maximum buffer size (20 MB) for buffering incoming video stream chunks
/// before dropping incomplete/corrupted frames.
pub const MAX_JPEG_BUFFER_BYTES: usize = 20 * 1024 * 1024;
pub const SOI: [u8; 2] = [0xFF, 0xD8];
pub const EOI: [u8; 2] = [0xFF, 0xD9];

/// Video Source connector configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoSourceConfig {
    /// Video streaming URL (e.g. `rtsp://localhost:8554/stream`, `http://...`, or video file).
    #[serde(default)]
    pub url: String,

    /// Frame extraction interval in milliseconds (default: 1000 ms = 1 frame/s).
    /// Accepts numbers (e.g. `1000`) or duration strings (e.g. `"500ms"`, `"1s"`, `"2m"`).
    #[serde(
        default = "default_video_interval",
        deserialize_with = "deserialize_interval"
    )]
    pub interval: u64,

    /// Target video frame codec. Defaults to `"mjpeg"`.
    #[serde(default = "default_video_codec")]
    pub codec: String,

    /// When true, logs FFmpeg stderr output for diagnostics.
    #[serde(default, rename = "debugResp", alias = "debug_resp")]
    pub debug_resp: bool,

    /// Optional map of input arguments passed to FFmpeg prior to the `-i` parameter.
    /// E.g. `{"rtsp_transport": "tcp"}`.
    #[serde(default, rename = "inputArgs", alias = "input_args")]
    pub input_args: HashMap<String, String>,

    /// Path or binary name of FFmpeg (default: `"ffmpeg"`).
    #[serde(
        default = "default_ffmpeg_path",
        rename = "ffmpegPath",
        alias = "ffmpeg_path"
    )]
    pub ffmpeg_path: String,
}

fn default_video_interval() -> u64 {
    1000
}

fn default_video_codec() -> String {
    "mjpeg".to_string()
}

fn default_ffmpeg_path() -> String {
    "ffmpeg".to_string()
}

impl Default for VideoSourceConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            interval: default_video_interval(),
            codec: default_video_codec(),
            debug_resp: false,
            input_args: HashMap::new(),
            ffmpeg_path: default_ffmpeg_path(),
        }
    }
}

impl VideoSourceConfig {
    pub fn set_interval_str(&mut self, s: &str) {
        self.interval = parse_interval_str(s);
    }
}

fn deserialize_interval<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = Value::deserialize(deserializer)?;
    Ok(parse_interval_value(&v))
}

pub fn parse_interval_value(val: &Value) -> u64 {
    match val {
        Value::Number(n) => n.as_u64().unwrap_or(1000),
        Value::String(s) => parse_interval_str(s),
        _ => 1000,
    }
}

pub fn parse_interval_str(s: &str) -> u64 {
    let trimmed = s.trim().to_lowercase();
    if trimmed.ends_with("ms") {
        let num_str = trimmed.trim_end_matches("ms").trim();
        num_str.parse::<u64>().unwrap_or(1000)
    } else if trimmed.ends_with('s') {
        let num_str = trimmed.trim_end_matches('s').trim();
        if let Ok(f) = num_str.parse::<f64>() {
            (f * 1000.0).max(0.0) as u64
        } else {
            1000
        }
    } else if trimmed.ends_with('m') {
        let num_str = trimmed.trim_end_matches('m').trim();
        if let Ok(f) = num_str.parse::<f64>() {
            (f * 60000.0).max(0.0) as u64
        } else {
            60000
        }
    } else {
        trimmed.parse::<u64>().unwrap_or(1000)
    }
}

/// Splits a continuous stream buffer into discrete JPEG image frames
/// delimited by SOI (`0xFF 0xD8`) and EOI (`0xFF 0xD9`) markers.
pub fn split_jpegs(buf: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    loop {
        if buf.len() < 2 {
            break;
        }

        // 1. Locate Start of Image (SOI: 0xFF, 0xD8)
        let soi_pos = if buf[0] == 0xFF && buf[1] == 0xD8 {
            Some(0)
        } else {
            buf.windows(2).position(|w| w == SOI)
        };

        let start = match soi_pos {
            Some(pos) => pos,
            None => {
                // Keep the trailing byte if it is 0xFF (could be first byte of split SOI)
                if buf.last() == Some(&0xFF) {
                    buf.drain(0..buf.len() - 1);
                } else {
                    buf.clear();
                }
                break;
            }
        };

        // Discard any preceding garbage
        if start > 0 {
            buf.drain(0..start);
        }

        // Buffer now starts with SOI at index 0..2.
        // 2. Search for End of Image (EOI: 0xFF, 0xD9) starting at index 2
        if buf.len() < 4 {
            break;
        }

        let eoi_rel = buf[2..].windows(2).position(|w| w == EOI);
        match eoi_rel {
            Some(rel) => {
                // End marker is at 2 + rel (the 0xFF byte), and ends at 2 + rel + 1 (the 0xD9 byte)
                // Total frame length is 2 + rel + 2
                let frame_len = 2 + rel + 2;
                let frame: Vec<u8> = buf.drain(0..frame_len).collect();
                frames.push(frame);
            }
            None => {
                // Prevent memory exhaustion on malformed/unbounded non-terminating streams
                if buf.len() > MAX_JPEG_BUFFER_BYTES {
                    tracing::warn!(
                        "Video frame buffer exceeded {} bytes without finding EOI marker; dropping partial buffer",
                        MAX_JPEG_BUFFER_BYTES
                    );
                    buf.clear();
                }
                break;
            }
        }
    }
    frames
}

/// Allowed network streaming protocols for video source URL.
/// Local filesystem paths (e.g. `file://`, `/etc/...`, `C:\...`) are prohibited to prevent local file exfiltration.
pub const ALLOWED_VIDEO_SCHEMES: &[&str] = &[
    "rtsp://", "rtsps://", "http://", "https://", "rtmp://", "rtmps://", "srt://", "udp://",
    "tcp://",
];

/// Whitelist of safe input arguments passed to FFmpeg prior to `-i`.
pub const ALLOWED_INPUT_ARGS: &[&str] = &[
    "rtsp_transport",
    "stimeout",
    "rw_timeout",
    "timeout",
    "re",
    "buffer_size",
    "max_delay",
    "analyzeduration",
    "probesize",
    "fflags",
    "fpsprobesize",
    "c:v",
    "vcodec",
    "f",
    "framerate",
    "r",
    "video_size",
    "s",
    "an",
    "sn",
    "dn",
];

/// Returns true if the video source URL uses an allowed network streaming protocol.
pub fn is_allowed_video_url(url: &str) -> bool {
    let trimmed = url.trim();
    let lower = trimmed.to_lowercase();
    ALLOWED_VIDEO_SCHEMES
        .iter()
        .any(|scheme| lower.starts_with(scheme))
}

/// Returns true if the given input argument key is in the allowed whitelist.
pub fn is_allowed_input_arg(key: &str) -> bool {
    let clean = key.trim_start_matches('-').to_lowercase();
    ALLOWED_INPUT_ARGS.contains(&clean.as_str())
}

/// Built-in Video Source connector spawning FFmpeg to extract
/// frames from RTSP / HTTP video streams into stream records.
pub struct VideoSource {
    pub config: VideoSourceConfig,
    pub tx: StreamSender,
}

impl VideoSource {
    pub fn new(config: VideoSourceConfig, tx: StreamSender) -> Self {
        Self { config, tx }
    }

    /// Builds the FFmpeg Command with arguments matching upstream eKuiper `image2pipe` stream extraction.
    pub fn build_command(&self) -> Result<Command, String> {
        let trimmed_url = self.config.url.trim();
        if !trimmed_url.is_empty() && !is_allowed_video_url(trimmed_url) {
            return Err(format!(
                "invalid video source URL '{}': only streaming protocols (rtsp, rtsps, http, https, rtmp, rtmps, srt, udp, tcp) are allowed",
                self.config.url
            ));
        }

        let ffmpeg_prog = if self.config.ffmpeg_path.is_empty() {
            "ffmpeg"
        } else {
            let p = std::path::Path::new(&self.config.ffmpeg_path);
            let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            if stem.eq_ignore_ascii_case("ffmpeg") {
                &self.config.ffmpeg_path
            } else {
                tracing::warn!(
                    "Configured ffmpegPath '{}' does not have stem 'ffmpeg'; falling back to 'ffmpeg'",
                    self.config.ffmpeg_path
                );
                "ffmpeg"
            }
        };
        let mut cmd = Command::new(ffmpeg_prog);
        cmd.kill_on_drop(true);
        cmd.arg("-nostdin")
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error");

        // Custom input arguments prior to -i
        let mut sorted_args: Vec<_> = self.config.input_args.iter().collect();
        sorted_args.sort_by_key(|(k, _)| *k);
        for (k, v) in sorted_args {
            if !is_allowed_input_arg(k) {
                return Err(format!(
                    "forbidden or unrecognized FFmpeg input argument '{}': only network streaming parameters are allowed",
                    k
                ));
            }
            let flag = if k.starts_with('-') {
                k.clone()
            } else {
                format!("-{}", k)
            };
            cmd.arg(flag);
            if !v.is_empty() {
                cmd.arg(v);
            }
        }

        cmd.arg("-i").arg(&self.config.url);

        // Frame rate filter if interval > 0
        if self.config.interval > 0 {
            let interval_ms = self.config.interval;
            let fps_filter = if interval_ms > 1000 && interval_ms.is_multiple_of(1000) {
                format!("fps=1/{}", interval_ms / 1000)
            } else if 1000u64.is_multiple_of(interval_ms) {
                format!("fps={}", 1000 / interval_ms)
            } else {
                format!("fps={:.3}", 1000.0 / interval_ms as f64)
            };
            cmd.arg("-vf").arg(fps_filter);
        }

        cmd.arg("-f")
            .arg("image2pipe")
            .arg("-vcodec")
            .arg(&self.config.codec)
            .arg("-q:v")
            .arg("2")
            .arg("pipe:1");

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        Ok(cmd)
    }

    /// Helper to convert a decoded image frame into a standardized StreamRecord.
    pub fn create_frame_record(frame: &[u8], url: &str, codec: &str) -> StreamRecord {
        let b64_img = base64::engine::general_purpose::STANDARD.encode(frame);
        let mut record_data = HashMap::new();
        record_data.insert("image".to_string(), Value::String(b64_img));
        record_data.insert("url".to_string(), Value::String(url.to_string()));
        record_data.insert("size".to_string(), Value::Number(frame.len().into()));
        record_data.insert("format".to_string(), Value::String(codec.to_string()));
        record_data.insert(
            "ts".to_string(),
            Value::Number(Utc::now().timestamp_millis().into()),
        );

        let mut rec = StreamRecord::new(record_data);
        let mut meta = HashMap::new();
        meta.insert("url".to_string(), Value::String(url.to_string()));
        rec.data.insert(
            crate::META_KEY.to_string(),
            Value::Object(meta.into_iter().collect()),
        );
        rec
    }

    /// Spawns the background video capture pipeline.
    pub fn spawn(self, mut cancel_rx: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            let max_backoff = Duration::from_secs(5);

            while !*cancel_rx.borrow() {
                if self.config.url.is_empty() {
                    tracing::warn!("Video source URL is empty; waiting for configuration...");
                    tokio::select! {
                        _ = cancel_rx.changed() => break,
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    }
                    continue;
                }

                tracing::info!(
                    "Starting video source pipeline for URL: {}",
                    self.config.url
                );
                let mut cmd = match self.build_command() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(
                            "Video source validation failed: {}. Aborting video stream pipeline.",
                            e
                        );
                        break;
                    }
                };

                let mut child = match cmd.spawn() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(
                            "Failed to execute FFmpeg at '{}': {}. Video source requires ffmpeg installed in PATH.",
                            self.config.ffmpeg_path,
                            e
                        );
                        tokio::select! {
                            _ = cancel_rx.changed() => break,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(max_backoff);
                        continue;
                    }
                };

                let mut stdout = match child.stdout.take() {
                    Some(out) => out,
                    None => {
                        let _ = child.kill().await;
                        continue;
                    }
                };

                let stderr = child.stderr.take();
                let debug_resp = self.config.debug_resp;
                let (last_stderr_tx, last_stderr_rx) = tokio::sync::watch::channel(String::new());

                if let Some(err_pipe) = stderr {
                    tokio::spawn(async move {
                        let mut reader = BufReader::new(err_pipe).lines();
                        while let Ok(Some(line)) = reader.next_line().await {
                            if debug_resp {
                                tracing::info!("ffmpeg stderr: {}", line);
                            }
                            let _ = last_stderr_tx.send(line);
                        }
                    });
                }

                let mut buf = Vec::with_capacity(64 * 1024);
                let mut read_chunk = [0u8; 16384];
                while !*cancel_rx.borrow() {
                    tokio::select! {
                        _ = cancel_rx.changed() => {
                            break;
                        }
                        res = stdout.read(&mut read_chunk) => {
                            match res {
                                Ok(0) => {
                                    // Process reached EOF
                                    break;
                                }
                                Ok(n) => {
                                    buf.extend_from_slice(&read_chunk[..n]);
                                    let frames = split_jpegs(&mut buf);
                                    for frame in frames {
                                        let rec = Self::create_frame_record(&frame, &self.config.url, &self.config.codec);
                                        if matches!(self.tx.send(rec).await, Err(rekuiper_core::PublishError::Closed)) {
                                            tracing::info!("Video source receiver closed; terminating video pipeline");
                                            let _ = child.kill().await;
                                            let _ = child.wait().await;
                                            return;
                                        }
                                    }
                                    backoff = Duration::from_millis(500);
                                }
                                Err(e) => {
                                    tracing::warn!("Error reading from ffmpeg stdout pipe: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                }

                // Cleanup process
                let _ = child.kill().await;
                let _ = child.wait().await;

                if *cancel_rx.borrow() {
                    break;
                }

                let last_err = last_stderr_rx.borrow().clone();
                if !last_err.is_empty() {
                    tracing::warn!("Video source FFmpeg exited: {}", last_err);
                }

                tokio::select! {
                    _ = cancel_rx.changed() => break,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(max_backoff);
            }
            tracing::info!("Video source background task stopped");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval_str() {
        assert_eq!(parse_interval_str("1000"), 1000);
        assert_eq!(parse_interval_str("500ms"), 500);
        assert_eq!(parse_interval_str("1s"), 1000);
        assert_eq!(parse_interval_str("2.5s"), 2500);
        assert_eq!(parse_interval_str("1m"), 60000);
        assert_eq!(parse_interval_str("invalid"), 1000);
    }

    #[test]
    fn test_video_source_config_deserialize() {
        let json_str = r#"{
            "url": "rtsp://camera.local:8554/live",
            "interval": "500ms",
            "codec": "mjpeg",
            "debugResp": true,
            "inputArgs": {
                "rtsp_transport": "tcp"
            },
            "ffmpegPath": "/usr/bin/ffmpeg"
        }"#;

        let cfg: VideoSourceConfig = serde_json::from_str(json_str).expect("Valid config");
        assert_eq!(cfg.url, "rtsp://camera.local:8554/live");
        assert_eq!(cfg.interval, 500);
        assert_eq!(cfg.codec, "mjpeg");
        assert!(cfg.debug_resp);
        assert_eq!(cfg.input_args.get("rtsp_transport").unwrap(), "tcp");
        assert_eq!(cfg.ffmpeg_path, "/usr/bin/ffmpeg");
    }

    #[test]
    fn test_split_jpegs_single_frame() {
        let mut buf = vec![0xFF, 0xD8, 0x11, 0x22, 0x33, 0xFF, 0xD9];
        let frames = split_jpegs(&mut buf);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], vec![0xFF, 0xD8, 0x11, 0x22, 0x33, 0xFF, 0xD9]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_split_jpegs_multiple_frames() {
        let mut buf = vec![
            0xFF, 0xD8, 0x01, 0x02, 0xFF, 0xD9, // Frame 1
            0xFF, 0xD8, 0x03, 0x04, 0x05, 0xFF, 0xD9, // Frame 2
        ];
        let frames = split_jpegs(&mut buf);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], vec![0xFF, 0xD8, 0x01, 0x02, 0xFF, 0xD9]);
        assert_eq!(frames[1], vec![0xFF, 0xD8, 0x03, 0x04, 0x05, 0xFF, 0xD9]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_split_jpegs_leading_garbage() {
        let mut buf = vec![
            0x00, 0x01, 0x02, 0xAA, // Garbage
            0xFF, 0xD8, 0x55, 0x66, 0xFF, 0xD9, // Valid Frame
        ];
        let frames = split_jpegs(&mut buf);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], vec![0xFF, 0xD8, 0x55, 0x66, 0xFF, 0xD9]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_split_jpegs_partial_chunk() {
        let mut buf = vec![0xFF, 0xD8, 0x11, 0x22]; // Incomplete frame
        let frames = split_jpegs(&mut buf);
        assert_eq!(frames.len(), 0);
        assert_eq!(buf, vec![0xFF, 0xD8, 0x11, 0x22]);

        // Append the rest of the frame
        buf.extend_from_slice(&[0x33, 0xFF, 0xD9]);
        let frames2 = split_jpegs(&mut buf);
        assert_eq!(frames2.len(), 1);
        assert_eq!(frames2[0], vec![0xFF, 0xD8, 0x11, 0x22, 0x33, 0xFF, 0xD9]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_split_jpegs_split_soi_across_chunks() {
        let mut buf = vec![0x10, 0x20, 0xFF]; // Garbage followed by lone 0xFF
        let frames = split_jpegs(&mut buf);
        assert_eq!(frames.len(), 0);
        assert_eq!(buf, vec![0xFF]); // Preserves 0xFF

        // Append 0xD8 and content
        buf.extend_from_slice(&[0xD8, 0x99, 0xFF, 0xD9]);
        let frames2 = split_jpegs(&mut buf);
        assert_eq!(frames2.len(), 1);
        assert_eq!(frames2[0], vec![0xFF, 0xD8, 0x99, 0xFF, 0xD9]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_build_command_structure_and_fps() {
        let mut input_args = HashMap::new();
        input_args.insert("rtsp_transport".to_string(), "tcp".to_string());

        let cfg = VideoSourceConfig {
            url: "rtsp://localhost:8554/stream".to_string(),
            interval: 1000,
            codec: "mjpeg".to_string(),
            debug_resp: false,
            input_args,
            ffmpeg_path: "/usr/local/bin/ffmpeg".to_string(),
        };

        let bus = rekuiper_core::StreamBus::default();
        let tx = bus.get_or_create("video_test");
        let source = VideoSource::new(cfg, tx.clone());
        let cmd = source.build_command().unwrap();

        assert_eq!(cmd.as_std().get_program(), "/usr/local/bin/ffmpeg");
        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();

        assert!(args.contains(&"-nostdin".to_string()));
        assert!(args.contains(&"-hide_banner".to_string()));
        assert!(args.contains(&"-loglevel".to_string()));
        assert!(args.contains(&"error".to_string()));
        assert!(args.contains(&"-rtsp_transport".to_string()));
        assert!(args.contains(&"tcp".to_string()));
        assert!(args.contains(&"-i".to_string()));
        assert!(args.contains(&"rtsp://localhost:8554/stream".to_string()));
        assert!(args.contains(&"-vf".to_string()));
        assert!(args.contains(&"fps=1".to_string()));
        assert!(args.contains(&"-f".to_string()));
        assert!(args.contains(&"image2pipe".to_string()));
        assert!(args.contains(&"-vcodec".to_string()));
        assert!(args.contains(&"mjpeg".to_string()));
        assert!(args.contains(&"-q:v".to_string()));
        assert!(args.contains(&"2".to_string()));
        assert!(args.contains(&"pipe:1".to_string()));

        // Test interval = 500 ms -> fps=2
        let mut cfg500 = source.config.clone();
        cfg500.interval = 500;
        let cmd500 = VideoSource::new(cfg500, tx.clone())
            .build_command()
            .unwrap();
        let args500: Vec<String> = cmd500
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        let vf_idx = args500.iter().position(|a| a == "-vf").unwrap();
        assert_eq!(args500[vf_idx + 1], "fps=2");

        // Test interval = 2000 ms -> fps=1/2
        let mut cfg2000 = source.config.clone();
        cfg2000.interval = 2000;
        let cmd2000 = VideoSource::new(cfg2000, tx.clone())
            .build_command()
            .unwrap();
        let args2000: Vec<String> = cmd2000
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        let vf_idx = args2000.iter().position(|a| a == "-vf").unwrap();
        assert_eq!(args2000[vf_idx + 1], "fps=1/2");

        // Test interval = 2500 ms -> fps=0.400
        let mut cfg2500 = source.config.clone();
        cfg2500.interval = 2500;
        let cmd2500 = VideoSource::new(cfg2500, tx.clone())
            .build_command()
            .unwrap();
        let args2500: Vec<String> = cmd2500
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        let vf_idx = args2500.iter().position(|a| a == "-vf").unwrap();
        assert_eq!(args2500[vf_idx + 1], "fps=0.400");

        // Test interval = 0 -> no -vf filter
        let mut cfg0 = source.config.clone();
        cfg0.interval = 0;
        let cmd0 = VideoSource::new(cfg0, tx.clone()).build_command().unwrap();
        let args0: Vec<String> = cmd0
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(!args0.contains(&"-vf".to_string()));
    }

    #[test]
    fn test_video_source_disallowed_urls() {
        let bus = rekuiper_core::StreamBus::default();
        let tx = bus.get_or_create("video_url_test");

        let hostile_urls = vec![
            "/etc/passwd",
            "/var/log/syslog",
            "C:\\Windows\\system.ini",
            "file:///etc/shadow",
            "file://C:/secret.txt",
            "relative/path/movie.mp4",
        ];

        for hostile in hostile_urls {
            let cfg = VideoSourceConfig {
                url: hostile.to_string(),
                ..Default::default()
            };
            let source = VideoSource::new(cfg, tx.clone());
            assert!(
                source.build_command().is_err(),
                "Hostile URL '{}' should be rejected",
                hostile
            );
        }

        // Allowed network streaming URLs
        let allowed_urls = vec![
            "rtsp://192.168.1.100:554/live",
            "rtsps://camera.local:322/stream",
            "http://10.0.0.1:8080/mjpeg",
            "https://stream.server.org/live.m3u8",
            "rtmp://live.stream.net/live",
            "rtmps://live.stream.net/live",
            "srt://127.0.0.1:9000",
            "udp://239.255.0.1:1234",
            "tcp://127.0.0.1:4000",
        ];

        for allowed in allowed_urls {
            let cfg = VideoSourceConfig {
                url: allowed.to_string(),
                ..Default::default()
            };
            let source = VideoSource::new(cfg, tx.clone());
            assert!(
                source.build_command().is_ok(),
                "Allowed URL '{}' should pass validation",
                allowed
            );
        }
    }

    #[test]
    fn test_video_source_input_args_whitelist() {
        let bus = rekuiper_core::StreamBus::default();
        let tx = bus.get_or_create("video_args_test");

        // Forbidden arguments (script execution, file outputs, etc.)
        let forbidden_args = vec!["filter_script", "y", "nostdin", "dumpgraph", "lavfi"];
        for forbidden in forbidden_args {
            let mut input_args = HashMap::new();
            input_args.insert(forbidden.to_string(), "true".to_string());
            let cfg = VideoSourceConfig {
                url: "rtsp://camera:554/live".to_string(),
                input_args,
                ..Default::default()
            };
            let source = VideoSource::new(cfg, tx.clone());
            assert!(
                source.build_command().is_err(),
                "Forbidden argument '{}' should be rejected",
                forbidden
            );
        }

        // Allowed network and codec options
        let mut valid_args = HashMap::new();
        valid_args.insert("rtsp_transport".to_string(), "tcp".to_string());
        valid_args.insert("stimeout".to_string(), "5000000".to_string());
        valid_args.insert("buffer_size".to_string(), "1024000".to_string());
        let valid_cfg = VideoSourceConfig {
            url: "rtsp://camera:554/live".to_string(),
            input_args: valid_args,
            ..Default::default()
        };
        let valid_source = VideoSource::new(valid_cfg, tx);
        assert!(valid_source.build_command().is_ok());
    }

    #[test]
    fn test_create_frame_record() {
        let fake_jpeg = vec![0xFF, 0xD8, 0x12, 0x34, 0xFF, 0xD9];
        let record =
            VideoSource::create_frame_record(&fake_jpeg, "rtsp://camera:554/live", "mjpeg");

        let expected_b64 = base64::engine::general_purpose::STANDARD.encode(&fake_jpeg);
        assert_eq!(
            record.data.get("image").unwrap(),
            &Value::String(expected_b64)
        );
        assert_eq!(
            record.data.get("url").unwrap(),
            &Value::String("rtsp://camera:554/live".to_string())
        );
        assert_eq!(
            record.data.get("size").unwrap(),
            &Value::Number(fake_jpeg.len().into())
        );
        assert_eq!(
            record.data.get("format").unwrap(),
            &Value::String("mjpeg".to_string())
        );
        assert!(record.data.contains_key("ts"));

        let meta = record
            .data
            .get(crate::META_KEY)
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(
            meta.get("url").unwrap().as_str().unwrap(),
            "rtsp://camera:554/live"
        );
    }

    #[tokio::test]
    async fn test_video_pipeline_frame_ingestion() {
        let bus = rekuiper_core::StreamBus::default();
        let tx = bus.get_or_create("video_stream");
        let mut rx = bus.subscribe("video_stream");

        let frame1 = vec![0xFF, 0xD8, 0x01, 0x02, 0xFF, 0xD9];
        let frame2 = vec![0xFF, 0xD8, 0x03, 0x04, 0x05, 0xFF, 0xD9];

        let mut incoming_stream = Vec::new();
        incoming_stream.extend_from_slice(&frame1);
        incoming_stream.extend_from_slice(&frame2);

        let frames = split_jpegs(&mut incoming_stream);
        assert_eq!(frames.len(), 2);
        for frame in frames {
            let rec = VideoSource::create_frame_record(&frame, "http://live.cam/mjpeg", "mjpeg");
            tx.send(rec).await.unwrap();
        }

        let r1 = rx.recv().await.unwrap();
        assert_eq!(r1.data.get("size").unwrap(), &serde_json::json!(6));
        let r2 = rx.recv().await.unwrap();
        assert_eq!(r2.data.get("size").unwrap(), &serde_json::json!(7));
    }
}
