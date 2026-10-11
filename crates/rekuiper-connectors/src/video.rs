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
    #[serde(default = "default_video_interval", deserialize_with = "deserialize_interval")]
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
    #[serde(default = "default_ffmpeg_path", rename = "ffmpegPath", alias = "ffmpeg_path")]
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

/// Native pure-Rust Video Source connector spawning FFmpeg to extract
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
    pub fn build_command(&self) -> Command {
        let mut cmd = Command::new(&self.config.ffmpeg_path);
        cmd.arg("-nostdin")
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error");

        // Custom input arguments prior to -i
        for (k, v) in &self.config.input_args {
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
            let fps_filter = if interval_ms >= 1000 && interval_ms % 1000 == 0 {
                format!("fps=1/{}", interval_ms / 1000)
            } else if 1000 % interval_ms == 0 {
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
        cmd
    }

    /// Spawns the background video capture pipeline.
    pub fn spawn(self, mut cancel_rx: watch::Receiver<bool>) {
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

                tracing::info!("Starting video source pipeline for URL: {}", self.config.url);
                let mut cmd = self.build_command();

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
                                        let b64_img = base64::engine::general_purpose::STANDARD.encode(&frame);
                                        let mut record_data = HashMap::new();
                                        record_data.insert("image".to_string(), Value::String(b64_img));
                                        record_data.insert("url".to_string(), Value::String(self.config.url.clone()));
                                        record_data.insert("size".to_string(), Value::Number(frame.len().into()));
                                        record_data.insert("format".to_string(), Value::String(self.config.codec.clone()));
                                        record_data.insert("ts".to_string(), Value::Number(Utc::now().timestamp_millis().into()));

                                        let mut rec = StreamRecord::new(record_data);
                                        let mut meta = HashMap::new();
                                        meta.insert("url".to_string(), Value::String(self.config.url.clone()));
                                        rec.data.insert(crate::META_KEY.to_string(), Value::Object(meta.into_iter().collect()));

                                        let _ = self.tx.send(rec).await;
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
        });
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
    fn test_build_command_structure() {
        let mut input_args = HashMap::new();
        input_args.insert("rtsp_transport".to_string(), "tcp".to_string());

        let cfg = VideoSourceConfig {
            url: "rtsp://localhost:8554/stream".to_string(),
            interval: 1000,
            codec: "mjpeg".to_string(),
            debug_resp: false,
            input_args,
            ffmpeg_path: "ffmpeg".to_string(),
        };

        let bus = rekuiper_core::StreamBus::default();
        let tx = bus.get_or_create("video_test");
        let source = VideoSource::new(cfg, tx);
        let cmd = source.build_command();
        // Verify Command program
        assert_eq!(cmd.as_std().get_program(), "ffmpeg");
    }
}
