use base64::Engine;
use rekuiper_connectors::{VideoSource, VideoSourceConfig};
use rekuiper_core::StreamBus;
use std::time::Duration;
use tokio::sync::watch;

#[tokio::test]
#[ignore = "requires live external network connection and installed ffmpeg"]
async fn test_live_public_http_video_stream() {
    let check = std::process::Command::new("ffmpeg")
        .arg("-version")
        .output();
    let Ok(_) = check else {
        println!("FFmpeg not found; skipping public video test");
        return;
    };

    if tokio::net::TcpStream::connect("vjs.zencdn.net:80")
        .await
        .is_err()
    {
        println!("Public video host unreachable; skipping network test");
        return;
    }

    let bus = StreamBus::default();
    let tx = bus.get_or_create("public_video");
    let mut rx = bus.subscribe("public_video");

    let config = VideoSourceConfig {
        url: "http://vjs.zencdn.net/v/oceans.mp4".to_string(),
        interval: 1000,
        codec: "mjpeg".to_string(),
        ..Default::default()
    };

    let (cancel_tx, cancel_rx) = watch::channel(false);
    let source = VideoSource::new(config, tx);
    let handle = source.spawn(cancel_rx);

    // Wait up to 15 seconds for first frame
    let frame_rec = tokio::time::timeout(Duration::from_secs(15), rx.recv()).await;

    // Shut down video pipeline
    let _ = cancel_tx.send(true);
    let _ = handle.await;

    let Ok(Some(rec)) = frame_rec else {
        panic!("Timed out or failed waiting for video frame from public HTTP stream");
    };

    let b64_img = rec
        .data
        .get("image")
        .and_then(|v| v.as_str())
        .expect("missing image field");
    let img_bytes = base64::engine::general_purpose::STANDARD
        .decode(b64_img)
        .expect("invalid base64 image data");

    assert!(
        img_bytes.len() > 100,
        "Image bytes too short: {}",
        img_bytes.len()
    );
    // JPEG SOI marker 0xFF, 0xD8
    assert_eq!(&img_bytes[0..2], &[0xFF, 0xD8]);
    // JPEG EOI marker 0xFF, 0xD9
    assert_eq!(&img_bytes[img_bytes.len() - 2..], &[0xFF, 0xD9]);
    assert_eq!(
        rec.data.get("format").and_then(|v| v.as_str()),
        Some("mjpeg")
    );
}

#[tokio::test]
#[ignore = "requires hardware webcam and installed ffmpeg"]
async fn test_live_webcam_stream() {
    // 1. Check if HP 5MP Camera is present
    let check = std::process::Command::new("ffmpeg")
        .args(["-list_devices", "true", "-f", "dshow", "-i", "dummy"])
        .output();
    let Ok(output) = check else {
        println!("FFmpeg not found; skipping webcam test");
        return;
    };
    let combined_err = String::from_utf8_lossy(&output.stderr);
    if !combined_err.contains("HP 5MP Camera") {
        println!("HP 5MP Camera not found; skipping live webcam test");
        return;
    }

    // 2. Start VideoSource listening on local UDP port
    let bus = StreamBus::default();
    let tx = bus.get_or_create("webcam_video");
    let mut rx = bus.subscribe("webcam_video");

    let config = VideoSourceConfig {
        url: "udp://127.0.0.1:18555".to_string(),
        interval: 1000,
        codec: "mjpeg".to_string(),
        ..Default::default()
    };

    let (cancel_tx, cancel_rx) = watch::channel(false);
    let source = VideoSource::new(config, tx);
    let handle = source.spawn(cancel_rx);

    // 3. Spawn background FFmpeg to broadcast webcam to udp://127.0.0.1:18555 for 5 seconds
    let mut bg_ffmpeg = tokio::process::Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-t",
            "5",
            "-f",
            "dshow",
            "-i",
            "video=HP 5MP Camera",
            "-f",
            "mjpeg",
            "udp://127.0.0.1:18555",
        ])
        .spawn()
        .expect("failed to spawn ffmpeg webcam streamer");

    // 4. Await first webcam frame
    let frame_rec = tokio::time::timeout(Duration::from_secs(10), rx.recv()).await;

    // Clean up
    let _ = cancel_tx.send(true);
    let _ = bg_ffmpeg.kill().await;
    let _ = handle.await;

    let Ok(Some(rec)) = frame_rec else {
        panic!("Timed out or failed waiting for webcam frame");
    };

    let b64_img = rec
        .data
        .get("image")
        .and_then(|v| v.as_str())
        .expect("missing image field");
    let img_bytes = base64::engine::general_purpose::STANDARD
        .decode(b64_img)
        .expect("invalid base64 image data");

    assert!(
        img_bytes.len() > 1000,
        "Webcam image bytes too short: {}",
        img_bytes.len()
    );
    // JPEG SOI marker
    assert_eq!(&img_bytes[0..2], &[0xFF, 0xD8]);
    // JPEG EOI marker
    assert_eq!(&img_bytes[img_bytes.len() - 2..], &[0xFF, 0xD9]);
    assert_eq!(
        rec.data.get("format").and_then(|v| v.as_str()),
        Some("mjpeg")
    );
}
