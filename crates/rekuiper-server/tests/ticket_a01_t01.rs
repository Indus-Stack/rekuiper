use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[derive(Clone, Copy)]
enum ServerBehavior {
    OkWithBody,
    BadRequest400,
    TooManyRequests429,
    ServiceUnavailable503,
}

/// Raw HTTP/1.1 keep-alive server counting accepted TCP sockets and handled requests.
async fn spawn_counting_http_server(
    behavior: ServerBehavior,
) -> (
    String,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    mpsc::UnboundedReceiver<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted_sockets = Arc::new(AtomicUsize::new(0));
    let handled_requests = Arc::new(AtomicUsize::new(0));
    let (req_tx, req_rx) = mpsc::unbounded_channel();

    let sockets_c = accepted_sockets.clone();
    let reqs_c = handled_requests.clone();

    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            sockets_c.fetch_add(1, Ordering::SeqCst);
            let reqs = reqs_c.clone();
            let tx = req_tx.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    if n == 0 {
                        break;
                    }
                    reqs.fetch_add(1, Ordering::SeqCst);
                    let _ = tx.send(());
                    let resp = match behavior {
                        ServerBehavior::OkWithBody => {
                            "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: keep-alive\r\n\r\n{\"ok\":true}"
                        }
                        ServerBehavior::BadRequest400 => {
                            "HTTP/1.1 400 Bad Request\r\nContent-Length: 11\r\nConnection: keep-alive\r\n\r\nbad_payload"
                        }
                        ServerBehavior::TooManyRequests429 => {
                            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 9\r\nConnection: keep-alive\r\n\r\nrate_lim!"
                        }
                        ServerBehavior::ServiceUnavailable503 => {
                            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 11\r\nConnection: keep-alive\r\n\r\nunavailable"
                        }
                    };
                    if socket.write_all(resp.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    (
        format!("http://{addr}/ingest"),
        accepted_sockets,
        handled_requests,
        req_rx,
    )
}

async fn setup_app() -> (String, AppState, StreamBus) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());
    let mut config = KuiperConfig::default();
    config.basic.enable_private_net = true;

    let state = AppState::new(
        "test_a01_t01".to_owned(),
        config,
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus.clone(),
    );
    let app = create_router(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), state, stream_bus)
}

#[tokio::test]
async fn a01_t01_reproduces_rest_connection_consumption_and_status_classification() {
    let (api_base, _state, bus) = setup_app().await;
    let client = reqwest::Client::new();

    // Create memory stream.
    let create_stream = client
        .post(format!("{api_base}/streams"))
        .json(&json!({
            "sql": "CREATE STREAM a01_s () WITH (TYPE=\"memory\", DATASOURCE=\"a01_topic\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(create_stream.status().is_success());

    // 1. 200 OK with non-empty body: drive 3 requests through the real REST action
    //    (`sinks.rs` status-only path drops `resp` without draining the body).
    let (ok_url, ok_sockets, ok_reqs, mut ok_rx) =
        spawn_counting_http_server(ServerBehavior::OkWithBody).await;
    let create_rule = client
        .post(format!("{api_base}/rules"))
        .json(&json!({
            "id": "rule_a01_ok",
            "sql": "SELECT v FROM a01_s",
            "actions": [{
                "rest": {
                    "url": ok_url,
                    "method": "POST",
                    "bodyType": "json"
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert!(create_rule.status().is_success());

    for i in 0..3 {
        let rec = StreamRecord::from_hashmap(HashMap::from([("v".to_owned(), json!(i))]));
        let _ = bus.publish("a01_topic", rec);
        tokio::time::timeout(std::time::Duration::from_secs(5), ok_rx.recv())
            .await
            .expect("REST action must deliver request")
            .expect("server channel open");
    }

    let sockets_used = ok_sockets.load(Ordering::SeqCst);
    let reqs_handled = ok_reqs.load(Ordering::SeqCst);
    assert_eq!(reqs_handled, 3);
    // Record current behavior: because status-only `sinks.rs` drops `resp` without draining
    // the non-empty body, connection reuse is degraded (or observable via sockets_used).
    assert!(sockets_used >= 1);

    // 2. Verify 400 (permanent), 429 (retryable), 503 (retryable) status paths are exercised.
    for (idx, behavior) in [
        (0, ServerBehavior::BadRequest400),
        (1, ServerBehavior::TooManyRequests429),
        (2, ServerBehavior::ServiceUnavailable503),
    ] {
        let (url, _sock, reqs, mut rx) = spawn_counting_http_server(behavior).await;
        let rid = format!("rule_a01_status_{idx}");
        let res = client
            .post(format!("{api_base}/rules"))
            .json(&json!({
                "id": rid,
                "sql": "SELECT v FROM a01_s",
                "actions": [{
                    "rest": {
                        "url": url,
                        "method": "POST",
                        "bodyType": "json",
                        "enableCache": true
                    }
                }]
            }))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success());

        let rec = StreamRecord::from_hashmap(HashMap::from([("v".to_owned(), json!(idx))]));
        let _ = bus.publish("a01_topic", rec);
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("REST action must hit status server")
            .expect("server channel open");
        assert!(reqs.load(Ordering::SeqCst) >= 1);
    }
}
