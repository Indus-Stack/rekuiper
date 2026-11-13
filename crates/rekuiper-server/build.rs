use std::env;
use std::fs;
use std::path::Path;

const ETC_FILES: &[&str] = &[
    "mqtt_source.json",
    "mqtt_source.yaml",
    "connections/connection.yaml",
    "sources/edgex.json",
    "sources/edgex.yaml",
    "sources/file.json",
    "sources/file.yaml",
    "sources/httppull.json",
    "sources/httppull.yaml",
    "sources/httppush.json",
    "sources/httppush.yaml",
    "sources/memory.json",
    "sources/memory.yaml",
    "sources/neuron.json",
    "sources/neuron.yaml",
    "sources/redis.json",
    "sources/redis.yaml",
    "sources/redisSub.json",
    "sources/redisSub.yaml",
    "sources/simulator.json",
    "sources/simulator.yaml",
    "sources/websocket.json",
    "sources/websocket.yaml",
    "sinks/edgex.json",
    "sinks/file.json",
    "sinks/log.json",
    "sinks/memory.json",
    "sinks/mqtt.json",
    "sinks/neuron.json",
    "sinks/nop.json",
    "sinks/redis.json",
    "sinks/redisPub.json",
    "sinks/rest.json",
    "sinks/websocket.json",
];

fn main() {
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let out_path = Path::new(&out_dir).join("embedded_etc.rs");
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let etc_root = Path::new(&manifest_dir).join("../../etc");

    let mut code = String::from(
        "fn embedded_etc_file(relative: &str) -> Option<&'static str> {\n    match relative {\n",
    );
    if etc_root.is_dir() {
        println!("cargo:rerun-if-changed={}", etc_root.display());
        for rel in ETC_FILES {
            let file_path = etc_root.join(rel);
            if let Ok(content) = fs::read_to_string(&file_path) {
                println!("cargo:rerun-if-changed={}", file_path.display());
                code.push_str(&format!("        {:?} => Some({:?}),\n", rel, content));
            }
        }
    }
    code.push_str("        _ => None,\n    }\n}\n");

    fs::write(&out_path, code).expect("failed to write embedded_etc.rs");
}
