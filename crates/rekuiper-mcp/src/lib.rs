pub mod prompts;
pub mod protocol;
pub mod resources;
pub mod tools;

use protocol::*;
use serde_json::json;

pub struct McpHandler {
    client: reqwest::Client,
    base_url: String,
}

impl McpHandler {
    pub fn new(base_url: impl Into<String>) -> Self {
        let token = std::env::var("REKUIPER_TOKEN")
            .or_else(|_| std::env::var("KUIPER_TOKEN"))
            .ok();
        Self::with_token(base_url, token)
    }

    pub fn with_token(base_url: impl Into<String>, token: Option<String>) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(tok) = token {
            let trimmed = tok.trim();
            let raw_token = trimmed
                .strip_prefix("Bearer ")
                .or_else(|| trimmed.strip_prefix("bearer "))
                .unwrap_or(trimmed);
            if !raw_token.is_empty() {
                if let Ok(val) = reqwest::header::HeaderValue::from_str(raw_token) {
                    headers.insert(reqwest::header::AUTHORIZATION, val);
                }
            }
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            client,
            base_url: base_url.into(),
        }
    }

    pub async fn handle_request(&self, req: JsonRpcRequest) -> Option<JsonRpcResponse> {
        let id = req.id;
        match req.method.as_str() {
            "initialize" => {
                let result = InitializeResult {
                    protocol_version: PROTOCOL_VERSION.to_string(),
                    capabilities: ServerCapabilities {
                        tools: Some(json!({})),
                        resources: Some(json!({})),
                        prompts: Some(json!({})),
                    },
                    server_info: ServerInfo {
                        name: SERVER_NAME.to_string(),
                        version: SERVER_VERSION.to_string(),
                    },
                };
                Some(JsonRpcResponse::success(id, json!(result)))
            }

            "notifications/initialized" => {
                // Client confirmed initialization. No response needed for notifications.
                None
            }

            "ping" => Some(JsonRpcResponse::success(id, json!({}))),

            "tools/list" => {
                let tool_list = tools::get_tool_definitions();
                Some(JsonRpcResponse::success(id, json!({ "tools": tool_list })))
            }

            "tools/call" => {
                let params = req.params.unwrap_or(json!({}));
                let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

                let res = tools::execute_tool(&self.client, &self.base_url, name, arguments).await;
                Some(JsonRpcResponse::success(id, json!(res)))
            }

            "resources/list" => {
                let res_list = resources::get_resource_definitions();
                Some(JsonRpcResponse::success(
                    id,
                    json!({ "resources": res_list }),
                ))
            }

            "resources/read" => {
                let params = req.params.unwrap_or(json!({}));
                let uri = params.get("uri").and_then(|v| v.as_str()).unwrap_or("");

                match resources::read_resource(&self.client, &self.base_url, uri).await {
                    Ok(content) => Some(JsonRpcResponse::success(
                        id,
                        json!({ "contents": [content] }),
                    )),
                    Err(err) => Some(JsonRpcResponse::error(id, -32002, err)),
                }
            }

            "prompts/list" => {
                let prompt_list = prompts::get_prompt_definitions();
                Some(JsonRpcResponse::success(
                    id,
                    json!({ "prompts": prompt_list }),
                ))
            }

            "prompts/get" => {
                let params = req.params.unwrap_or(json!({}));
                let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let arguments = params.get("arguments").cloned();

                match prompts::get_prompt_messages(name, arguments) {
                    Ok(messages) => Some(JsonRpcResponse::success(
                        id,
                        json!({ "messages": messages }),
                    )),
                    Err(err) => Some(JsonRpcResponse::error(id, -32002, err)),
                }
            }

            unknown => Some(JsonRpcResponse::error(
                id,
                -32601,
                format!("Method not found: '{}'", unknown),
            )),
        }
    }
}
