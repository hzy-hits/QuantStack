/// DeepSeek API client — OpenAI-compatible chat completions.
///
/// Used ONLY for structured extraction (classification, NER, sentiment).
/// NEVER for reasoning, analysis, or generating insights.
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::warn;

use std::sync::Arc;

const API_URL: &str = "https://api.deepseek.com/v1/chat/completions";

#[derive(Clone)]
pub struct DeepSeekClient {
    client: reqwest::Client,
    api_key: String,
    model: String,
    semaphore: Arc<Semaphore>,
}

#[derive(Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<Message>,
    temperature: f64,
    max_tokens: u32,
    response_format: ResponseFormat,
}

#[derive(Serialize)]
struct ResponseFormat {
    r#type: String,
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: Message,
}

impl DeepSeekClient {
    pub fn new(api_key: &str, model: &str, concurrency: usize) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()?;

        Ok(Self {
            client,
            api_key: api_key.to_string(),
            model: model.to_string(),
            semaphore: Arc::new(Semaphore::new(concurrency)),
        })
    }

    /// Send a chat completion request with concurrency control.
    /// Returns the assistant's response content.
    pub async fn complete(&self, system: &str, user: &str) -> Result<String> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| anyhow!("semaphore error: {}", e))?;

        let req = ChatRequest {
            model: self.model.clone(),
            messages: vec![
                Message {
                    role: "system".to_string(),
                    content: system.to_string(),
                },
                Message {
                    role: "user".to_string(),
                    content: user.to_string(),
                },
            ],
            temperature: 0.0, // deterministic — we want extraction, not creativity
            max_tokens: 512,
            response_format: ResponseFormat {
                r#type: "json_object".to_string(),
            },
        };

        let resp = self
            .client
            .post(API_URL)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&req)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!(status = %status, body = body, "deepseek api error");
            return Err(anyhow!("DeepSeek API error: {} {}", status, body));
        }

        let chat: ChatResponse = resp.json().await?;
        chat.choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| anyhow!("DeepSeek returned empty choices"))
    }
}
