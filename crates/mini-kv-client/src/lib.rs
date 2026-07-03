use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server error: {0}")]
    Server(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Deserialize)]
struct GetResponse {
    value: Option<String>,
}

#[derive(Deserialize)]
struct ScanResponse {
    items: Vec<KvPair>,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize, Deserialize)]
pub struct KvPair {
    pub key: String,
    pub value: String,
}

#[derive(Serialize)]
struct PutRequest {
    value: String,
    ttl_secs: Option<u64>,
}

#[derive(Clone)]
pub struct MiniKvClient {
    base_url: String,
    client: reqwest::Client,
}

impl MiniKvClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let url = format!("{}/kv/{key}", self.base_url);
        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            let body: GetResponse = resp.json().await?;
            Ok(body.value)
        } else {
            let err: ErrorResponse = resp.json().await?;
            Err(ClientError::Server(err.error))
        }
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<()> {
        self.put_with_ttl(key, value, None).await
    }

    pub async fn put_with_ttl(&self, key: &str, value: &str, ttl: Option<Duration>) -> Result<()> {
        let url = format!("{}/kv/{key}", self.base_url);
        let body = PutRequest {
            value: value.to_string(),
            ttl_secs: ttl.map(|d| d.as_secs()),
        };
        let resp = self.client.post(&url).json(&body).send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            let err: ErrorResponse = resp.json().await?;
            Err(ClientError::Server(err.error))
        }
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let url = format!("{}/kv/{key}", self.base_url);
        let resp = self.client.delete(&url).send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            let err: ErrorResponse = resp.json().await?;
            Err(ClientError::Server(err.error))
        }
    }

    pub async fn scan(&self, start: &str, end: &str) -> Result<Vec<KvPair>> {
        let url = format!("{}/scan?start={start}&end={end}", self.base_url);
        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            let body: ScanResponse = resp.json().await?;
            Ok(body.items)
        } else {
            let err: ErrorResponse = resp.json().await?;
            Err(ClientError::Server(err.error))
        }
    }

    pub async fn scan_prefix(&self, prefix: &str) -> Result<Vec<KvPair>> {
        let url = format!("{}/scan-prefix?prefix={prefix}", self.base_url);
        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            let body: ScanResponse = resp.json().await?;
            Ok(body.items)
        } else {
            let err: ErrorResponse = resp.json().await?;
            Err(ClientError::Server(err.error))
        }
    }

    pub async fn flush(&self) -> Result<()> {
        let url = format!("{}/flush", self.base_url);
        let resp = self.client.post(&url).send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            let err: ErrorResponse = resp.json().await?;
            Err(ClientError::Server(err.error))
        }
    }
}
