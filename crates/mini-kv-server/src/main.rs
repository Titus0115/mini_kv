use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::Json;
use axum::Router;
use serde::{Deserialize, Serialize};
use mini_kv_core::{Storage, WalEngine};

#[derive(Clone)]
struct AppState {
    engine: Arc<WalEngine>,
}

#[derive(Deserialize)]
struct PutRequest {
    value: String,
    ttl_secs: Option<u64>,
}

#[derive(Deserialize)]
struct ScanQuery {
    start: String,
    end: String,
}

#[derive(Deserialize)]
struct ScanPrefixQuery {
    prefix: String,
}

#[derive(Serialize)]
struct GetResponse {
    value: Option<String>,
}

#[derive(Serialize)]
struct ScanResponse {
    items: Vec<KvPair>,
}

#[derive(Serialize)]
struct KvPair {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

enum AppError {
    Kv(mini_kv_core::KvError),
}

impl From<mini_kv_core::KvError> for AppError {
    fn from(e: mini_kv_core::KvError) -> Self {
        AppError::Kv(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            AppError::Kv(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        (status, Json(ErrorResponse { error: msg })).into_response()
    }
}

async fn handle_get(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let val = state.engine.get(key.as_bytes())?;
    let value = val.map(|v| String::from_utf8_lossy(&v).to_string());
    Ok(Json(GetResponse { value }))
}

async fn handle_put(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PutRequest>,
) -> Result<impl IntoResponse, AppError> {
    let value_bytes = body.value.into_bytes();
    if let Some(ttl) = body.ttl_secs {
        state.engine.put_with_ttl(key.as_bytes(), &value_bytes, Duration::from_secs(ttl))?;
    } else {
        state.engine.put(key.as_bytes(), &value_bytes)?;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn handle_delete(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    state.engine.delete(key.as_bytes())?;
    Ok(StatusCode::NO_CONTENT)
}

async fn handle_scan(
    State(state): State<AppState>,
    Query(query): Query<ScanQuery>,
) -> Result<impl IntoResponse, AppError> {
    let items = state.engine.scan(query.start.as_bytes(), query.end.as_bytes())?;
    let items = items
        .into_iter()
        .map(|(k, v)| KvPair {
            key: String::from_utf8_lossy(&k).to_string(),
            value: String::from_utf8_lossy(&v).to_string(),
        })
        .collect();
    Ok(Json(ScanResponse { items }))
}

async fn handle_scan_prefix(
    State(state): State<AppState>,
    Query(query): Query<ScanPrefixQuery>,
) -> Result<impl IntoResponse, AppError> {
    let items = state.engine.scan_prefix(query.prefix.as_bytes())?;
    let items = items
        .into_iter()
        .map(|(k, v)| KvPair {
            key: String::from_utf8_lossy(&k).to_string(),
            value: String::from_utf8_lossy(&v).to_string(),
        })
        .collect();
    Ok(Json(ScanResponse { items }))
}

async fn handle_flush(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    state.engine.flush()?;
    Ok(StatusCode::NO_CONTENT)
}

#[tokio::main]
async fn main() {
    let data_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./data".to_string());
    let addr_str = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:3000".to_string());

    let engine = WalEngine::open(&data_dir).expect("failed to open storage engine");
    let state = AppState {
        engine: Arc::new(engine),
    };

    let app = Router::new()
        .route("/kv/{key}", get(handle_get))
        .route("/kv/{key}", post(handle_put))
        .route("/kv/{key}", delete(handle_delete))
        .route("/scan", get(handle_scan))
        .route("/scan-prefix", get(handle_scan_prefix))
        .route("/flush", post(handle_flush))
        .with_state(state);

    let addr: SocketAddr = addr_str.parse().expect("invalid address");
    tracing::info!("mini-kv server listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
