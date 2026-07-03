use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Json;
use axum::Router;
use base64::Engine;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{fmt, EnvFilter};
use uuid::Uuid;

use mini_kv_core::{Janitor, Storage, WalEngine};

/// Command-line arguments for mini-kv-server
#[derive(Parser, Debug, Clone)]
#[command(name = "mini-kv-server", about = "Mini-KV HTTP server")]
pub struct Args {
    /// Data directory
    #[arg(long, default_value = "./data", env = "MINI_KV_DATA_DIR")]
    pub data_dir: String,

    /// Listen host
    #[arg(long, default_value = "127.0.0.1", env = "MINI_KV_HOST")]
    pub host: String,

    /// Listen port
    #[arg(long, default_value_t = 3456, env = "MINI_KV_PORT")]
    pub port: u16,

    /// Log level
    #[arg(long, default_value = "error", env = "MINI_KV_LOG")]
    pub log_level: String,

    /// WAL size threshold in bytes
    #[arg(long, default_value_t = 67108864)]
    pub wal_threshold_bytes: u64,

    /// Janitor cleanup interval in seconds
    #[arg(long, default_value_t = 30)]
    pub janitor_interval_secs: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<WalEngine>,
    pub metrics: Arc<Metrics>,
    pub started_at: Instant,
}

#[derive(Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub status_2xx: AtomicU64,
    pub status_4xx: AtomicU64,
    pub status_5xx: AtomicU64,
}

#[derive(Deserialize)]
struct PutRequest {
    value: String,
    #[serde(default)]
    value_base64: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    value_base64: Option<String>,
}

#[derive(Serialize)]
struct ScanResponse {
    items: Vec<KvPair>,
}

#[derive(Serialize)]
struct KvPair {
    key: String,
    value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_base64: Option<String>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    key_count: usize,
    ttl_key_count: usize,
    storage_bytes: u64,
    expired_cleanup_count: u64,
    uptime_secs: u64,
}

pub enum AppError {
    Kv(mini_kv_core::KvError),
    BadRequest(String),
}

impl From<mini_kv_core::KvError> for AppError {
    fn from(e: mini_kv_core::KvError) -> Self {
        AppError::Kv(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            AppError::Kv(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
        };
        (status, Json(ErrorResponse { error: msg })).into_response()
    }
}

fn is_binary_accept(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("application/octet-stream"))
        .unwrap_or(false)
}

fn is_binary_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("application/octet-stream"))
        .unwrap_or(false)
}

/// X-Request-Id middleware
async fn request_id_middleware(mut req: Request<Body>, next: Next) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v: &axum::http::HeaderValue| v.to_str().ok())
        .map(|s: &str| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    req.extensions_mut().insert(request_id.clone());

    let mut response = next.run(req).await;

    response
        .headers_mut()
        .insert("x-request-id", request_id.parse().unwrap());

    response
}

// Handlers

async fn handle_get(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let val = state.engine.get(key.as_bytes())?;
    match val {
        Some(data) if is_binary_accept(&headers) => Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            Bytes::from(data),
        )
            .into_response()),
        Some(data) => {
            let is_utf8 = std::str::from_utf8(&data).is_ok();
            let resp = GetResponse {
                value: if is_utf8 {
                    Some(String::from_utf8_lossy(&data).to_string())
                } else {
                    None
                },
                value_base64: if !is_utf8 {
                    Some(base64::engine::general_purpose::STANDARD.encode(&data))
                } else {
                    None
                },
            };
            Ok(Json(resp).into_response())
        }
        None => Ok(Json(GetResponse {
            value: None,
            value_base64: None,
        })
        .into_response()),
    }
}

async fn handle_put(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, AppError> {
    let (value_bytes, ttl_secs) = if is_binary_content_type(&headers) {
        (body.to_vec(), None)
    } else {
        let req: PutRequest =
            serde_json::from_slice(&body).map_err(|e| AppError::BadRequest(e.to_string()))?;
        let bytes = if let Some(b64) = req.value_base64 {
            base64::engine::general_purpose::STANDARD
                .decode(&b64)
                .map_err(|e| AppError::BadRequest(format!("invalid base64: {e}")))?
        } else {
            req.value.into_bytes()
        };
        (bytes, req.ttl_secs)
    };

    if let Some(ttl) = ttl_secs {
        state
            .engine
            .put_with_ttl(key.as_bytes(), &value_bytes, Duration::from_secs(ttl))?;
    } else {
        state.engine.put(key.as_bytes(), &value_bytes)?;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn handle_delete(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, AppError> {
    state.engine.delete(key.as_bytes())?;
    Ok(StatusCode::NO_CONTENT)
}

async fn handle_scan(
    State(state): State<AppState>,
    Query(query): Query<ScanQuery>,
) -> Result<Json<ScanResponse>, AppError> {
    let items = state
        .engine
        .scan(query.start.as_bytes(), query.end.as_bytes())?;
    let items = items
        .into_iter()
        .map(|(k, v)| {
            let is_utf8 = std::str::from_utf8(&v).is_ok();
            KvPair {
                key: String::from_utf8_lossy(&k).to_string(),
                value: if is_utf8 {
                    String::from_utf8_lossy(&v).to_string()
                } else {
                    String::new()
                },
                value_base64: if !is_utf8 {
                    Some(base64::engine::general_purpose::STANDARD.encode(&v))
                } else {
                    None
                },
            }
        })
        .collect();
    Ok(Json(ScanResponse { items }))
}

async fn handle_scan_prefix(
    State(state): State<AppState>,
    Query(query): Query<ScanPrefixQuery>,
) -> Result<Json<ScanResponse>, AppError> {
    let items = state.engine.scan_prefix(query.prefix.as_bytes())?;
    let items = items
        .into_iter()
        .map(|(k, v)| {
            let is_utf8 = std::str::from_utf8(&v).is_ok();
            KvPair {
                key: String::from_utf8_lossy(&k).to_string(),
                value: if is_utf8 {
                    String::from_utf8_lossy(&v).to_string()
                } else {
                    String::new()
                },
                value_base64: if !is_utf8 {
                    Some(base64::engine::general_purpose::STANDARD.encode(&v))
                } else {
                    None
                },
            }
        })
        .collect();
    Ok(Json(ScanResponse { items }))
}

async fn handle_flush(State(state): State<AppState>) -> Result<StatusCode, AppError> {
    state.engine.flush()?;
    Ok(StatusCode::NO_CONTENT)
}

async fn handle_health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        key_count: state.engine.key_count(),
        ttl_key_count: state.engine.ttl_key_count(),
        storage_bytes: state.engine.storage_bytes(),
        expired_cleanup_count: state.engine.expired_cleanup_count(),
        uptime_secs: state.started_at.elapsed().as_secs(),
    })
}

async fn handle_metrics(State(state): State<AppState>) -> String {
    let m = &state.metrics;
    let kc = state.engine.key_count();
    let tc = state.engine.ttl_key_count();
    let sb = state.engine.storage_bytes();
    let ec = state.engine.expired_cleanup_count();
    let up = state.started_at.elapsed().as_secs();
    let _rt = m.requests_total.load(Ordering::Relaxed);
    let s2 = m.status_2xx.load(Ordering::Relaxed);
    let s4 = m.status_4xx.load(Ordering::Relaxed);
    let s5 = m.status_5xx.load(Ordering::Relaxed);

    format!(
        r#"# HELP mini_kv_keys_total Current number of keys
# TYPE mini_kv_keys_total gauge
mini_kv_keys_total {kc}
# HELP mini_kv_ttl_keys_total Keys with TTL
# TYPE mini_kv_ttl_keys_total gauge
mini_kv_ttl_keys_total {tc}
# HELP mini_kv_storage_bytes Storage size in bytes
# TYPE mini_kv_storage_bytes gauge
mini_kv_storage_bytes {sb}
# HELP mini_kv_expired_cleanups_total Total expired key cleanups
# TYPE mini_kv_expired_cleanups_total counter
mini_kv_expired_cleanups_total {ec}
# HELP mini_kv_http_requests_total Total HTTP requests
# TYPE mini_kv_http_requests_total counter
mini_kv_http_requests_total{{status="2xx"}} {s2}
mini_kv_http_requests_total{{status="4xx"}} {s4}
mini_kv_http_requests_total{{status="5xx"}} {s5}
# HELP mini_kv_uptime_seconds Server uptime in seconds
# TYPE mini_kv_uptime_seconds gauge
mini_kv_uptime_seconds {up}
"#
    )
}

/// Run the server with given arguments
pub async fn run_server(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing with JSON format
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    fmt()
        .json()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    tracing::info!(
        host = %args.host,
        port = args.port,
        data_dir = %args.data_dir,
        wal_threshold_bytes = args.wal_threshold_bytes,
        janitor_interval_secs = args.janitor_interval_secs,
        "server starting"
    );

    let engine = Arc::new(WalEngine::open_with_threshold(
        &args.data_dir,
        args.wal_threshold_bytes,
    )?);
    let janitor = Janitor::start(
        engine.clone(),
        Duration::from_secs(args.janitor_interval_secs),
    );

    let state = AppState {
        engine,
        metrics: Arc::new(Metrics::default()),
        started_at: Instant::now(),
    };

    let app = Router::new()
        .route("/kv/{key}", get(handle_get))
        .route("/kv/{key}", post(handle_put))
        .route("/kv/{key}", delete(handle_delete))
        .route("/kv", get(handle_scan))
        .route("/kv/prefix", get(handle_scan_prefix))
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics))
        .route("/flush", post(handle_flush))
        .with_state(state)
        .layer(middleware::from_fn(request_id_middleware));

    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    tracing::info!("mini-kv server listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Graceful shutdown signal
    let shutdown_signal = async {
        let ctrl_c = async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => { tracing::info!("received Ctrl+C, shutting down"); }
            _ = terminate => { tracing::info!("received SIGTERM, shutting down"); }
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await?;

    // Cleanup
    janitor.shutdown()?;
    tracing::info!("server stopped");

    Ok(())
}
