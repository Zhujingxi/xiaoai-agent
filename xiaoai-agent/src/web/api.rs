use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use super::config_store::{ConfigStoreError, EditableConfig, SecretUpdate};
use super::restart::RestartError;
use super::WebState;

#[derive(Serialize)]
struct ApiErrorBody {
    error: String,
    field: Option<String>,
}

struct ApiError {
    status: StatusCode,
    body: ApiErrorBody,
}

impl ApiError {
    fn new(status: StatusCode, error: &str, field: Option<String>) -> Self {
        Self {
            status,
            body: ApiErrorBody {
                error: error.to_string(),
                field,
            },
        }
    }

    fn invalid_request(status: StatusCode) -> Self {
        let error = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "invalid_request"
        };
        Self::new(status, error, None)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

impl From<ConfigStoreError> for ApiError {
    fn from(error: ConfigStoreError) -> Self {
        match error {
            ConfigStoreError::Field { field, .. } => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_config", Some(field))
            }
            ConfigStoreError::Validation { field, .. } => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_config", field)
            }
            ConfigStoreError::Io(_) | ConfigStoreError::Yaml(_) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "configuration_unavailable",
                None,
            ),
        }
    }
}

impl From<RestartError> for ApiError {
    fn from(error: RestartError) -> Self {
        match error {
            RestartError::AlreadyScheduled => {
                Self::new(StatusCode::CONFLICT, "restart_already_scheduled", None)
            }
            #[cfg(not(unix))]
            RestartError::Unsupported => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "restart_failed", None)
            }
            RestartError::CurrentExe(_) | RestartError::Spawn(_) => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "restart_failed", None)
            }
        }
    }
}

#[derive(Deserialize)]
struct LogsQuery {
    limit: Option<usize>,
}

#[derive(Serialize)]
struct LogsResponse {
    entries: Vec<String>,
}

#[derive(Serialize)]
struct RestartResponse {
    restarting: bool,
}

pub fn router(state: WebState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/config", get(get_config).put(put_config))
        .route("/api/status", get(get_status))
        .route("/api/logs", get(get_logs))
        .route("/api/restart", post(restart))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(state)
}

async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("index.html"),
    )
}

async fn get_config(State(state): State<WebState>) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.store.load().await?))
}

async fn put_config(
    State(state): State<WebState>,
    payload: Result<Json<EditableConfig<SecretUpdate>>, JsonRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let Json(update) = payload.map_err(|error| ApiError::invalid_request(error.status()))?;
    let response = state.store.save(update).await?;
    tracing::info!(target: "xiaoai_agent::web_status", "configuration saved");
    Ok(Json(response))
}

async fn get_status(State(state): State<WebState>) -> impl IntoResponse {
    Json(state.status.snapshot())
}

async fn get_logs(
    State(state): State<WebState>,
    query: Result<Query<LogsQuery>, QueryRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let Query(query) = query.map_err(|error| ApiError::invalid_request(error.status()))?;
    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    Ok(Json(LogsResponse {
        entries: state.status.log_entries(limit),
    }))
}

async fn restart(State(state): State<WebState>) -> Result<impl IntoResponse, ApiError> {
    let _guard = state.store.operation_lock().await;
    state.restarter.schedule_restart()?;
    tracing::info!(target: "xiaoai_agent::web_status", "restart scheduled");
    Ok((
        StatusCode::ACCEPTED,
        Json(RestartResponse { restarting: true }),
    ))
}

async fn not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "not_found", None)
}

async fn method_not_allowed() -> ApiError {
    ApiError::new(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", None)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;
    use tracing_subscriber::fmt::writer::MakeWriterExt;

    use super::router;
    use crate::config::AppConfig;
    use crate::web::config_store::ConfigStore;
    use crate::web::restart::{RestartController, RestartError};
    use crate::web::status::{LogBuffer, RuntimeStatus};
    use crate::web::WebState;

    struct MockRestarter {
        calls: Arc<AtomicUsize>,
    }

    impl RestartController for MockRestarter {
        fn schedule_restart(&self) -> Result<(), RestartError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct TestFixture {
        _dir: tempfile::TempDir,
        config_path: std::path::PathBuf,
        store: Arc<ConfigStore>,
        restart_calls: Arc<AtomicUsize>,
        state: WebState,
    }

    async fn test_router(override_yaml: &str) -> (axum::Router, TestFixture) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.yaml");
        std::fs::copy(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("agent.example.yaml"),
            &path,
        )
        .unwrap();

        if !override_yaml.is_empty() {
            let mut base: serde_yaml::Value =
                serde_yaml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            let overrides: serde_yaml::Value = serde_yaml::from_str(override_yaml).unwrap();
            merge_yaml(&mut base, overrides);
            std::fs::write(&path, serde_yaml::to_string(&base).unwrap()).unwrap();
        }

        let restart_required = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let store = Arc::new(ConfigStore::new(path.clone(), restart_required.clone()));
        let logs = LogBuffer::new(300, 2048);
        for index in 0..250 {
            logs.push(format!("log-{index:03}"));
        }
        let status = Arc::new(RuntimeStatus::new(
            Arc::new(AppConfig::load(&path).unwrap()),
            logs,
            restart_required,
        ));
        let restart_calls = Arc::new(AtomicUsize::new(0));
        let state = WebState {
            store: store.clone(),
            status,
            restarter: Arc::new(MockRestarter {
                calls: restart_calls.clone(),
            }),
        };
        let fixture_state = state.clone();

        (
            router(state),
            TestFixture {
                _dir: dir,
                config_path: path,
                store,
                restart_calls,
                state: fixture_state,
            },
        )
    }

    fn merge_yaml(base: &mut serde_yaml::Value, overrides: serde_yaml::Value) {
        match (base, overrides) {
            (serde_yaml::Value::Mapping(base), serde_yaml::Value::Mapping(overrides)) => {
                for (key, value) in overrides {
                    match base.get_mut(&key) {
                        Some(base_value) => merge_yaml(base_value, value),
                        None => {
                            base.insert(key, value);
                        }
                    }
                }
            }
            (base, value) => *base = value,
        }
    }

    async fn response_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn index_serves_the_configuration_page() {
        let (app, _fixture) = test_router("").await;
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "text/html; charset=utf-8"
        );
        let bytes = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        for marker in [
            "XiaoAI Agent 配置",
            "运行状态",
            "语音与模型",
            "工具与可选功能",
            "常规设置",
            "高级与调试信息",
            "保存配置",
            "重启服务",
            "data-provider-section",
            "loadConfig",
            "saveConfig",
            "restartAgent",
            "const RESTART_TIMEOUT_MS = 30000",
            "new AbortController()",
            "deadline - Date.now()",
            "timeoutMs: remaining",
            "pageState.pendingRestart",
            "restartRequired || pageState.pendingRestart",
        ] {
            assert!(body.contains(marker), "missing marker: {marker}");
        }
        for label in [
            "清除 Qwen API 密钥",
            "清除 OpenAI 兼容 ASR API 密钥",
            "清除 OpenAI 实时转写 API 密钥",
            "清除 LLM API 密钥",
            "清除 QWeather 请求 URL",
            "清除 Tavily API 密钥",
            "清除 Home Assistant 访问令牌",
            "清除 Navidrome 密码",
            "清除网易云音乐密码",
            "清除网易云音乐 MD5 密码",
            "清除 AirPlay 密码",
        ] {
            assert!(
                body.contains(&format!(r#"aria-label="{label}""#)),
                "missing accessible secret action: {label}"
            );
        }
        assert_eq!(body.matches("data-clear-secret=").count(), 11);
        assert_eq!(body.matches(r#"aria-label="清除"#).count(), 11);
        for forbidden in ["react", "vue", "bootstrap", "tailwind", "websocket"] {
            assert!(!body.to_ascii_lowercase().contains(forbidden));
        }
    }

    #[tokio::test]
    async fn config_response_is_json_and_redacts_all_known_secrets() {
        let secrets = [
            "voice-secret",
            "asr-secret",
            "realtime-secret",
            "top-secret",
            "weather-secret",
            "search-secret",
            "ha-secret",
            "navidrome-secret",
            "netease-secret",
            "netease-md5-secret",
            "airplay-secret",
        ];
        let (app, _fixture) = test_router(
            r#"
voice:
  qwen:
    api_key: voice-secret
asr:
  open_ai:
    api_key: asr-secret
  openai_realtime:
    api_key: realtime-secret
llm:
  api_key: top-secret
agent:
  weather:
    qweather_url: weather-secret
  web_search:
    api_key: search-secret
mcp:
  home_assistant:
    token: ha-secret
music:
  navidrome:
    password: navidrome-secret
  netease:
    password: netease-secret
    md5_password: netease-md5-secret
airplay:
  password: airplay-secret
"#,
        )
        .await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        let bytes = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        for secret in secrets {
            assert!(!body.contains(secret), "secret leaked: {secret}");
        }
        assert!(body.contains("configured"));
    }

    #[tokio::test]
    async fn restart_returns_accepted_and_calls_controller_once() {
        let (app, fixture) = test_router("").await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/restart")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(fixture.restart_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({"restarting": true})
        );
    }

    #[tokio::test]
    async fn successful_mutations_emit_only_fixed_safe_status_events() {
        let (_app, fixture) = test_router("").await;
        let private_request_value = "private-request-value-must-not-be-logged";
        let mut update = crate::web::config_store::EditableConfig::from_app(
            &AppConfig::load(&fixture.config_path).unwrap(),
        )
        .into_update_keep_secrets();
        update.llm.model = private_request_value.to_string();

        let event_logs = LogBuffer::new(10, 2048);
        let web_log_writer = event_logs
            .clone()
            .with_filter(|metadata| metadata.target() == "xiaoai_agent::web_status");
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(web_log_writer)
            .finish();
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);

        tracing::info!("ordinary log with {private_request_value}");
        assert!(super::put_config(
            axum::extract::State(fixture.state.clone()),
            Ok(axum::Json(update)),
        )
        .await
        .is_ok());
        assert!(super::restart(axum::extract::State(fixture.state.clone()))
            .await
            .is_ok());

        let entries = event_logs.entries(10);
        assert_eq!(entries.len(), 2);
        assert!(entries[0].contains("configuration saved"));
        assert!(entries[1].contains("restart scheduled"));
        let combined = entries.join("\n");
        assert!(!combined.contains(private_request_value));
        assert!(!combined.contains(&fixture.config_path.display().to_string()));
    }

    #[tokio::test]
    async fn restart_waits_for_the_config_operation_lock() {
        let (app, fixture) = test_router("").await;
        let guard = fixture.store.operation_lock().await;
        let mut pending = Box::pin(
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/restart")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            ),
        );

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut pending)
                .await
                .is_err()
        );
        assert_eq!(fixture.restart_calls.load(Ordering::SeqCst), 0);
        drop(guard);

        assert_eq!(pending.await.unwrap().status(), StatusCode::ACCEPTED);
        assert_eq!(fixture.restart_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn status_returns_the_runtime_snapshot() {
        let (app, _fixture) = test_router("").await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
        assert!(body["uptime_s"].is_u64());
        assert_eq!(body["restart_required"], false);
    }

    #[tokio::test]
    async fn logs_limit_is_clamped_to_the_supported_range() {
        let (app, _fixture) = test_router("").await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/logs?limit=500")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["entries"].as_array().unwrap().len(), 200);
        assert_eq!(body["entries"][0], "log-050");
        assert_eq!(body["entries"][199], "log-249");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/logs?limit=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response_json(response).await;
        assert_eq!(body["entries"], serde_json::json!(["log-249"]));
    }

    #[tokio::test]
    async fn invalid_config_json_returns_a_stable_json_400() {
        let (app, _fixture) = test_router("").await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/config")
                    .header("content-type", "application/json")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({"error": "invalid_request", "field": null})
        );
    }

    #[tokio::test]
    async fn internal_config_errors_do_not_disclose_contents_or_paths() {
        let (app, fixture) = test_router("").await;
        let secret = "secret-from-malformed-config";
        std::fs::write(
            &fixture.config_path,
            format!("llm:\n  api_key: {secret}\nmalformed: ["),
        )
        .unwrap();
        let path = fixture.config_path.display().to_string();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = to_bytes(response.into_body(), 8 * 1024).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&body).unwrap(),
            serde_json::json!({"error": "configuration_unavailable", "field": null})
        );
        assert!(!body.contains(secret));
        assert!(!body.contains(&path));
    }

    #[tokio::test]
    async fn config_body_over_64_kib_returns_a_stable_json_413() {
        let (app, _fixture) = test_router("").await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/config")
                    .header("content-type", "application/json")
                    .body(Body::from(vec![b' '; 65 * 1024]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({"error": "payload_too_large", "field": null})
        );
    }

    #[tokio::test]
    async fn unknown_route_returns_a_stable_json_404() {
        let (app, _fixture) = test_router("").await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/not-found")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({"error": "not_found", "field": null})
        );
    }
}
