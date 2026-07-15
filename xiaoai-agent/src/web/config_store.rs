use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value};
use tokio::sync::{Mutex, MutexGuard};

const MAX_SECRET_CHARS: usize = 4096;
const VALIDATION_PATHS: &[&str] = &[
    "voice.qwen.api_key",
    "voice.qwen.url",
    "voice.qwen.input_sample_rate",
    "voice.qwen.output_sample_rate",
    "voice.qwen.connect_timeout_s",
    "voice.qwen.event_timeout_s",
    "voice.qwen.tool_timeout_s",
    "mcp.home_assistant.timeout_s",
];
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SecretStatus {
    pub configured: bool,
    pub masked: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretMode {
    Keep,
    Replace,
    Clear,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretUpdate {
    pub mode: SecretMode,
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigStoreError {
    #[error("invalid configuration field {field}: {message}")]
    Field { field: String, message: String },
    #[error("configuration validation failed: {message}")]
    Validation {
        field: Option<String>,
        message: String,
    },
    #[error("configuration I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration YAML failed: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

impl ConfigStoreError {
    pub fn field(&self) -> Option<&str> {
        match self {
            Self::Field { field, .. } => Some(field),
            Self::Validation { field, .. } => field.as_deref(),
            _ => None,
        }
    }
}

pub fn redact_secret(value: &str) -> SecretStatus {
    let value = value.trim();
    if value.is_empty() || value == "EMPTY" {
        return SecretStatus::default();
    }
    let chars = value.chars().collect::<Vec<_>>();
    let masked = if chars.len() <= 4 {
        "••••".to_string()
    } else {
        format!(
            "••••{}",
            chars[chars.len() - 4..].iter().collect::<String>()
        )
    };
    SecretStatus {
        configured: true,
        masked: Some(masked),
    }
}

fn yaml_key(key: &str) -> Value {
    Value::String(key.to_string())
}

fn set_yaml<T: Serialize>(
    root: &mut Value,
    path: &[&str],
    value: T,
) -> Result<(), ConfigStoreError> {
    let value = serde_yaml::to_value(value)?;
    let (last, parents) = path.split_last().expect("configuration path is non-empty");
    let mut current = root;
    for key in parents {
        if !matches!(current, Value::Mapping(_)) {
            *current = Value::Mapping(Mapping::new());
        }
        let mapping = current.as_mapping_mut().expect("mapping created above");
        current = mapping
            .entry(yaml_key(key))
            .or_insert_with(|| Value::Mapping(Mapping::new()));
    }
    current
        .as_mapping_mut()
        .ok_or_else(|| ConfigStoreError::Field {
            field: path.join("."),
            message: "parent is not a YAML mapping".to_string(),
        })?
        .insert(yaml_key(last), value);
    Ok(())
}

fn apply_secret(
    root: &mut Value,
    path: &[&str],
    update: &SecretUpdate,
) -> Result<(), ConfigStoreError> {
    match update.mode {
        SecretMode::Keep => Ok(()),
        SecretMode::Clear => set_yaml(root, path, ""),
        SecretMode::Replace => {
            let value = update.value.as_deref().unwrap_or("").trim();
            if value.is_empty() || value.chars().count() > MAX_SECRET_CHARS {
                return Err(ConfigStoreError::Field {
                    field: path.join("."),
                    message: "replacement secret must contain 1 to 4096 characters".to_string(),
                });
            }
            set_yaml(root, path, value)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EditableConfig<S> {
    pub runtime: RuntimeFields,
    pub voice: VoiceFields<S>,
    pub asr: AsrFields<S>,
    pub llm: LlmFields<S>,
    pub agent: AgentFields<S>,
    pub home_assistant: HomeAssistantFields<S>,
    pub music: MusicFields<S>,
    pub airplay: AirPlayFields<S>,
    pub led_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFields {
    pub acknowledge_text: Vec<String>,
    pub session_idle_timeout_s: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceFields<S> {
    pub runtime: String,
    pub qwen: QwenFields<S>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QwenFields<S> {
    pub url: String,
    pub api_key: S,
    pub model: String,
    pub voice: String,
    pub connect_timeout_s: f64,
    pub event_timeout_s: f64,
    pub tool_timeout_s: f64,
    pub max_tool_calls: usize,
    pub max_tool_iterations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsrFields<S> {
    pub provider: String,
    pub open_ai: OpenAiAsrFields<S>,
    pub openai_realtime: RealtimeAsrFields<S>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenAiAsrFields<S> {
    pub base_url: String,
    pub api_key: S,
    pub model: String,
    pub language: String,
    pub prompt: String,
    pub timeout_s: f64,
    pub retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealtimeAsrFields<S> {
    pub base_url: String,
    pub api_key: S,
    pub model: String,
    pub target_sample_rate: u32,
    pub chunk_ms: u32,
    pub timeout_s: f64,
    pub retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmFields<S> {
    pub base_url: String,
    pub api_key: S,
    pub model: String,
    pub timeout_s: f64,
    pub max_tokens: u64,
    pub retries: u32,
    pub temperature: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentFields<S> {
    pub timezone: String,
    pub weather: WeatherFields<S>,
    pub web_search: WebSearchFields<S>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeatherFields<S> {
    pub qweather_url: S,
    pub default_location: String,
    pub ip_lookup_url: String,
    pub timeout_s: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSearchFields<S> {
    pub enabled: bool,
    pub tavily_url: String,
    pub api_key: S,
    pub timeout_s: f64,
    pub max_results: usize,
    pub search_depth: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HomeAssistantFields<S> {
    pub enabled: bool,
    pub url: String,
    pub token: S,
    pub timeout_s: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MusicFields<S> {
    pub enabled: bool,
    pub provider: String,
    pub interruption_mode: String,
    pub navidrome: NavidromeFields<S>,
    pub netease: NeteaseFields<S>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NavidromeFields<S> {
    pub base_url: String,
    pub username: String,
    pub password: S,
    pub api_version: String,
    pub timeout_s: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeteaseFields<S> {
    pub api_base_url: String,
    pub login_mode: String,
    pub account: String,
    pub phone: String,
    pub password: S,
    pub md5_password: S,
    pub default_level: String,
    pub timeout_s: f64,
    pub login_on_start: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AirPlayFields<S> {
    pub enabled: bool,
    pub name: String,
    pub port: u16,
    pub password: S,
    pub interruption_mode: String,
    pub duck_gain: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigResponse {
    pub editable: EditableConfig<SecretStatus>,
    pub advanced: serde_json::Value,
    pub restart_required: bool,
}

impl EditableConfig<SecretStatus> {
    pub fn from_app(app: &crate::config::AppConfig) -> Self {
        use crate::config::{AsrProvider, VoiceRuntime};

        Self {
            runtime: RuntimeFields {
                acknowledge_text: app.runtime.acknowledge_text.clone(),
                session_idle_timeout_s: app.runtime.session_idle_timeout_s,
            },
            voice: VoiceFields {
                runtime: match app.voice.runtime {
                    VoiceRuntime::Legacy => "legacy",
                    VoiceRuntime::NativeQwen => "native_qwen",
                }
                .to_string(),
                qwen: QwenFields {
                    url: app.voice.qwen.url.clone(),
                    api_key: redact_secret(&app.voice.qwen.api_key),
                    model: app.voice.qwen.model.clone(),
                    voice: app.voice.qwen.voice.clone(),
                    connect_timeout_s: app.voice.qwen.connect_timeout_s,
                    event_timeout_s: app.voice.qwen.event_timeout_s,
                    tool_timeout_s: app.voice.qwen.tool_timeout_s,
                    max_tool_calls: app.voice.qwen.max_tool_calls,
                    max_tool_iterations: app.voice.qwen.max_tool_iterations,
                },
            },
            asr: AsrFields {
                provider: match app.asr.provider {
                    AsrProvider::OpenAi => "open_ai",
                    AsrProvider::OpenAiRealtime => "openai_realtime",
                    AsrProvider::XiaomiAivs => "xiaomi_aivs",
                }
                .to_string(),
                open_ai: OpenAiAsrFields {
                    base_url: app.asr.open_ai.base_url.clone(),
                    api_key: redact_secret(&app.asr.open_ai.api_key),
                    model: app.asr.open_ai.model.clone(),
                    language: app.asr.open_ai.language.clone(),
                    prompt: app.asr.open_ai.prompt.clone(),
                    timeout_s: app.asr.open_ai.timeout_s,
                    retries: app.asr.open_ai.retries,
                },
                openai_realtime: RealtimeAsrFields {
                    base_url: app.asr.openai_realtime.base_url.clone(),
                    api_key: redact_secret(&app.asr.openai_realtime.api_key),
                    model: app.asr.openai_realtime.model.clone(),
                    target_sample_rate: app.asr.openai_realtime.target_sample_rate,
                    chunk_ms: app.asr.openai_realtime.chunk_ms,
                    timeout_s: app.asr.openai_realtime.timeout_s,
                    retries: app.asr.openai_realtime.retries,
                },
            },
            llm: LlmFields {
                base_url: app.llm.base_url.clone(),
                api_key: redact_secret(&app.llm.api_key),
                model: app.llm.model.clone(),
                timeout_s: app.llm.timeout_s,
                max_tokens: app.llm.max_tokens,
                retries: app.llm.retries,
                temperature: app.llm.temperature,
            },
            agent: AgentFields {
                timezone: app.agent.timezone.clone(),
                weather: WeatherFields {
                    qweather_url: redact_secret(&app.agent.weather.qweather_url),
                    default_location: app.agent.weather.default_location.clone(),
                    ip_lookup_url: app.agent.weather.ip_lookup_url.clone(),
                    timeout_s: app.agent.weather.timeout_s,
                },
                web_search: WebSearchFields {
                    enabled: app.agent.web_search.enabled,
                    tavily_url: app.agent.web_search.tavily_url.clone(),
                    api_key: redact_secret(&app.agent.web_search.api_key),
                    timeout_s: app.agent.web_search.timeout_s,
                    max_results: app.agent.web_search.max_results,
                    search_depth: app.agent.web_search.search_depth.clone(),
                },
            },
            home_assistant: HomeAssistantFields {
                enabled: app.mcp.home_assistant.enabled,
                url: app.mcp.home_assistant.url.clone(),
                token: redact_secret(&app.mcp.home_assistant.token),
                timeout_s: app.mcp.home_assistant.timeout_s,
            },
            music: MusicFields {
                enabled: app.music.enabled,
                provider: app.music.provider.clone(),
                interruption_mode: app.music.interruption.mode.clone(),
                navidrome: NavidromeFields {
                    base_url: app.music.navidrome.base_url.clone(),
                    username: app.music.navidrome.username.clone(),
                    password: redact_secret(&app.music.navidrome.password),
                    api_version: app.music.navidrome.api_version.clone(),
                    timeout_s: app.music.navidrome.timeout_s,
                },
                netease: NeteaseFields {
                    api_base_url: app.music.netease.api_base_url.clone(),
                    login_mode: app.music.netease.login_mode.clone(),
                    account: app.music.netease.account.clone(),
                    phone: app.music.netease.phone.clone(),
                    password: redact_secret(&app.music.netease.password),
                    md5_password: redact_secret(&app.music.netease.md5_password),
                    default_level: app.music.netease.default_level.clone(),
                    timeout_s: app.music.netease.timeout_s,
                    login_on_start: app.music.netease.login_on_start,
                },
            },
            airplay: AirPlayFields {
                enabled: app.airplay.enabled,
                name: app.airplay.name.clone(),
                port: app.airplay.port,
                password: redact_secret(&app.airplay.password),
                interruption_mode: app.airplay.interruption.mode.clone(),
                duck_gain: app.airplay.interruption.duck_gain,
            },
            led_enabled: app.device.led_enabled,
        }
    }

    #[cfg(test)]
    pub fn into_update_keep_secrets(self) -> EditableConfig<SecretUpdate> {
        let keep = || SecretUpdate {
            mode: SecretMode::Keep,
            value: None,
        };
        EditableConfig {
            runtime: self.runtime,
            voice: VoiceFields {
                runtime: self.voice.runtime,
                qwen: QwenFields {
                    url: self.voice.qwen.url,
                    api_key: keep(),
                    model: self.voice.qwen.model,
                    voice: self.voice.qwen.voice,
                    connect_timeout_s: self.voice.qwen.connect_timeout_s,
                    event_timeout_s: self.voice.qwen.event_timeout_s,
                    tool_timeout_s: self.voice.qwen.tool_timeout_s,
                    max_tool_calls: self.voice.qwen.max_tool_calls,
                    max_tool_iterations: self.voice.qwen.max_tool_iterations,
                },
            },
            asr: AsrFields {
                provider: self.asr.provider,
                open_ai: OpenAiAsrFields {
                    base_url: self.asr.open_ai.base_url,
                    api_key: keep(),
                    model: self.asr.open_ai.model,
                    language: self.asr.open_ai.language,
                    prompt: self.asr.open_ai.prompt,
                    timeout_s: self.asr.open_ai.timeout_s,
                    retries: self.asr.open_ai.retries,
                },
                openai_realtime: RealtimeAsrFields {
                    base_url: self.asr.openai_realtime.base_url,
                    api_key: keep(),
                    model: self.asr.openai_realtime.model,
                    target_sample_rate: self.asr.openai_realtime.target_sample_rate,
                    chunk_ms: self.asr.openai_realtime.chunk_ms,
                    timeout_s: self.asr.openai_realtime.timeout_s,
                    retries: self.asr.openai_realtime.retries,
                },
            },
            llm: LlmFields {
                base_url: self.llm.base_url,
                api_key: keep(),
                model: self.llm.model,
                timeout_s: self.llm.timeout_s,
                max_tokens: self.llm.max_tokens,
                retries: self.llm.retries,
                temperature: self.llm.temperature,
            },
            agent: AgentFields {
                timezone: self.agent.timezone,
                weather: WeatherFields {
                    qweather_url: keep(),
                    default_location: self.agent.weather.default_location,
                    ip_lookup_url: self.agent.weather.ip_lookup_url,
                    timeout_s: self.agent.weather.timeout_s,
                },
                web_search: WebSearchFields {
                    enabled: self.agent.web_search.enabled,
                    tavily_url: self.agent.web_search.tavily_url,
                    api_key: keep(),
                    timeout_s: self.agent.web_search.timeout_s,
                    max_results: self.agent.web_search.max_results,
                    search_depth: self.agent.web_search.search_depth,
                },
            },
            home_assistant: HomeAssistantFields {
                enabled: self.home_assistant.enabled,
                url: self.home_assistant.url,
                token: keep(),
                timeout_s: self.home_assistant.timeout_s,
            },
            music: MusicFields {
                enabled: self.music.enabled,
                provider: self.music.provider,
                interruption_mode: self.music.interruption_mode,
                navidrome: NavidromeFields {
                    base_url: self.music.navidrome.base_url,
                    username: self.music.navidrome.username,
                    password: keep(),
                    api_version: self.music.navidrome.api_version,
                    timeout_s: self.music.navidrome.timeout_s,
                },
                netease: NeteaseFields {
                    api_base_url: self.music.netease.api_base_url,
                    login_mode: self.music.netease.login_mode,
                    account: self.music.netease.account,
                    phone: self.music.netease.phone,
                    password: keep(),
                    md5_password: keep(),
                    default_level: self.music.netease.default_level,
                    timeout_s: self.music.netease.timeout_s,
                    login_on_start: self.music.netease.login_on_start,
                },
            },
            airplay: AirPlayFields {
                enabled: self.airplay.enabled,
                name: self.airplay.name,
                port: self.airplay.port,
                password: keep(),
                interruption_mode: self.airplay.interruption_mode,
                duck_gain: self.airplay.duck_gain,
            },
            led_enabled: self.led_enabled,
        }
    }
}

impl ConfigResponse {
    pub fn advanced_from_app(app: &crate::config::AppConfig) -> serde_json::Value {
        serde_json::json!({
            "runtime_kws": {
                "kws_vpm_lib": app.runtime.kws_vpm_lib,
                "kws_vpm_config_dir": app.runtime.kws_vpm_config_dir,
                "kws_pcm": app.runtime.kws_pcm,
                "kws_sample_rate": app.runtime.kws_sample_rate,
                "kws_channels": app.runtime.kws_channels,
                "kws_bits_per_sample": app.runtime.kws_bits_per_sample,
                "kws_frame_ms": app.runtime.kws_frame_ms,
                "kws_period_size": app.runtime.kws_period_size,
                "kws_buffer_size": app.runtime.kws_buffer_size,
                "kws_ref_channel_index": app.runtime.kws_ref_channel_index,
                "kws_start_status": app.runtime.kws_start_status,
            },
            "capture": {
                "pcm": app.capture.pcm,
                "sample_rate": app.capture.sample_rate,
                "channels": app.capture.channels,
                "bits_per_sample": app.capture.bits_per_sample,
                "period_size": app.capture.period_size,
                "buffer_size": app.capture.buffer_size,
                "threshold": app.capture.threshold,
                "mic_gain": app.capture.mic_gain,
                "block_ms": app.capture.block_ms,
                "pre_roll_ms": app.capture.pre_roll_ms,
                "silence_ms": app.capture.silence_ms,
                "min_speech_ms": app.capture.min_speech_ms,
                "max_utterance_s": app.capture.max_utterance_s,
                "cooldown_s": app.capture.cooldown_s,
                "print_levels": app.capture.print_levels,
            },
            "device_commands": {
                "tts_command": app.device.tts_command,
                "play_url_command": app.device.play_url_command,
                "stop_audio_command": app.device.stop_audio_command,
                "pause_audio_command": app.device.pause_audio_command,
                "resume_audio_command": app.device.resume_audio_command,
                "duck_audio_command": app.device.duck_audio_command,
                "unduck_audio_command": app.device.unduck_audio_command,
                "abort_command": app.device.abort_command,
            },
            "led_ids": {
                "led_listening": app.device.led_listening,
                "led_user_speaking": app.device.led_user_speaking,
                "led_thinking": app.device.led_thinking,
                "led_speaking": app.device.led_speaking,
            },
            "xiaomi_aivs": {
                "sdk_lib": app.asr.xiaomi_aivs.sdk_lib,
                "token_path": app.asr.xiaomi_aivs.token_path,
                "miio_dir": app.asr.xiaomi_aivs.miio_dir,
                "engine_mode": app.asr.xiaomi_aivs.engine_mode,
                "connect_wait_ms": app.asr.xiaomi_aivs.connect_wait_ms,
                "chunk_ms": app.asr.xiaomi_aivs.chunk_ms,
                "wait_after_finish_ms": app.asr.xiaomi_aivs.wait_after_finish_ms,
                "asr_only": app.asr.xiaomi_aivs.asr_only,
                "allow_cloud_execution": app.asr.xiaomi_aivs.allow_cloud_execution,
            },
            "airplay_output": {
                "hwaddr": app.airplay.hwaddr,
                "backend": app.airplay.output.backend,
                "aplay_path": app.airplay.output.aplay_path,
                "device": app.airplay.output.device,
                "format": app.airplay.output.format,
            },
        })
    }
}

pub struct ConfigStore {
    path: PathBuf,
    operation: Mutex<()>,
    restart_required: Arc<AtomicBool>,
}

impl ConfigStore {
    pub fn new(path: PathBuf, restart_required: Arc<AtomicBool>) -> Self {
        Self {
            path,
            operation: Mutex::new(()),
            restart_required,
        }
    }

    pub fn restart_required_flag(&self) -> Arc<AtomicBool> {
        self.restart_required.clone()
    }

    pub async fn operation_lock(&self) -> MutexGuard<'_, ()> {
        self.operation.lock().await
    }

    pub async fn load(&self) -> Result<ConfigResponse, ConfigStoreError> {
        let _guard = self.operation.lock().await;
        self.load_locked()
    }

    pub async fn save(
        &self,
        update: EditableConfig<SecretUpdate>,
    ) -> Result<ConfigResponse, ConfigStoreError> {
        let _guard = self.operation.lock().await;
        self.save_locked(update)
    }

    fn load_locked(&self) -> Result<ConfigResponse, ConfigStoreError> {
        let app = crate::config::AppConfig::load(&self.path).map_err(map_app_config_error)?;
        Ok(self.response_from_app(&app))
    }

    fn save_locked(
        &self,
        update: EditableConfig<SecretUpdate>,
    ) -> Result<ConfigResponse, ConfigStoreError> {
        validate_update(&update)?;

        let current_text = std::fs::read_to_string(&self.path)?;
        let mut candidate_yaml: Value = serde_yaml::from_str(&current_text)?;
        apply_update_to_yaml(&mut candidate_yaml, &update)?;
        let candidate_text = serde_yaml::to_string(&candidate_yaml)?;

        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let temp_path = parent.join(format!(
            ".agent.yaml.tmp-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));

        let result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut temp_file = options.open(&temp_path)?;
            temp_file.write_all(candidate_text.as_bytes())?;
            temp_file.sync_all()?;
            drop(temp_file);

            let candidate =
                crate::config::AppConfig::load(&temp_path).map_err(map_app_config_error)?;
            let backup = self.path.with_file_name(format!(
                "{}.bak",
                self.path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("agent.yaml")
            ));
            std::fs::copy(&self.path, &backup)?;
            replace_formal_file(&temp_path, &self.path)?;
            Ok(candidate)
        })();

        let candidate = match result {
            Ok(candidate) => candidate,
            Err(error) => {
                let _ = std::fs::remove_file(&temp_path);
                return Err(error);
            }
        };

        self.restart_required.store(true, Ordering::SeqCst);
        Ok(self.response_from_app(&candidate))
    }

    fn response_from_app(&self, app: &crate::config::AppConfig) -> ConfigResponse {
        ConfigResponse {
            editable: EditableConfig::<SecretStatus>::from_app(app),
            advanced: ConfigResponse::advanced_from_app(app),
            restart_required: self.restart_required.load(Ordering::SeqCst),
        }
    }
}

fn map_app_config_error(error: anyhow::Error) -> ConfigStoreError {
    let message = format!("{error:#}");
    let field = VALIDATION_PATHS
        .iter()
        .find(|path| message.contains(**path))
        .map(|path| (*path).to_string());

    let error = match error.downcast::<std::io::Error>() {
        Ok(error) => return ConfigStoreError::Io(error),
        Err(error) => error,
    };
    match error.downcast::<serde_yaml::Error>() {
        Ok(error) => ConfigStoreError::Yaml(error),
        Err(_) => ConfigStoreError::Validation { field, message },
    }
}

fn invalid_field(field: &str, message: impl Into<String>) -> ConfigStoreError {
    ConfigStoreError::Field {
        field: field.to_string(),
        message: message.into(),
    }
}

fn validate_update(update: &EditableConfig<SecretUpdate>) -> Result<(), ConfigStoreError> {
    if !matches!(update.voice.runtime.as_str(), "legacy" | "native_qwen") {
        return Err(invalid_field(
            "voice.runtime",
            "must be legacy or native_qwen",
        ));
    }
    if !matches!(
        update.asr.provider.as_str(),
        "open_ai" | "openai_realtime" | "xiaomi_aivs"
    ) {
        return Err(invalid_field("asr.provider", "unsupported ASR provider"));
    }
    if !matches!(update.music.provider.as_str(), "navidrome" | "netease") {
        return Err(invalid_field(
            "music.provider",
            "must be navidrome or netease",
        ));
    }
    validate_positive_timeout(
        "runtime.session_idle_timeout_s",
        update.runtime.session_idle_timeout_s,
    )?;

    for (field, value) in [
        (
            "voice.qwen.connect_timeout_s",
            update.voice.qwen.connect_timeout_s,
        ),
        (
            "voice.qwen.event_timeout_s",
            update.voice.qwen.event_timeout_s,
        ),
        (
            "voice.qwen.tool_timeout_s",
            update.voice.qwen.tool_timeout_s,
        ),
        ("asr.open_ai.timeout_s", update.asr.open_ai.timeout_s),
        (
            "asr.openai_realtime.timeout_s",
            update.asr.openai_realtime.timeout_s,
        ),
        ("llm.timeout_s", update.llm.timeout_s),
        ("agent.weather.timeout_s", update.agent.weather.timeout_s),
        (
            "agent.web_search.timeout_s",
            update.agent.web_search.timeout_s,
        ),
        (
            "mcp.home_assistant.timeout_s",
            update.home_assistant.timeout_s,
        ),
        (
            "music.navidrome.timeout_s",
            update.music.navidrome.timeout_s,
        ),
        ("music.netease.timeout_s", update.music.netease.timeout_s),
    ] {
        validate_positive_timeout(field, value)?;
    }

    if !(1..=10).contains(&update.agent.web_search.max_results) {
        return Err(invalid_field(
            "agent.web_search.max_results",
            "must be between 1 and 10",
        ));
    }
    if update.airplay.port == 0 {
        return Err(invalid_field("airplay.port", "must be greater than zero"));
    }
    if !update.airplay.duck_gain.is_finite() || !(0.0..=1.0).contains(&update.airplay.duck_gain) {
        return Err(invalid_field(
            "airplay.interruption.duck_gain",
            "must be finite and between 0 and 1",
        ));
    }

    for value in &update.runtime.acknowledge_text {
        validate_string("runtime.acknowledge_text", value)?;
    }
    for (field, value) in [
        ("voice.runtime", update.voice.runtime.as_str()),
        ("voice.qwen.url", update.voice.qwen.url.as_str()),
        ("voice.qwen.model", update.voice.qwen.model.as_str()),
        ("voice.qwen.voice", update.voice.qwen.voice.as_str()),
        ("asr.provider", update.asr.provider.as_str()),
        ("asr.open_ai.base_url", update.asr.open_ai.base_url.as_str()),
        ("asr.open_ai.model", update.asr.open_ai.model.as_str()),
        ("asr.open_ai.language", update.asr.open_ai.language.as_str()),
        ("asr.open_ai.prompt", update.asr.open_ai.prompt.as_str()),
        (
            "asr.openai_realtime.base_url",
            update.asr.openai_realtime.base_url.as_str(),
        ),
        (
            "asr.openai_realtime.model",
            update.asr.openai_realtime.model.as_str(),
        ),
        ("llm.base_url", update.llm.base_url.as_str()),
        ("llm.model", update.llm.model.as_str()),
        ("agent.timezone", update.agent.timezone.as_str()),
        (
            "agent.weather.default_location",
            update.agent.weather.default_location.as_str(),
        ),
        (
            "agent.weather.ip_lookup_url",
            update.agent.weather.ip_lookup_url.as_str(),
        ),
        (
            "agent.web_search.tavily_url",
            update.agent.web_search.tavily_url.as_str(),
        ),
        (
            "agent.web_search.search_depth",
            update.agent.web_search.search_depth.as_str(),
        ),
        ("mcp.home_assistant.url", update.home_assistant.url.as_str()),
        ("music.provider", update.music.provider.as_str()),
        (
            "music.interruption.mode",
            update.music.interruption_mode.as_str(),
        ),
        (
            "music.navidrome.base_url",
            update.music.navidrome.base_url.as_str(),
        ),
        (
            "music.navidrome.username",
            update.music.navidrome.username.as_str(),
        ),
        (
            "music.navidrome.api_version",
            update.music.navidrome.api_version.as_str(),
        ),
        (
            "music.netease.api_base_url",
            update.music.netease.api_base_url.as_str(),
        ),
        (
            "music.netease.login_mode",
            update.music.netease.login_mode.as_str(),
        ),
        (
            "music.netease.account",
            update.music.netease.account.as_str(),
        ),
        ("music.netease.phone", update.music.netease.phone.as_str()),
        (
            "music.netease.default_level",
            update.music.netease.default_level.as_str(),
        ),
        ("airplay.name", update.airplay.name.as_str()),
        (
            "airplay.interruption.mode",
            update.airplay.interruption_mode.as_str(),
        ),
    ] {
        validate_string(field, value)?;
    }
    Ok(())
}

fn validate_positive_timeout(field: &str, value: f64) -> Result<(), ConfigStoreError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(invalid_field(field, "must be finite and greater than zero"));
    }
    Ok(())
}

fn validate_string(field: &str, value: &str) -> Result<(), ConfigStoreError> {
    if value.chars().count() > MAX_SECRET_CHARS {
        return Err(invalid_field(field, "must not exceed 4096 characters"));
    }
    Ok(())
}

#[cfg(unix)]
fn replace_formal_file(temp_path: &Path, formal_path: &Path) -> std::io::Result<()> {
    std::fs::rename(temp_path, formal_path)
}

#[cfg(any(windows, test))]
fn replacement_old_path(formal_path: &Path) -> PathBuf {
    formal_path.with_file_name(format!(
        ".{}-{}-{}.replace-old",
        formal_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("agent.yaml"),
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

#[cfg(any(windows, test))]
fn replace_via_old_path<R, C>(
    temp_path: &Path,
    formal_path: &Path,
    old_path: &Path,
    mut rename: R,
    cleanup_old: C,
) -> std::io::Result<()>
where
    R: FnMut(&Path, &Path) -> std::io::Result<()>,
    C: FnOnce(&Path) -> std::io::Result<()>,
{
    rename(formal_path, old_path)?;
    if let Err(replace_error) = rename(temp_path, formal_path) {
        return match rename(old_path, formal_path) {
            Ok(()) => Err(replace_error),
            Err(restore_error) => Err(std::io::Error::new(
                restore_error.kind(),
                format!(
                    "configuration replacement failed ({replace_error}); restoring the formal file failed ({restore_error})"
                ),
            )),
        };
    }

    // The new formal file is committed. Cleanup must not turn that success into
    // an error; a future attempt uses a different collision-safe old path.
    let _ = cleanup_old(old_path);
    Ok(())
}

#[cfg(windows)]
fn replace_formal_file(temp_path: &Path, formal_path: &Path) -> std::io::Result<()> {
    let old_path = replacement_old_path(formal_path);
    replace_via_old_path(
        temp_path,
        formal_path,
        &old_path,
        |from, to| std::fs::rename(from, to),
        std::fs::remove_file,
    )
}

#[cfg(not(any(unix, windows)))]
fn replace_formal_file(temp_path: &Path, formal_path: &Path) -> std::io::Result<()> {
    std::fs::rename(temp_path, formal_path)
}

pub fn apply_update_to_yaml(
    root: &mut Value,
    update: &EditableConfig<SecretUpdate>,
) -> Result<(), ConfigStoreError> {
    macro_rules! set {
        ($path:expr, $value:expr) => {
            set_yaml(root, $path, $value)?
        };
    }

    set!(
        &["runtime", "acknowledge_text"],
        &update.runtime.acknowledge_text
    );
    set!(
        &["runtime", "session_idle_timeout_s"],
        update.runtime.session_idle_timeout_s
    );
    set!(&["voice", "runtime"], &update.voice.runtime);
    set!(&["voice", "qwen", "url"], &update.voice.qwen.url);
    set!(&["voice", "qwen", "model"], &update.voice.qwen.model);
    set!(&["voice", "qwen", "voice"], &update.voice.qwen.voice);
    set!(
        &["voice", "qwen", "connect_timeout_s"],
        update.voice.qwen.connect_timeout_s
    );
    set!(
        &["voice", "qwen", "event_timeout_s"],
        update.voice.qwen.event_timeout_s
    );
    set!(
        &["voice", "qwen", "tool_timeout_s"],
        update.voice.qwen.tool_timeout_s
    );
    set!(
        &["voice", "qwen", "max_tool_calls"],
        update.voice.qwen.max_tool_calls
    );
    set!(
        &["voice", "qwen", "max_tool_iterations"],
        update.voice.qwen.max_tool_iterations
    );
    apply_secret(
        root,
        &["voice", "qwen", "api_key"],
        &update.voice.qwen.api_key,
    )?;

    set!(&["asr", "provider"], &update.asr.provider);
    set!(
        &["asr", "open_ai", "base_url"],
        &update.asr.open_ai.base_url
    );
    set!(&["asr", "open_ai", "model"], &update.asr.open_ai.model);
    set!(
        &["asr", "open_ai", "language"],
        &update.asr.open_ai.language
    );
    set!(&["asr", "open_ai", "prompt"], &update.asr.open_ai.prompt);
    set!(
        &["asr", "open_ai", "timeout_s"],
        update.asr.open_ai.timeout_s
    );
    set!(&["asr", "open_ai", "retries"], update.asr.open_ai.retries);
    apply_secret(
        root,
        &["asr", "open_ai", "api_key"],
        &update.asr.open_ai.api_key,
    )?;
    set!(
        &["asr", "openai_realtime", "base_url"],
        &update.asr.openai_realtime.base_url
    );
    set!(
        &["asr", "openai_realtime", "model"],
        &update.asr.openai_realtime.model
    );
    set!(
        &["asr", "openai_realtime", "target_sample_rate"],
        update.asr.openai_realtime.target_sample_rate
    );
    set!(
        &["asr", "openai_realtime", "chunk_ms"],
        update.asr.openai_realtime.chunk_ms
    );
    set!(
        &["asr", "openai_realtime", "timeout_s"],
        update.asr.openai_realtime.timeout_s
    );
    set!(
        &["asr", "openai_realtime", "retries"],
        update.asr.openai_realtime.retries
    );
    apply_secret(
        root,
        &["asr", "openai_realtime", "api_key"],
        &update.asr.openai_realtime.api_key,
    )?;

    set!(&["llm", "base_url"], &update.llm.base_url);
    set!(&["llm", "model"], &update.llm.model);
    set!(&["llm", "timeout_s"], update.llm.timeout_s);
    set!(&["llm", "max_tokens"], update.llm.max_tokens);
    set!(&["llm", "retries"], update.llm.retries);
    set!(&["llm", "temperature"], update.llm.temperature);
    apply_secret(root, &["llm", "api_key"], &update.llm.api_key)?;

    set!(&["agent", "timezone"], &update.agent.timezone);
    apply_secret(
        root,
        &["agent", "weather", "qweather_url"],
        &update.agent.weather.qweather_url,
    )?;
    set!(
        &["agent", "weather", "default_location"],
        &update.agent.weather.default_location
    );
    set!(
        &["agent", "weather", "ip_lookup_url"],
        &update.agent.weather.ip_lookup_url
    );
    set!(
        &["agent", "weather", "timeout_s"],
        update.agent.weather.timeout_s
    );
    set!(
        &["agent", "web_search", "enabled"],
        update.agent.web_search.enabled
    );
    set!(
        &["agent", "web_search", "tavily_url"],
        &update.agent.web_search.tavily_url
    );
    set!(
        &["agent", "web_search", "timeout_s"],
        update.agent.web_search.timeout_s
    );
    set!(
        &["agent", "web_search", "max_results"],
        update.agent.web_search.max_results
    );
    set!(
        &["agent", "web_search", "search_depth"],
        &update.agent.web_search.search_depth
    );
    apply_secret(
        root,
        &["agent", "web_search", "api_key"],
        &update.agent.web_search.api_key,
    )?;

    set!(
        &["mcp", "home_assistant", "enabled"],
        update.home_assistant.enabled
    );
    set!(
        &["mcp", "home_assistant", "url"],
        &update.home_assistant.url
    );
    set!(
        &["mcp", "home_assistant", "timeout_s"],
        update.home_assistant.timeout_s
    );
    apply_secret(
        root,
        &["mcp", "home_assistant", "token"],
        &update.home_assistant.token,
    )?;

    set!(&["music", "enabled"], update.music.enabled);
    set!(&["music", "provider"], &update.music.provider);
    set!(
        &["music", "interruption", "mode"],
        &update.music.interruption_mode
    );
    set!(
        &["music", "navidrome", "base_url"],
        &update.music.navidrome.base_url
    );
    set!(
        &["music", "navidrome", "username"],
        &update.music.navidrome.username
    );
    set!(
        &["music", "navidrome", "api_version"],
        &update.music.navidrome.api_version
    );
    set!(
        &["music", "navidrome", "timeout_s"],
        update.music.navidrome.timeout_s
    );
    apply_secret(
        root,
        &["music", "navidrome", "password"],
        &update.music.navidrome.password,
    )?;
    set!(
        &["music", "netease", "api_base_url"],
        &update.music.netease.api_base_url
    );
    set!(
        &["music", "netease", "login_mode"],
        &update.music.netease.login_mode
    );
    set!(
        &["music", "netease", "account"],
        &update.music.netease.account
    );
    set!(&["music", "netease", "phone"], &update.music.netease.phone);
    set!(
        &["music", "netease", "default_level"],
        &update.music.netease.default_level
    );
    set!(
        &["music", "netease", "timeout_s"],
        update.music.netease.timeout_s
    );
    set!(
        &["music", "netease", "login_on_start"],
        update.music.netease.login_on_start
    );
    apply_secret(
        root,
        &["music", "netease", "password"],
        &update.music.netease.password,
    )?;
    apply_secret(
        root,
        &["music", "netease", "md5_password"],
        &update.music.netease.md5_password,
    )?;

    set!(&["airplay", "enabled"], update.airplay.enabled);
    set!(&["airplay", "name"], &update.airplay.name);
    set!(&["airplay", "port"], update.airplay.port);
    set!(
        &["airplay", "interruption", "mode"],
        &update.airplay.interruption_mode
    );
    set!(
        &["airplay", "interruption", "duck_gain"],
        update.airplay.duck_gain
    );
    apply_secret(root, &["airplay", "password"], &update.airplay.password)?;
    set!(&["device", "led_enabled"], update.led_enabled);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("agent.example.yaml")
    }

    fn yaml_string<'a>(root: &'a serde_yaml::Value, path: &[&str]) -> &'a str {
        yaml_value(root, path).as_str().unwrap()
    }

    fn yaml_value<'a>(root: &'a serde_yaml::Value, path: &[&str]) -> &'a serde_yaml::Value {
        path.iter().fold(root, |current, key| {
            current
                .as_mapping()
                .unwrap()
                .get(serde_yaml::Value::String((*key).to_string()))
                .unwrap()
        })
    }

    #[test]
    fn secrets_are_never_returned_verbatim() {
        assert_eq!(
            redact_secret("sk-example-1234"),
            SecretStatus {
                configured: true,
                masked: Some("••••1234".to_string()),
            }
        );
        assert_eq!(redact_secret("EMPTY"), SecretStatus::default());
        assert_eq!(
            redact_secret("abc"),
            SecretStatus {
                configured: true,
                masked: Some("••••".to_string()),
            }
        );
    }

    #[test]
    fn secret_update_requires_explicit_replace_or_clear() {
        let mut root: serde_yaml::Value =
            serde_yaml::from_str("llm:\n  api_key: old-secret\n").unwrap();
        apply_secret(
            &mut root,
            &["llm", "api_key"],
            &SecretUpdate {
                mode: SecretMode::Keep,
                value: None,
            },
        )
        .unwrap();
        assert_eq!(yaml_string(&root, &["llm", "api_key"]), "old-secret");

        apply_secret(
            &mut root,
            &["llm", "api_key"],
            &SecretUpdate {
                mode: SecretMode::Replace,
                value: Some("new-secret".to_string()),
            },
        )
        .unwrap();
        assert_eq!(yaml_string(&root, &["llm", "api_key"]), "new-secret");

        apply_secret(
            &mut root,
            &["llm", "api_key"],
            &SecretUpdate {
                mode: SecretMode::Clear,
                value: None,
            },
        )
        .unwrap();
        assert_eq!(yaml_string(&root, &["llm", "api_key"]), "");
    }

    #[test]
    fn replacement_rejects_missing_or_oversized_secret() {
        let mut root = serde_yaml::Value::Mapping(Default::default());
        let missing = apply_secret(
            &mut root,
            &["llm", "api_key"],
            &SecretUpdate {
                mode: SecretMode::Replace,
                value: None,
            },
        )
        .unwrap_err();
        assert_eq!(missing.field(), Some("llm.api_key"));

        let oversized = apply_secret(
            &mut root,
            &["llm", "api_key"],
            &SecretUpdate {
                mode: SecretMode::Replace,
                value: Some("x".repeat(4097)),
            },
        )
        .unwrap_err();
        assert_eq!(oversized.field(), Some("llm.api_key"));
    }

    #[test]
    fn validation_error_reports_its_optional_field() {
        let with_field = ConfigStoreError::Validation {
            field: Some("voice.runtime".to_string()),
            message: "unsupported runtime".to_string(),
        };
        assert_eq!(with_field.field(), Some("voice.runtime"));

        let without_field = ConfigStoreError::Validation {
            field: None,
            message: "invalid configuration".to_string(),
        };
        assert_eq!(without_field.field(), None);
    }

    #[test]
    fn editable_config_redacts_all_eleven_secret_fields() {
        let mut app = crate::config::AppConfig::default();
        app.voice.qwen.api_key = "voice-secret-0001".to_string();
        app.asr.open_ai.api_key = "asr-secret-0002".to_string();
        app.asr.openai_realtime.api_key = "realtime-secret-0003".to_string();
        app.llm.api_key = "llm-secret-0004".to_string();
        app.agent.weather.qweather_url = "weather-secret-0005".to_string();
        app.agent.web_search.api_key = "search-secret-0006".to_string();
        app.mcp.home_assistant.token = "ha-secret-0007".to_string();
        app.music.navidrome.password = "navidrome-secret-0008".to_string();
        app.music.netease.password = "netease-secret-0009".to_string();
        app.music.netease.md5_password = "md5-secret-0010".to_string();
        app.airplay.password = "airplay-secret-0011".to_string();

        let editable = EditableConfig::<SecretStatus>::from_app(&app);
        let statuses = [
            &editable.voice.qwen.api_key,
            &editable.asr.open_ai.api_key,
            &editable.asr.openai_realtime.api_key,
            &editable.llm.api_key,
            &editable.agent.weather.qweather_url,
            &editable.agent.web_search.api_key,
            &editable.home_assistant.token,
            &editable.music.navidrome.password,
            &editable.music.netease.password,
            &editable.music.netease.md5_password,
            &editable.airplay.password,
        ];
        assert!(statuses.iter().all(|status| status.configured));
        let response = serde_json::to_string(&editable).unwrap();
        assert!(!response.contains("secret-"));
    }

    #[test]
    fn whitelist_update_preserves_hidden_fields() {
        let source = include_str!("../../agent.example.yaml");
        let app: crate::config::AppConfig = serde_yaml::from_str(source).unwrap();
        let mut root: serde_yaml::Value = serde_yaml::from_str(source).unwrap();
        let tts_command = yaml_value(&root, &["device", "tts_command"]).clone();
        let capture_threshold = yaml_value(&root, &["capture", "threshold"]).clone();
        let allow_cloud_execution =
            yaml_value(&root, &["asr", "xiaomi_aivs", "allow_cloud_execution"]).clone();

        let mut update = EditableConfig::<SecretStatus>::from_app(&app).into_update_keep_secrets();
        update.llm.model = "replacement-model".to_string();
        apply_update_to_yaml(&mut root, &update).unwrap();

        assert_eq!(yaml_string(&root, &["llm", "model"]), "replacement-model");
        assert_eq!(yaml_value(&root, &["device", "tts_command"]), &tts_command);
        assert_eq!(
            yaml_value(&root, &["capture", "threshold"]),
            &capture_threshold
        );
        assert_eq!(
            yaml_value(&root, &["asr", "xiaomi_aivs", "allow_cloud_execution"]),
            &allow_cloud_execution
        );
    }

    #[test]
    fn advanced_response_contains_complete_read_only_groups() {
        let app = crate::config::AppConfig::default();
        let advanced = ConfigResponse::advanced_from_app(&app);

        assert_eq!(
            advanced,
            serde_json::json!({
                "runtime_kws": {
                    "kws_vpm_lib": app.runtime.kws_vpm_lib,
                    "kws_vpm_config_dir": app.runtime.kws_vpm_config_dir,
                    "kws_pcm": app.runtime.kws_pcm,
                    "kws_sample_rate": app.runtime.kws_sample_rate,
                    "kws_channels": app.runtime.kws_channels,
                    "kws_bits_per_sample": app.runtime.kws_bits_per_sample,
                    "kws_frame_ms": app.runtime.kws_frame_ms,
                    "kws_period_size": app.runtime.kws_period_size,
                    "kws_buffer_size": app.runtime.kws_buffer_size,
                    "kws_ref_channel_index": app.runtime.kws_ref_channel_index,
                    "kws_start_status": app.runtime.kws_start_status,
                },
                "capture": {
                    "pcm": app.capture.pcm,
                    "sample_rate": app.capture.sample_rate,
                    "channels": app.capture.channels,
                    "bits_per_sample": app.capture.bits_per_sample,
                    "period_size": app.capture.period_size,
                    "buffer_size": app.capture.buffer_size,
                    "threshold": app.capture.threshold,
                    "mic_gain": app.capture.mic_gain,
                    "block_ms": app.capture.block_ms,
                    "pre_roll_ms": app.capture.pre_roll_ms,
                    "silence_ms": app.capture.silence_ms,
                    "min_speech_ms": app.capture.min_speech_ms,
                    "max_utterance_s": app.capture.max_utterance_s,
                    "cooldown_s": app.capture.cooldown_s,
                    "print_levels": app.capture.print_levels,
                },
                "device_commands": {
                    "tts_command": app.device.tts_command,
                    "play_url_command": app.device.play_url_command,
                    "stop_audio_command": app.device.stop_audio_command,
                    "pause_audio_command": app.device.pause_audio_command,
                    "resume_audio_command": app.device.resume_audio_command,
                    "duck_audio_command": app.device.duck_audio_command,
                    "unduck_audio_command": app.device.unduck_audio_command,
                    "abort_command": app.device.abort_command,
                },
                "led_ids": {
                    "led_listening": app.device.led_listening,
                    "led_user_speaking": app.device.led_user_speaking,
                    "led_thinking": app.device.led_thinking,
                    "led_speaking": app.device.led_speaking,
                },
                "xiaomi_aivs": {
                    "sdk_lib": app.asr.xiaomi_aivs.sdk_lib,
                    "token_path": app.asr.xiaomi_aivs.token_path,
                    "miio_dir": app.asr.xiaomi_aivs.miio_dir,
                    "engine_mode": app.asr.xiaomi_aivs.engine_mode,
                    "connect_wait_ms": app.asr.xiaomi_aivs.connect_wait_ms,
                    "chunk_ms": app.asr.xiaomi_aivs.chunk_ms,
                    "wait_after_finish_ms": app.asr.xiaomi_aivs.wait_after_finish_ms,
                    "asr_only": app.asr.xiaomi_aivs.asr_only,
                    "allow_cloud_execution": app.asr.xiaomi_aivs.allow_cloud_execution,
                },
                "airplay_output": {
                    "hwaddr": app.airplay.hwaddr,
                    "backend": app.airplay.output.backend,
                    "aplay_path": app.airplay.output.aplay_path,
                    "device": app.airplay.output.device,
                    "format": app.airplay.output.format,
                },
            })
        );
    }

    #[test]
    fn committed_replacement_ignores_old_file_cleanup_failure() {
        let dir = tempfile::tempdir().unwrap();
        let formal_path = dir.path().join("agent.yaml");
        let temp_path = dir.path().join("candidate.tmp");
        let old_path = dir.path().join("candidate.replace-old");
        std::fs::write(&formal_path, b"old").unwrap();
        std::fs::write(&temp_path, b"new").unwrap();

        let result = replace_via_old_path(
            &temp_path,
            &formal_path,
            &old_path,
            |from, to| std::fs::rename(from, to),
            |_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected cleanup failure",
                ))
            },
        );

        assert!(result.is_ok());
        assert_eq!(std::fs::read(&formal_path).unwrap(), b"new");
        assert_eq!(std::fs::read(&old_path).unwrap(), b"old");
    }

    #[test]
    fn replacement_old_paths_do_not_collide_with_prior_cleanup_failures() {
        let formal_path = std::path::Path::new("agent.yaml");

        let first = replacement_old_path(formal_path);
        let second = replacement_old_path(formal_path);

        assert_ne!(first, second);
        assert!(first
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".replace-old"));
        assert_eq!(first.parent(), formal_path.parent());
    }

    #[tokio::test]
    async fn save_creates_backup_and_preserves_hidden_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.yaml");
        std::fs::copy(example_config_path(), &path).unwrap();
        let restart_required = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let store = ConfigStore::new(path.clone(), restart_required.clone());
        assert!(std::sync::Arc::ptr_eq(
            &store.restart_required_flag(),
            &restart_required
        ));
        drop(store.operation_lock().await);
        let mut update = store
            .load()
            .await
            .unwrap()
            .editable
            .into_update_keep_secrets();
        update.llm.model = "test-model".to_string();

        let saved = store.save(update).await.unwrap();

        assert_eq!(saved.editable.llm.model, "test-model");
        assert!(saved.restart_required);
        assert!(restart_required.load(std::sync::atomic::Ordering::SeqCst));
        assert!(path.with_file_name("agent.yaml.bak").exists());
        let current: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let backup: serde_yaml::Value = serde_yaml::from_str(
            &std::fs::read_to_string(path.with_file_name("agent.yaml.bak")).unwrap(),
        )
        .unwrap();
        assert_eq!(yaml_string(&current, &["llm", "model"]), "test-model");
        assert_ne!(yaml_string(&backup, &["llm", "model"]), "test-model");
        assert_eq!(
            yaml_string(&current, &["device", "tts_command"]),
            yaml_string(&backup, &["device", "tts_command"]),
        );
    }

    #[tokio::test]
    async fn invalid_native_qwen_config_never_replaces_formal_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.yaml");
        std::fs::copy(example_config_path(), &path).unwrap();
        let original = std::fs::read(&path).unwrap();
        let store = ConfigStore::new(path.clone(), Default::default());
        let mut update = store
            .load()
            .await
            .unwrap()
            .editable
            .into_update_keep_secrets();
        update.voice.runtime = "native_qwen".to_string();
        update.voice.qwen.api_key = SecretUpdate {
            mode: SecretMode::Clear,
            value: None,
        };

        let error = store.save(update).await.unwrap_err();

        assert_eq!(error.field(), Some("voice.qwen.api_key"));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(!path.with_file_name("agent.yaml.bak").exists());
    }

    #[tokio::test]
    async fn save_backup_failure_never_replaces_formal_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.yaml");
        std::fs::copy(example_config_path(), &path).unwrap();
        let original = std::fs::read(&path).unwrap();
        std::fs::create_dir(path.with_file_name("agent.yaml.bak")).unwrap();
        let store = ConfigStore::new(path.clone(), Default::default());
        let mut update = store
            .load()
            .await
            .unwrap()
            .editable
            .into_update_keep_secrets();
        update.llm.model = "test-model".to_string();

        let error = store.save(update).await.unwrap_err();

        assert!(matches!(error, ConfigStoreError::Io(_)));
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }

    #[tokio::test]
    async fn save_rejects_invalid_payload_fields_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.yaml");
        std::fs::copy(example_config_path(), &path).unwrap();
        let original = std::fs::read(&path).unwrap();
        let store = ConfigStore::new(path.clone(), Default::default());

        macro_rules! assert_invalid {
            ($field:literal, $change:expr) => {{
                let mut update = store
                    .load()
                    .await
                    .unwrap()
                    .editable
                    .into_update_keep_secrets();
                $change(&mut update);
                let error = store.save(update).await.unwrap_err();
                assert_eq!(error.field(), Some($field));
                assert_eq!(std::fs::read(&path).unwrap(), original);
                assert!(!path.with_file_name("agent.yaml.bak").exists());
            }};
        }

        assert_invalid!("voice.runtime", |update: &mut EditableConfig<
            SecretUpdate,
        >| update.voice.runtime =
            "other".to_string());
        assert_invalid!("asr.provider", |update: &mut EditableConfig<
            SecretUpdate,
        >| update.asr.provider =
            "other".to_string());
        assert_invalid!("music.provider", |update: &mut EditableConfig<
            SecretUpdate,
        >| update.music.provider =
            "other".to_string());
        assert_invalid!(
            "runtime.session_idle_timeout_s",
            |update: &mut EditableConfig<SecretUpdate>| update.runtime.session_idle_timeout_s = 0.0
        );
        assert_invalid!(
            "voice.qwen.connect_timeout_s",
            |update: &mut EditableConfig<SecretUpdate>| update.voice.qwen.connect_timeout_s = 0.0
        );
        assert_invalid!("asr.open_ai.timeout_s", |update: &mut EditableConfig<
            SecretUpdate,
        >| update
            .asr
            .open_ai
            .timeout_s =
            f64::NAN);
        assert_invalid!(
            "agent.web_search.max_results",
            |update: &mut EditableConfig<SecretUpdate>| update.agent.web_search.max_results = 11
        );
        assert_invalid!("airplay.port", |update: &mut EditableConfig<
            SecretUpdate,
        >| update.airplay.port = 0);
        assert_invalid!(
            "airplay.interruption.duck_gain",
            |update: &mut EditableConfig<SecretUpdate>| update.airplay.duck_gain = 1.1
        );
        assert_invalid!("llm.model", |update: &mut EditableConfig<SecretUpdate>| {
            update.llm.model = "x".repeat(4097)
        });
    }
}
