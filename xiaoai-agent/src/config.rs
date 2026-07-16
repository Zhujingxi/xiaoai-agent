use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::qwen_realtime::{AudioFormat, SampleRate};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AppConfig {
    pub root: Option<PathBuf>,
    pub runtime: RuntimeConfig,
    pub voice: RealtimeVoiceConfig,
    pub device: DeviceConfig,
    pub capture: CaptureConfig,
    pub asr: AsrConfig,
    pub llm: LlmConfig,
    pub agent: AgentConfig,
    pub mcp: McpConfig,
    pub music: MusicConfig,
    pub airplay: AirPlayConfig,
}

impl AppConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = fs::read_to_string(path)?;
        let mut config: Self = serde_yaml::from_str(&text)?;
        if config.root.is_none() {
            config.root = Some(
                path.parent()
                    .unwrap_or_else(|| Path::new("."))
                    .canonicalize()
                    .unwrap_or_else(|_| PathBuf::from(".")),
            );
        }
        config.resolve_paths();
        config.llm.base_url = openai_base_url(&config.llm.base_url);
        config.asr.open_ai.base_url = openai_base_url(&config.asr.open_ai.base_url);
        config.asr.openai_realtime.base_url =
            openai_realtime_base_url(&config.asr.openai_realtime.base_url);
        config.voice.validate()?;
        if config.mcp.home_assistant.enabled {
            config.mcp.home_assistant.validated_timeout_duration()?;
        }
        Ok(config)
    }

    fn resolve_paths(&mut self) {
        let Some(root) = self.root.as_deref() else {
            return;
        };
        resolve_optional_path(root, &mut self.music.netease.cookie_file);
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct RealtimeVoiceConfig {
    pub runtime: VoiceRuntime,
    pub qwen: QwenRealtimeConfig,
}

impl RealtimeVoiceConfig {
    pub fn validate(&self) -> Result<(), RealtimeVoiceConfigError> {
        self.qwen.validate(self.runtime == VoiceRuntime::NativeQwen)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum VoiceRuntime {
    #[default]
    Legacy,
    NativeQwen,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct QwenRealtimeConfig {
    pub url: String,
    pub api_key: String,
    pub model: String,
    pub voice: String,
    pub input_audio_format: AudioFormat,
    pub output_audio_format: AudioFormat,
    pub input_sample_rate: SampleRate,
    pub output_sample_rate: SampleRate,
    pub connect_timeout_s: f64,
    pub event_timeout_s: f64,
    pub tool_timeout_s: f64,
    pub max_tool_calls: usize,
    pub max_tool_iterations: usize,
}

impl Default for QwenRealtimeConfig {
    fn default() -> Self {
        Self {
            url: "https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api/v1/webrtc/realtime"
                .to_string(),
            api_key: "EMPTY".to_string(),
            model: "qwen3.5-omni-plus-realtime".to_string(),
            voice: "Tina".to_string(),
            input_audio_format: AudioFormat::Pcm,
            output_audio_format: AudioFormat::Pcm,
            input_sample_rate: SampleRate(16_000),
            output_sample_rate: SampleRate(48_000),
            connect_timeout_s: 10.0,
            event_timeout_s: 30.0,
            tool_timeout_s: 10.0,
            max_tool_calls: 8,
            max_tool_iterations: 4,
        }
    }
}

impl QwenRealtimeConfig {
    fn validate(&self, enabled: bool) -> Result<(), RealtimeVoiceConfigError> {
        if !self.url.starts_with("https://") {
            return Err(RealtimeVoiceConfigError::InvalidUrl);
        }
        if self.model.trim().is_empty() {
            return Err(RealtimeVoiceConfigError::EmptyModel);
        }
        if self.voice.trim().is_empty() {
            return Err(RealtimeVoiceConfigError::EmptyVoice);
        }
        if self.input_sample_rate.0 != 16_000 {
            return Err(RealtimeVoiceConfigError::InvalidInputSampleRate(
                self.input_sample_rate.0,
            ));
        }
        if self.output_sample_rate.0 != 48_000 {
            return Err(RealtimeVoiceConfigError::InvalidOutputSampleRate(
                self.output_sample_rate.0,
            ));
        }
        if !self.connect_timeout_s.is_finite() || self.connect_timeout_s <= 0.0 {
            return Err(RealtimeVoiceConfigError::InvalidConnectTimeout);
        }
        if !self.event_timeout_s.is_finite() || self.event_timeout_s <= 0.0 {
            return Err(RealtimeVoiceConfigError::InvalidEventTimeout);
        }
        if !self.tool_timeout_s.is_finite() || self.tool_timeout_s <= 0.0 {
            return Err(RealtimeVoiceConfigError::InvalidToolTimeout);
        }
        if self.max_tool_calls == 0 || self.max_tool_iterations == 0 {
            return Err(RealtimeVoiceConfigError::InvalidToolLimit);
        }
        if enabled && (self.api_key.trim().is_empty() || self.api_key == "EMPTY") {
            return Err(RealtimeVoiceConfigError::MissingApiKey);
        }
        if enabled && self.url.contains("{WorkspaceId}") {
            return Err(RealtimeVoiceConfigError::MissingWorkspaceId);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum RealtimeVoiceConfigError {
    #[error("voice.qwen.url must use the https:// WebRTC signaling endpoint")]
    InvalidUrl,
    #[error("voice.qwen.model must not be empty")]
    EmptyModel,
    #[error("voice.qwen.voice must not be empty")]
    EmptyVoice,
    #[error("voice.qwen.input_sample_rate must be 16000, got {0}")]
    InvalidInputSampleRate(u32),
    #[error("voice.qwen.output_sample_rate must be 48000 for WebRTC Opus, got {0}")]
    InvalidOutputSampleRate(u32),
    #[error("voice.qwen.connect_timeout_s must be finite and greater than zero")]
    InvalidConnectTimeout,
    #[error("voice.qwen.event_timeout_s must be finite and greater than zero")]
    InvalidEventTimeout,
    #[error("voice.qwen.tool_timeout_s must be finite and greater than zero")]
    InvalidToolTimeout,
    #[error("voice.qwen max_tool_calls and max_tool_iterations must be greater than zero")]
    InvalidToolLimit,
    #[error("voice.qwen.api_key is required when voice.runtime is native_qwen")]
    MissingApiKey,
    #[error("voice.qwen.url must replace {{WorkspaceId}} with the Bailian workspace ID")]
    MissingWorkspaceId,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub kws_vpm_lib: String,
    pub kws_vpm_config_dir: String,
    pub kws_pcm: String,
    pub kws_sample_rate: u32,
    pub kws_channels: u32,
    pub kws_bits_per_sample: u32,
    pub kws_frame_ms: u32,
    pub kws_period_size: u32,
    pub kws_buffer_size: u32,
    pub kws_ref_channel_index: u32,
    pub kws_start_status: Option<i32>,
    pub acknowledge_text: Vec<String>,
    pub session_idle_timeout_s: f64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            kws_vpm_lib: "/usr/lib/libvpm.so".to_string(),
            kws_vpm_config_dir: "/usr/share/mipns/vpm/json_segment".to_string(),
            kws_pcm: "noop".to_string(),
            kws_sample_rate: 48_000,
            kws_channels: 4,
            kws_bits_per_sample: 32,
            kws_frame_ms: 8,
            kws_period_size: 384,
            kws_buffer_size: 4096,
            kws_ref_channel_index: 3,
            kws_start_status: Some(6),
            acknowledge_text: vec!["在".to_string(), "我在".to_string(), "哎".to_string()],
            session_idle_timeout_s: 20.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DeviceConfig {
    pub tts_command: String,
    pub play_url_command: String,
    pub stop_audio_command: String,
    pub pause_audio_command: String,
    pub resume_audio_command: String,
    pub duck_audio_command: String,
    pub unduck_audio_command: String,
    pub abort_command: String,
    pub led_enabled: bool,
    pub led_listening: u8,
    pub led_user_speaking: u8,
    pub led_thinking: u8,
    pub led_speaking: u8,
}

impl Default for DeviceConfig {
    fn default() -> Self {
        Self {
            tts_command: "/usr/sbin/tts_play.sh {text}".to_string(),
            play_url_command:
                "killall miplayer 2>/dev/null; miplayer -f {url} >/tmp/xiaoai-miplayer.log 2>&1 &"
                    .to_string(),
            stop_audio_command: "killall tts_play.sh miplayer 2>/dev/null; mphelper pause"
                .to_string(),
            pause_audio_command:
                "mphelper pause 2>/dev/null || true; for p in $(pidof miplayer 2>/dev/null); do kill -STOP \"$p\" 2>/dev/null || true; done"
                    .to_string(),
            resume_audio_command:
                "for p in $(pidof miplayer 2>/dev/null); do kill -CONT \"$p\" 2>/dev/null || true; done; mphelper play 2>/dev/null || true"
                    .to_string(),
            duck_audio_command:
                "amixer sget mysoftvol | awk -F'[][]' '/%/ {print $2; exit}' | tr -d '%' >/tmp/xiaoai-mysoftvol.before; amixer sset mysoftvol 25% >/dev/null 2>&1 || true"
                    .to_string(),
            unduck_audio_command:
                "v=$(cat /tmp/xiaoai-mysoftvol.before 2>/dev/null); if [ -n \"$v\" ]; then amixer sset mysoftvol \"${v}%\" >/dev/null 2>&1 || true; fi; rm -f /tmp/xiaoai-mysoftvol.before"
                    .to_string(),
            abort_command: "killall tts_play.sh miplayer 2>/dev/null; mphelper pause 2>/dev/null || true; ubus call mediaplayer media_control '{\"player\":\"mediaplayer\",\"action\":\"pause\",\"volume\":0}' 2>/dev/null || true; ubus call mediaplayer player_play_operation '{\"media\":\"app_ios\",\"action\":\"stop\"}' 2>/dev/null || true".to_string(),
            led_enabled: true,
            led_listening: 3,
            led_user_speaking: 4,
            led_thinking: 2,
            led_speaking: 5,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CaptureConfig {
    pub pcm: String,
    pub sample_rate: u32,
    pub channels: u16,
    pub bits_per_sample: u16,
    pub period_size: u32,
    pub buffer_size: u32,
    pub threshold: f64,
    pub mic_gain: f64,
    pub block_ms: u64,
    pub pre_roll_ms: u64,
    pub silence_ms: u64,
    pub min_speech_ms: u64,
    pub max_utterance_s: f64,
    pub cooldown_s: f64,
    pub print_levels: bool,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            pcm: "vpm_asr".to_string(),
            sample_rate: 16000,
            channels: 1,
            bits_per_sample: 16,
            period_size: 360,
            buffer_size: 1440,
            threshold: 0.006,
            mic_gain: 1.0,
            block_ms: 100,
            pre_roll_ms: 300,
            silence_ms: 1500,
            min_speech_ms: 300,
            max_utterance_s: 15.0,
            cooldown_s: 0.7,
            print_levels: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AsrConfig {
    pub provider: AsrProvider,
    pub open_ai: OpenAiAsrConfig,
    pub openai_realtime: OpenAiRealtimeAsrConfig,
    pub xiaomi_aivs: XiaomiAivsAsrConfig,
}

impl Default for AsrConfig {
    fn default() -> Self {
        Self {
            provider: AsrProvider::XiaomiAivs,
            open_ai: OpenAiAsrConfig::default(),
            openai_realtime: OpenAiRealtimeAsrConfig::default(),
            xiaomi_aivs: XiaomiAivsAsrConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AsrProvider {
    #[default]
    #[serde(alias = "openai")]
    OpenAi,
    #[serde(alias = "openai_realtime", alias = "open_ai_realtime")]
    OpenAiRealtime,
    XiaomiAivs,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OpenAiAsrConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub language: String,
    pub prompt: String,
    pub timeout_s: f64,
    pub retries: u32,
}

impl Default for OpenAiAsrConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: "EMPTY".to_string(),
            model: "whisper-1".to_string(),
            language: "zh".to_string(),
            prompt: String::new(),
            timeout_s: 5.0,
            retries: 1,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OpenAiRealtimeAsrConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub target_sample_rate: u32,
    pub chunk_ms: u32,
    pub timeout_s: f64,
    pub retries: u32,
}

impl Default for OpenAiRealtimeAsrConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: "EMPTY".to_string(),
            model: "gpt-realtime-whisper".to_string(),
            target_sample_rate: 24_000,
            chunk_ms: 200,
            timeout_s: 10.0,
            retries: 1,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct XiaomiAivsAsrConfig {
    pub sdk_lib: String,
    pub token_path: PathBuf,
    pub miio_dir: PathBuf,
    pub engine_mode: i32,
    pub connect_wait_ms: u64,
    pub chunk_ms: u64,
    pub wait_after_finish_ms: u64,
    pub asr_only: bool,
    pub allow_cloud_execution: bool,
}

impl Default for XiaomiAivsAsrConfig {
    fn default() -> Self {
        Self {
            sdk_lib: "/usr/lib/libaivs_sdk.so".to_string(),
            token_path: PathBuf::from("/data/TOKEN"),
            miio_dir: PathBuf::from("/data/miio"),
            engine_mode: 2,
            connect_wait_ms: 1500,
            chunk_ms: 100,
            wait_after_finish_ms: 15000,
            asr_only: true,
            allow_cloud_execution: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LlmConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub timeout_s: f64,
    pub max_tokens: u64,
    pub retries: u32,
    pub temperature: f64,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: "EMPTY".to_string(),
            model: "gpt-5.4-mini".to_string(),
            timeout_s: 5.0,
            max_tokens: 300,
            retries: 1,
            temperature: 0.5,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AgentConfig {
    pub timezone: String,
    pub weather: WeatherConfig,
    pub web_search: WebSearchConfig,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            timezone: "Asia/Shanghai".to_string(),
            weather: WeatherConfig::default(),
            web_search: WebSearchConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WeatherConfig {
    pub qweather_url: String,
    pub default_location: String,
    pub ip_lookup_url: String,
    pub timeout_s: f64,
}

impl Default for WeatherConfig {
    fn default() -> Self {
        Self {
            qweather_url: String::new(),
            default_location: String::new(),
            ip_lookup_url: "https://ipapi.co/json/".to_string(),
            timeout_s: 5.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WebSearchConfig {
    pub enabled: bool,
    pub tavily_url: String,
    pub api_key: String,
    pub timeout_s: f64,
    pub max_results: usize,
    pub search_depth: String,
}

impl Default for WebSearchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tavily_url: "https://api.tavily.com/search".to_string(),
            api_key: String::new(),
            timeout_s: 10.0,
            max_results: 3,
            search_depth: "basic".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct McpConfig {
    pub home_assistant: HomeAssistantMcpConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HomeAssistantMcpConfig {
    pub enabled: bool,
    pub url: String,
    pub token: String,
    pub timeout_s: f64,
}

impl Default for HomeAssistantMcpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: "http://homeassistant.local:8123/mcp_server/sse".to_string(),
            token: String::new(),
            timeout_s: 5.0,
        }
    }
}

impl HomeAssistantMcpConfig {
    pub(crate) fn validated_timeout_duration(&self) -> anyhow::Result<Duration> {
        anyhow::ensure!(
            self.timeout_s.is_finite() && self.timeout_s > 0.0,
            "mcp.home_assistant.timeout_s must be finite and greater than zero"
        );
        let timeout = Duration::try_from_secs_f64(self.timeout_s).map_err(|_| {
            anyhow::anyhow!("mcp.home_assistant.timeout_s is too large to represent as a duration")
        })?;
        anyhow::ensure!(
            tokio::time::Instant::now().checked_add(timeout).is_some(),
            "mcp.home_assistant.timeout_s is too large to use as a Tokio deadline"
        );
        Ok(timeout)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MusicConfig {
    pub enabled: bool,
    pub provider: String,
    pub interruption: MusicInterruptionConfig,
    pub netease: NeteaseConfig,
    pub navidrome: NavidromeConfig,
}

impl Default for MusicConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "navidrome".to_string(),
            interruption: MusicInterruptionConfig::default(),
            netease: NeteaseConfig::default(),
            navidrome: NavidromeConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AirPlayConfig {
    pub enabled: bool,
    pub name: String,
    pub port: u16,
    pub password: String,
    pub hwaddr: String,
    pub output: AirPlayOutputConfig,
    pub interruption: AirPlayInterruptionConfig,
}

impl Default for AirPlayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            name: "XiaoAI AirPlay".to_string(),
            port: 5000,
            password: String::new(),
            hwaddr: String::new(),
            output: AirPlayOutputConfig::default(),
            interruption: AirPlayInterruptionConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AirPlayOutputConfig {
    pub backend: String,
    pub aplay_path: String,
    pub device: String,
    pub format: String,
}

impl Default for AirPlayOutputConfig {
    fn default() -> Self {
        Self {
            backend: "aplay".to_string(),
            aplay_path: "/usr/bin/aplay".to_string(),
            device: "default".to_string(),
            format: "S16_LE".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AirPlayInterruptionConfig {
    pub mode: String,
    pub duck_gain: f32,
}

impl Default for AirPlayInterruptionConfig {
    fn default() -> Self {
        Self {
            mode: "duck".to_string(),
            duck_gain: 0.25,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MusicInterruptionConfig {
    pub mode: String,
}

impl Default for MusicInterruptionConfig {
    fn default() -> Self {
        Self {
            mode: "pause".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NeteaseConfig {
    pub api_base_url: String,
    pub login_mode: String,
    pub account: String,
    pub phone: String,
    pub password: String,
    pub md5_password: String,
    pub cookie_file: Option<PathBuf>,
    pub default_level: String,
    pub timeout_s: f64,
    pub login_on_start: bool,
}

impl Default for NeteaseConfig {
    fn default() -> Self {
        Self {
            api_base_url: "http://127.0.0.1:3300".to_string(),
            login_mode: "captcha".to_string(),
            account: String::new(),
            phone: String::new(),
            password: String::new(),
            md5_password: String::new(),
            cookie_file: None,
            default_level: "standard".to_string(),
            timeout_s: 5.0,
            login_on_start: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NavidromeConfig {
    pub base_url: String,
    pub username: String,
    pub password: String,
    pub api_version: String,
    pub timeout_s: f64,
}

impl Default for NavidromeConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:4533".to_string(),
            username: String::new(),
            password: String::new(),
            api_version: "1.16.1".to_string(),
            timeout_s: 5.0,
        }
    }
}

pub fn openai_base_url(raw: &str) -> String {
    let base = raw.trim_end_matches('/');
    if base.ends_with("/v1") {
        base.to_string()
    } else {
        format!("{base}/v1")
    }
}

pub fn openai_realtime_base_url(raw: &str) -> String {
    let base = raw.trim_end_matches('/');
    if base.ends_with("/v1") || base.ends_with("/realtime") {
        base.to_string()
    } else {
        format!("{base}/v1")
    }
}

pub fn require_music_enabled(config: &AppConfig) -> anyhow::Result<()> {
    anyhow::ensure!(config.music.enabled, "music.enabled is false");
    Ok(())
}

pub fn timeout_duration(seconds: f64) -> Duration {
    Duration::from_secs_f64(seconds.max(0.1))
}

fn resolve_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn resolve_optional_path(root: &Path, path: &mut Option<PathBuf>) {
    if let Some(value) = path {
        *value = resolve_path(root, value);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        AppConfig, AsrConfig, AsrProvider, RealtimeVoiceConfig, RealtimeVoiceConfigError,
        VoiceRuntime,
    };

    fn load_config(yaml: &str, label: &str) -> anyhow::Result<AppConfig> {
        let path = std::env::temp_dir().join(format!(
            "xiaoai-agent-config-{}-{label}.yaml",
            std::process::id()
        ));
        fs::write(&path, yaml).expect("failed to write temporary config");
        let result = AppConfig::load(&path);
        fs::remove_file(path).expect("failed to remove temporary config");
        result
    }

    #[test]
    fn parses_openai_realtime_provider_alias() {
        let config: AsrConfig = serde_yaml::from_str("provider: openai_realtime\n").unwrap();
        assert!(matches!(config.provider, AsrProvider::OpenAiRealtime));
    }

    #[test]
    fn realtime_voice_defaults_to_legacy_and_validates() {
        let config: RealtimeVoiceConfig = serde_yaml::from_str("{}\n").unwrap();
        assert_eq!(config.runtime, VoiceRuntime::Legacy);
        assert_eq!(config.qwen.voice, "Tina");
        assert_eq!(config.qwen.input_sample_rate.0, 16_000);
        assert_eq!(config.qwen.output_sample_rate.0, 48_000);
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn native_qwen_requires_api_key() {
        let config: RealtimeVoiceConfig = serde_yaml::from_str("runtime: native_qwen\n").unwrap();
        assert_eq!(
            config.validate(),
            Err(RealtimeVoiceConfigError::MissingApiKey)
        );
    }

    #[test]
    fn native_qwen_requires_workspace_scoped_webrtc_url() {
        let config: RealtimeVoiceConfig =
            serde_yaml::from_str("runtime: native_qwen\nqwen:\n  api_key: test\n").unwrap();
        assert_eq!(
            config.validate(),
            Err(RealtimeVoiceConfigError::MissingWorkspaceId)
        );
    }

    #[test]
    fn rejects_unsupported_qwen_audio_rate() {
        let config: RealtimeVoiceConfig =
            serde_yaml::from_str("qwen:\n  input_sample_rate: 24000\n").unwrap();
        assert_eq!(
            config.validate(),
            Err(RealtimeVoiceConfigError::InvalidInputSampleRate(24_000))
        );
    }

    #[test]
    fn parses_and_validates_native_tool_limits() {
        let config: RealtimeVoiceConfig = serde_yaml::from_str(
            "runtime: native_qwen\nqwen:\n  url: https://wsid.cn-beijing.maas.aliyuncs.com/api/v1/webrtc/realtime\n  api_key: test\n  tool_timeout_s: 2.5\n  max_tool_calls: 3\n  max_tool_iterations: 2\n",
        )
        .unwrap();
        assert_eq!(config.qwen.tool_timeout_s, 2.5);
        assert_eq!(config.qwen.max_tool_calls, 3);
        assert_eq!(config.qwen.max_tool_iterations, 2);
        assert_eq!(config.validate(), Ok(()));

        for yaml in [
            "qwen:\n  tool_timeout_s: 0\n",
            "qwen:\n  max_tool_calls: 0\n",
            "qwen:\n  max_tool_iterations: 0\n",
        ] {
            let invalid: RealtimeVoiceConfig = serde_yaml::from_str(yaml).unwrap();
            assert!(
                invalid.validate().is_err(),
                "accepted invalid config: {yaml}"
            );
        }
    }

    #[test]
    fn enabled_mcp_rejects_non_positive_and_non_finite_timeout() {
        for (label, timeout) in [
            ("zero", "0"),
            ("negative", "-1"),
            ("positive-infinity", ".inf"),
            ("negative-infinity", "-.inf"),
            ("nan", ".nan"),
        ] {
            let yaml =
                format!("mcp:\n  home_assistant:\n    enabled: true\n    timeout_s: {timeout}\n");
            let error = load_config(&yaml, label).expect_err("invalid MCP timeout accepted");
            assert!(
                error
                    .to_string()
                    .contains("mcp.home_assistant.timeout_s must be finite and greater than zero"),
                "unclear native MCP timeout error for {timeout}: {error}"
            );
        }
    }

    #[test]
    fn enabled_mcp_rejects_timeout_too_large_for_tokio_deadline() {
        let timeout = 1.0e19;
        assert!(
            std::time::Duration::try_from_secs_f64(timeout).is_ok(),
            "regression value must remain representable as Duration"
        );
        let yaml =
            format!("mcp:\n  home_assistant:\n    enabled: true\n    timeout_s: {timeout:.1e}\n");
        let error = load_config(&yaml, "tokio-deadline-overflow")
            .expect_err("MCP timeout that overflows a Tokio deadline was accepted");
        assert!(
            error
                .to_string()
                .contains("mcp.home_assistant.timeout_s is too large to use as a Tokio deadline"),
            "unclear native MCP deadline overflow error: {error}"
        );
    }

    #[test]
    fn enabled_mcp_accepts_valid_timeout_and_legacy_defaults_stay_unchanged() {
        let native = "voice:\n  runtime: native_qwen\n  qwen:\n    url: https://wsid.cn-beijing.maas.aliyuncs.com/api/v1/webrtc/realtime\n    api_key: test\nmcp:\n  home_assistant:\n    enabled: true\n    timeout_s: 0.25\n";
        assert_eq!(
            load_config(native, "valid-native")
                .expect("valid native MCP timeout rejected")
                .mcp
                .home_assistant
                .timeout_s,
            0.25
        );

        let legacy = "mcp:\n  home_assistant:\n    enabled: true\n    timeout_s: 5\n";
        let legacy =
            load_config(legacy, "valid-legacy").expect("valid legacy MCP timeout rejected");
        assert_eq!(legacy.voice.runtime, VoiceRuntime::Legacy);
        assert_eq!(legacy.mcp.home_assistant.timeout_s, 5.0);
    }
}
