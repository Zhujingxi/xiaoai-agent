use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tracing_subscriber::fmt::MakeWriter;

use crate::config::{AppConfig, AsrProvider, VoiceRuntime};

const LAST_ERROR_MAX_CHARS: usize = 2048;

#[derive(Debug, Clone)]
pub struct LogBuffer {
    entries: Arc<Mutex<VecDeque<String>>>,
    capacity: usize,
    max_chars: usize,
}

impl LogBuffer {
    pub fn new(capacity: usize, max_chars: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
            max_chars,
        }
    }

    pub fn push(&self, message: impl AsRef<str>) {
        let message = message.as_ref().trim_end_matches(['\r', '\n']);
        if message.is_empty() {
            return;
        }

        let message = truncate_chars(message, self.max_chars);
        if message.is_empty() {
            return;
        }
        let mut entries = lock_recover(&self.entries);
        entries.push_back(message);
        while entries.len() > self.capacity {
            entries.pop_front();
        }
    }

    pub fn entries(&self, limit: usize) -> Vec<String> {
        let entries = lock_recover(&self.entries);
        let skip = entries.len().saturating_sub(limit);
        entries.iter().skip(skip).cloned().collect()
    }
}

pub struct LogWriter {
    bytes: Vec<u8>,
    buffer: LogBuffer,
}

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        let message = String::from_utf8_lossy(&self.bytes);
        self.buffer.push(message.as_ref());
    }
}

impl<'a> MakeWriter<'a> for LogBuffer {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter {
            bytes: Vec::new(),
            buffer: self.clone(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StatusSnapshot {
    pub version: &'static str,
    pub started_at_unix_ms: u64,
    pub uptime_s: u64,
    pub kws_started: bool,
    pub active_turn: bool,
    pub restart_required: bool,
    pub voice_runtime: String,
    pub asr_provider: String,
    pub web_search_enabled: bool,
    pub home_assistant_enabled: bool,
    pub music_enabled: bool,
    pub airplay_enabled: bool,
    pub last_error: Option<String>,
}

pub struct RuntimeStatus {
    started_at: SystemTime,
    started: Instant,
    kws_started: AtomicBool,
    active_turn: AtomicBool,
    restart_required: Arc<AtomicBool>,
    voice_runtime: String,
    asr_provider: String,
    web_search_enabled: bool,
    home_assistant_enabled: bool,
    music_enabled: bool,
    airplay_enabled: bool,
    last_error: Mutex<Option<String>>,
    redactions: Vec<String>,
    logs: LogBuffer,
}

impl RuntimeStatus {
    pub fn new(config: Arc<AppConfig>, logs: LogBuffer, restart_required: Arc<AtomicBool>) -> Self {
        let voice_runtime = match config.voice.runtime {
            VoiceRuntime::Legacy => "legacy",
            VoiceRuntime::NativeQwen => "native_qwen",
        }
        .to_string();
        let asr_provider = match &config.asr.provider {
            AsrProvider::OpenAi => "open_ai",
            AsrProvider::OpenAiRealtime => "openai_realtime",
            AsrProvider::XiaomiAivs => "xiaomi_aivs",
        }
        .to_string();
        let mut redactions = vec![
            config.voice.qwen.api_key.clone(),
            config.asr.open_ai.api_key.clone(),
            config.asr.openai_realtime.api_key.clone(),
            config.llm.api_key.clone(),
            config.agent.weather.qweather_url.clone(),
            config.agent.web_search.api_key.clone(),
            config.mcp.home_assistant.token.clone(),
            config.music.navidrome.password.clone(),
            config.music.netease.password.clone(),
            config.music.netease.md5_password.clone(),
            config.airplay.password.clone(),
        ];
        redactions.retain(|secret| !secret.is_empty());
        redactions
            .sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
        redactions.dedup();

        Self {
            started_at: SystemTime::now(),
            started: Instant::now(),
            kws_started: AtomicBool::new(false),
            active_turn: AtomicBool::new(false),
            restart_required,
            voice_runtime,
            asr_provider,
            web_search_enabled: config.agent.web_search.enabled,
            home_assistant_enabled: config.mcp.home_assistant.enabled,
            music_enabled: config.music.enabled,
            airplay_enabled: config.airplay.enabled,
            last_error: Mutex::new(None),
            redactions,
            logs,
        }
    }

    pub fn set_kws_started(&self, started: bool) {
        self.kws_started.store(started, Ordering::SeqCst);
    }

    pub fn set_active_turn(&self, active: bool) {
        self.active_turn.store(active, Ordering::SeqCst);
    }

    pub fn set_last_error(&self, message: impl AsRef<str>) {
        let mut redacted = message.as_ref().to_string();
        for secret in &self.redactions {
            redacted = redacted.replace(secret, "[redacted]");
        }
        *lock_recover(&self.last_error) = Some(truncate_chars(&redacted, LAST_ERROR_MAX_CHARS));
    }

    pub fn clear_last_error(&self) {
        *lock_recover(&self.last_error) = None;
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        let started_at_unix_ms = self
            .started_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        StatusSnapshot {
            version: env!("CARGO_PKG_VERSION"),
            started_at_unix_ms,
            uptime_s: self.started.elapsed().as_secs(),
            kws_started: self.kws_started.load(Ordering::SeqCst),
            active_turn: self.active_turn.load(Ordering::SeqCst),
            restart_required: self.restart_required.load(Ordering::SeqCst),
            voice_runtime: self.voice_runtime.clone(),
            asr_provider: self.asr_provider.clone(),
            web_search_enabled: self.web_search_enabled,
            home_assistant_enabled: self.home_assistant_enabled,
            music_enabled: self.music_enabled,
            airplay_enabled: self.airplay_enabled,
            last_error: lock_recover(&self.last_error).clone(),
        }
    }

    pub fn log_entries(&self, limit: usize) -> Vec<String> {
        self.logs.entries(limit)
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::{LogBuffer, RuntimeStatus};
    use crate::config::{AppConfig, AsrProvider, VoiceRuntime};

    #[test]
    fn log_buffer_is_bounded_and_truncates_entries() {
        let logs = LogBuffer::new(2, 8);
        logs.push("1234567890");
        assert_eq!(logs.entries(200), vec!["12345678"]);
        logs.push("second");
        logs.push("third");
        assert_eq!(logs.entries(200), vec!["second", "third"]);
    }

    #[test]
    fn log_buffer_ignores_entries_truncated_to_empty() {
        let logs = LogBuffer::new(2, 0);
        logs.push("not empty before truncation");
        assert!(logs.entries(200).is_empty());
    }

    #[test]
    fn runtime_snapshot_exposes_state_without_secrets() {
        let mut config = AppConfig::default();
        config.llm.api_key = "snapshot-secret".to_string();
        let config = std::sync::Arc::new(config);
        let restart_required = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let status =
            RuntimeStatus::new(config, LogBuffer::new(200, 2048), restart_required.clone());
        status.set_kws_started(true);
        status.set_active_turn(true);
        status.set_last_error("ASR failed with snapshot-secret");
        restart_required.store(true, std::sync::atomic::Ordering::SeqCst);
        let snapshot = status.snapshot();
        assert!(snapshot.started_at_unix_ms > 0);
        assert!(snapshot.kws_started);
        assert!(snapshot.active_turn);
        assert!(snapshot.restart_required);
        assert_eq!(snapshot.voice_runtime, "legacy");
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(!json.contains("snapshot-secret"));
        assert!(json.contains("[redacted]"));
    }

    #[test]
    fn log_buffer_handles_unicode_line_endings_limits_and_writer_drops() {
        let logs = LogBuffer::new(3, 3);
        logs.push("你好世界\r\n");
        logs.push("\r\n");
        {
            let mut writer = tracing_subscriber::fmt::MakeWriter::make_writer(&logs);
            writer.write_all(b"writer\n").unwrap();
        }
        logs.push("last");

        assert_eq!(logs.entries(200), vec!["你好世", "wri", "las"]);
        assert_eq!(logs.entries(1), vec!["las"]);
        assert!(logs.entries(0).is_empty());
    }

    #[test]
    fn runtime_status_copies_startup_config_and_redacts_all_secrets() {
        let mut app = AppConfig::default();
        app.voice.runtime = VoiceRuntime::NativeQwen;
        app.asr.provider = AsrProvider::OpenAiRealtime;
        app.agent.web_search.enabled = true;
        app.mcp.home_assistant.enabled = true;
        app.music.enabled = true;
        app.airplay.enabled = true;

        let secrets = [
            "voice-secret-0001",
            "asr-secret-0002",
            "realtime-secret-0003",
            "llm-secret-0004",
            "https://weather.example/v7/weather/now?location=101010100&key=weather-secret-0005",
            "search-secret-0006",
            "ha-secret-0007",
            "navidrome-secret-0008",
            "netease-secret-0009",
            "md5-secret-0010",
            "airplay-secret-0011",
        ];
        app.voice.qwen.api_key = secrets[0].to_string();
        app.asr.open_ai.api_key = secrets[1].to_string();
        app.asr.openai_realtime.api_key = secrets[2].to_string();
        app.llm.api_key = secrets[3].to_string();
        app.agent.weather.qweather_url = secrets[4].to_string();
        app.agent.web_search.api_key = secrets[5].to_string();
        app.mcp.home_assistant.token = secrets[6].to_string();
        app.music.navidrome.password = secrets[7].to_string();
        app.music.netease.password = secrets[8].to_string();
        app.music.netease.md5_password = secrets[9].to_string();
        app.airplay.password = secrets[10].to_string();

        let mut config = std::sync::Arc::new(app);
        let restart_required = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let status =
            RuntimeStatus::new(config.clone(), LogBuffer::new(200, 2048), restart_required);
        assert_eq!(std::sync::Arc::strong_count(&config), 1);
        let mutable_config = std::sync::Arc::get_mut(&mut config).unwrap();
        mutable_config.voice.runtime = VoiceRuntime::Legacy;
        mutable_config.asr.provider = AsrProvider::OpenAi;
        mutable_config.agent.web_search.enabled = false;
        mutable_config.mcp.home_assistant.enabled = false;
        mutable_config.music.enabled = false;
        mutable_config.airplay.enabled = false;

        status.set_last_error(format!("all secrets: {}", secrets.join(" | ")));
        let snapshot = status.snapshot();
        assert_eq!(snapshot.voice_runtime, "native_qwen");
        assert_eq!(snapshot.asr_provider, "openai_realtime");
        assert!(snapshot.web_search_enabled);
        assert!(snapshot.home_assistant_enabled);
        assert!(snapshot.music_enabled);
        assert!(snapshot.airplay_enabled);
        let error = snapshot.last_error.unwrap();
        for secret in secrets {
            assert!(!error.contains(secret), "secret leaked: {secret}");
        }
        assert_eq!(error.matches("[redacted]").count(), 11);
    }

    #[test]
    fn last_error_is_unicode_bounded_and_clearable() {
        let status = RuntimeStatus::new(
            std::sync::Arc::new(AppConfig::default()),
            LogBuffer::new(200, 2048),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        status.set_last_error("界".repeat(3000));
        assert_eq!(status.snapshot().last_error.unwrap().chars().count(), 2048);
        status.clear_last_error();
        assert_eq!(status.snapshot().last_error, None);
    }

    #[test]
    fn runtime_status_delegates_log_entries() {
        let logs = LogBuffer::new(2, 2048);
        let status = RuntimeStatus::new(
            std::sync::Arc::new(AppConfig::default()),
            logs.clone(),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        logs.push("first");
        logs.push("second");
        assert_eq!(status.log_entries(1), vec!["second"]);
    }
}
