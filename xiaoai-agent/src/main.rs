mod agent;
mod airplay;
mod asr;
mod audio;
mod base;
mod capture;
mod config;
mod device;
mod mcp;
mod mcp_legacy_sse;
mod monitor;
mod music;
mod qwen_realtime;
mod qwen_voice;
mod shell;
mod tools;
mod vad;
mod weather;
mod web;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::EnvFilter;

use crate::agent::AgentRuntime;
use crate::airplay::AirPlayService;
use crate::asr::AsrClient;
use crate::audio::record::AudioRecorder;
use crate::capture::{record_utterance, record_utterance_streaming};
use crate::config::{AppConfig, CaptureConfig, DeviceConfig, QwenRealtimeConfig, VoiceRuntime};
use crate::device::Device;
use crate::monitor::kws::{KwsMonitor, KwsMonitorEvent};
use crate::music::MusicService;
use crate::qwen_voice::{QwenVoiceService, SessionHandle};

const ASR_SERVICE_ERROR_PROMPT: &str = "抱歉，语音识别服务遇到问题，请稍后重试";
const LLM_SERVICE_ERROR_PROMPT: &str = "抱歉，大模型服务遇到问题，请稍后重试";
// Progress tone played while a slow remote brain (hermes runtime) is working.
// Keep in sync with the device.thinking_sound_command default.
const THINKING_SOUND_WAV_PATH: &str = "/tmp/xiaoai-thinking.wav";
// Bounds how often a dead prewarmed native session may be replaced. Async
// failures (connect timeout, ICE, server-side idle timeout) already take
// seconds to surface; this only guards against synchronous spawn errors.
const WARM_RESPAWN_MIN_INTERVAL: Duration = Duration::from_secs(5);
// Rotate the prewarmed native session well before Qwen's ~300 s idle timeout.
// A session that is allowed to idle out reconnects inside run(), and such
// reconnected sessions die at ICE level within ~48 s; freshly spawned
// sessions stay healthy for the full 300 s.
const WARM_SESSION_MAX_AGE: Duration = Duration::from_secs(240);
// A fail-closed native MCP client is reconnected while the speaker is idle.
// This bounds how often a reconnect attempt may run when Home Assistant
// stays unreachable.
const MCP_RECONNECT_MIN_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Parser)]
#[command(name = "xiaoai-agent")]
#[command(about = "Standalone XiaoAI on-device agent: flexkws + cloud ASR + Rig agent")]
struct Cli {
    #[arg(short, long, default_value = "/data/open-xiaoai/agent.yaml")]
    config: PathBuf,

    #[arg(long, default_value = "0.0.0.0")]
    web_bind: std::net::IpAddr,

    #[arg(long, default_value_t = 8080)]
    web_port: u16,
}

struct ActiveTurn {
    task: JoinHandle<()>,
    native_session: Option<SessionHandle>,
    activated: Arc<AtomicBool>,
    preserve_active_status: Arc<AtomicBool>,
}

impl ActiveTurn {
    fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    fn interrupt_native(&self) -> bool {
        if self.task.is_finished() || !self.activated.load(Ordering::SeqCst) {
            return false;
        }
        let Some(session) = &self.native_session else {
            return false;
        };
        session.interrupt();
        true
    }

    fn activate_native(&self) -> bool {
        let Some(session) = &self.native_session else {
            return false;
        };
        self.activated.store(true, Ordering::SeqCst);
        session.activate();
        true
    }

    async fn cancel_for_replacement(mut self) {
        self.preserve_active_status.store(true, Ordering::SeqCst);
        if let Some(session) = self.native_session.take() {
            let _ = session.cancel().await;
            if let Err(err) = self.task.await {
                warn!("cancelled Qwen turn task ended unexpectedly: {err:?}");
            }
        } else if !self.task.is_finished() {
            self.task.abort();
        } else if let Err(err) = self.task.await {
            warn!("turn task ended unexpectedly: {err:?}");
        }
    }

    async fn join(self) {
        if let Err(err) = self.task.await {
            warn!("turn task ended unexpectedly: {err:?}");
        }
    }
}

fn spawn_native_session(
    qwen: QwenVoiceService,
    idle_timeout: Duration,
    device: Device,
    device_config: crate::config::DeviceConfig,
    status: Arc<crate::web::status::RuntimeStatus>,
) -> anyhow::Result<ActiveTurn> {
    let session = qwen.prepare_session(idle_timeout)?;
    let native_session = session.handle();
    let activated = Arc::new(AtomicBool::new(false));
    let task_activated = activated.clone();
    let preserve_active_status = Arc::new(AtomicBool::new(false));
    let task_preserve_active_status = preserve_active_status.clone();
    Ok(ActiveTurn {
        task: tokio::spawn(async move {
            let result = session.run().await;
            cleanup_turn_leds(&device, &device_config).await;
            match result {
                Ok(()) => {
                    if task_activated.load(Ordering::SeqCst)
                        && !task_preserve_active_status.load(Ordering::SeqCst)
                    {
                        status.clear_last_error();
                    }
                }
                Err(error) => {
                    status.set_last_error(error.to_string());
                    error!(target: "xiaoai_agent::web_status", "native Qwen voice session failed");
                    error!("native Qwen voice session failed: {error:?}");
                }
            }
            if task_activated.load(Ordering::SeqCst) {
                info!(target: "xiaoai_agent::web_status", "voice turn finished");
            }
        }),
        native_session: Some(native_session),
        activated,
        preserve_active_status,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_buffer = crate::web::status::LogBuffer::new(200, 2048);
    let web_log_writer = log_buffer
        .clone()
        .with_filter(|metadata| metadata.target() == "xiaoai_agent::web_status");
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr.and(web_log_writer))
        .init();

    let cli = Cli::parse();
    let mut app_config = AppConfig::load(&cli.config)
        .with_context(|| format!("failed to load config {}", cli.config.display()))?;
    app_config.voice.qwen.speaker_instructions =
        Some(crate::agent::speaker_instructions(&app_config));
    let config = Arc::new(app_config);

    let device = Device::new(config.device.clone());
    let asr = AsrClient::new(config.asr.clone())?;
    let music = Arc::new(MusicService::new(config.clone(), device.clone())?);
    let airplay = AirPlayService::start(config.airplay.clone()).await?;
    let agent = Arc::new(AgentRuntime::new(config.clone(), device.clone(), music.clone()).await?);
    let qwen_voice = build_qwen_voice_service(
        config.voice.runtime,
        config.voice.qwen.clone(),
        config.capture.clone(),
        agent.tool_server(),
        {
            let agent = agent.clone();
            Arc::new(move || crate::qwen_voice::NativeMcpSnapshot {
                generation: agent.native_mcp_generation(),
                client: agent.native_mcp_client(),
            })
        },
    );
    if let Some(qwen) = &qwen_voice {
        qwen.preload_tools()
            .await
            .context("preload native Qwen tool definitions")?;
    }
    if config.voice.runtime == VoiceRuntime::Hermes && config.agent.thinking_sound.enabled {
        if let Err(err) = write_thinking_sound_wav(std::path::Path::new(THINKING_SOUND_WAV_PATH)) {
            warn!("failed to prepare thinking sound WAV: {err:?}");
        }
    }

    let (kws_tx, mut kws_rx) = mpsc::channel::<KwsMonitorEvent>(16);
    let restart_required = Arc::new(AtomicBool::new(false));
    let status = Arc::new(crate::web::status::RuntimeStatus::new(
        config.clone(),
        log_buffer,
        restart_required.clone(),
    ));
    let store = Arc::new(crate::web::config_store::ConfigStore::new(
        cli.config.clone(),
        restart_required,
    ));
    let restarter = Arc::new(crate::web::restart::ProcessRestarter::current()?);
    let listener = tokio::net::TcpListener::bind((cli.web_bind, cli.web_port))
        .await
        .with_context(|| format!("failed to bind web UI on {}:{}", cli.web_bind, cli.web_port))?;
    let web_address = listener
        .local_addr()
        .context("failed to inspect bound web UI address")?;
    info!(%web_address, "web UI listening");
    let web_state = crate::web::WebState {
        store,
        status: status.clone(),
        restarter,
    };
    let web_status = status.clone();
    let _web_task = tokio::spawn(async move {
        if let Err(error) = crate::web::serve(listener, web_state).await {
            web_status.set_last_error(error.to_string());
            error!(target: "xiaoai_agent::web_status", "web UI stopped");
            error!("web UI stopped: {error}");
        }
    });

    let mut kws = KwsMonitor::new();
    start_kws_monitor(&mut kws, config.runtime.clone(), kws_tx.clone()).await;

    info!("xiaoai-agent ready");
    device
        .blink_ready(config.device.led_listening, Duration::from_millis(250))
        .await;

    let mut active_turn: Option<ActiveTurn> = None;
    let native_idle_timeout =
        Duration::from_secs_f64(config.runtime.session_idle_timeout_s.max(1.0));
    let mut warm_spawned_at: Option<Instant> = None;
    let mut warm_native_turn = qwen_voice
        .clone()
        .map(|qwen| {
            spawn_native_session(
                qwen,
                native_idle_timeout,
                device.clone(),
                config.device.clone(),
                status.clone(),
            )
        })
        .transpose()?;
    if warm_native_turn.is_some() {
        warm_spawned_at = Some(Instant::now());
    }
    let mut turn_check = interval(Duration::from_millis(250));
    turn_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_warm_respawn: Option<Instant> = None;
    let mut last_mcp_reconnect: Option<Instant> = None;
    let mut last_mcp_generation = agent.native_mcp_generation();

    loop {
        tokio::select! {
            Some(event) = kws_rx.recv() => {
                match event {
                    KwsMonitorEvent::Started => {
                        status.set_kws_started(true);
                        info!(target: "xiaoai_agent::web_status", "KWS ready");
                        info!("KWS monitor started");
                    }
                    KwsMonitorEvent::Keyword(keyword) => {
                        info!("WAKE keyword={keyword}");
                        if active_turn
                            .as_ref()
                            .is_some_and(ActiveTurn::interrupt_native)
                        {
                            info!("wake keyword interrupted the active Qwen conversation");
                            continue;
                        }
                        if let Some(turn) = active_turn.take() {
                            turn.cancel_for_replacement().await;
                        }
                        let _ = AudioRecorder::instance().stop_recording().await;
                        let music_interrupted = music.interrupt_for_wake().await;
                        let airplay_interrupted = airplay.interrupt_for_wake().await;
                        if !music_interrupted && !airplay_interrupted {
                            device.abort_current_output().await;
                        }
                        cleanup_turn_leds(&device, &config.device).await;
                        agent.reset_session("wake keyword").await;

                        if let Some(qwen) = qwen_voice.clone() {
                            warm_spawned_at = None;
                            let turn = match warm_native_turn.take() {
                                Some(turn) if !turn.is_finished() => turn,
                                Some(turn) => {
                                    turn.join().await;
                                    match spawn_native_session(
                                        qwen,
                                        native_idle_timeout,
                                        device.clone(),
                                        config.device.clone(),
                                        status.clone(),
                                    ) {
                                        Ok(turn) => turn,
                                        Err(error) => {
                                            status.set_active_turn(false);
                                            status.set_last_error(error.to_string());
                                            error!("failed to prepare native Qwen voice session: {error:?}");
                                            continue;
                                        }
                                    }
                                }
                                None => match spawn_native_session(
                                    qwen,
                                    native_idle_timeout,
                                    device.clone(),
                                    config.device.clone(),
                                    status.clone(),
                                ) {
                                    Ok(turn) => turn,
                                    Err(error) => {
                                        status.set_active_turn(false);
                                        status.set_last_error(error.to_string());
                                        error!("failed to prepare native Qwen voice session: {error:?}");
                                        continue;
                                    }
                                },
                            };
                            turn.activate_native();
                            device.show_led(config.device.led_listening).await;
                            if let Some(text) =
                                choose_acknowledge_text(&config.runtime.acknowledge_text)
                            {
                                let ack_device = device.clone();
                                tokio::spawn(async move {
                                    if let Err(err) = ack_device.speak(&text).await {
                                        warn!("failed to speak acknowledge text: {err:?}");
                                    }
                                });
                            }
                            info!(target: "xiaoai_agent::web_status", "voice turn started");
                            status.set_active_turn(true);
                            active_turn = Some(turn);
                        } else {
                            let state = TurnState {
                                config: config.clone(),
                                device: device.clone(),
                                asr: asr.clone(),
                                agent: agent.clone(),
                                music: music.clone(),
                                airplay: airplay.clone(),
                            };
                            let turn_status = status.clone();
                            let preserve_active_status = Arc::new(AtomicBool::new(false));
                            let task_preserve_active_status = preserve_active_status.clone();
                            info!(target: "xiaoai_agent::web_status", "voice turn started");
                            status.set_active_turn(true);
                            active_turn = Some(ActiveTurn {
                                task: tokio::spawn(async move {
                                    match run_turn(state).await {
                                        Ok(()) => {
                                            if !task_preserve_active_status.load(Ordering::SeqCst) {
                                                turn_status.clear_last_error();
                                            }
                                        }
                                        Err(error) => {
                                            turn_status.set_last_error(error.to_string());
                                            error!(target: "xiaoai_agent::web_status", "legacy voice turn failed");
                                            error!("turn failed: {error:?}");
                                        }
                                    }
                                    if !task_preserve_active_status.load(Ordering::SeqCst) {
                                        turn_status.set_active_turn(false);
                                    }
                                    info!(target: "xiaoai_agent::web_status", "voice turn finished");
                                }),
                                native_session: None,
                                activated: Arc::new(AtomicBool::new(true)),
                                preserve_active_status,
                            });
                        }
                    }
                }
            }
            _ = turn_check.tick() => {
                let turn_finished = active_turn
                    .as_ref()
                    .map(|handle| handle.is_finished())
                    .unwrap_or(false);
                if turn_finished {
                    if let Some(handle) = active_turn.take() {
                        let was_native = handle.native_session.is_some();
                        handle.join().await;
                        if was_native {
                            if let Some(qwen) = qwen_voice.clone() {
                                warm_native_turn = spawn_native_session(
                                    qwen,
                                    native_idle_timeout,
                                    device.clone(),
                                    config.device.clone(),
                                    status.clone(),
                                ).ok();
                                if warm_native_turn.is_some() {
                                    warm_spawned_at = Some(Instant::now());
                                }
                            }
                        }
                    }
                    status.set_active_turn(false);
                    device
                        .blink_ready(config.device.led_listening, Duration::from_millis(250))
                        .await;
                }
                // A fail-closed native MCP client (poisoned by a timeout or a
                // cancelled in-flight call) is replaced while idle so smart
                // home tools recover without a manual restart.
                if active_turn.is_none()
                    && agent.native_mcp_needs_reconnect()
                    && last_mcp_reconnect
                        .is_none_or(|at| at.elapsed() >= MCP_RECONNECT_MIN_INTERVAL)
                {
                    last_mcp_reconnect = Some(Instant::now());
                    let agent = agent.clone();
                    tokio::spawn(async move {
                        agent.reconnect_native_mcp().await;
                    });
                }
                let mcp_generation = agent.native_mcp_generation();
                if mcp_generation != last_mcp_generation {
                    last_mcp_generation = mcp_generation;
                    // The prewarmed session resolved the old MCP client when it
                    // was spawned; rotate it so the next wake uses the fresh one.
                    if let Some(turn) = warm_native_turn.take() {
                        warm_spawned_at = None;
                        info!("rotating warm native Qwen session after MCP reconnect");
                        turn.cancel_for_replacement().await;
                    }
                }
                // Retire the warm session before Qwen's ~300 s idle timeout can
                // kill it (see WARM_SESSION_MAX_AGE).
                let warm_too_old = warm_native_turn.is_some()
                    && warm_spawned_at.is_some_and(|at| at.elapsed() >= WARM_SESSION_MAX_AGE);
                if warm_too_old {
                    if let Some(turn) = warm_native_turn.take() {
                        warm_spawned_at = None;
                        info!("rotating warm native Qwen session before server idle timeout");
                        turn.cancel_for_replacement().await;
                    }
                }
                let warm_finished = warm_native_turn
                    .as_ref()
                    .is_some_and(ActiveTurn::is_finished);
                if warm_finished {
                    warm_spawned_at = None;
                    if let Some(turn) = warm_native_turn.take() {
                        turn.join().await;
                    }
                }
                // Qwen closes idle prewarmed sessions after ~300 s and the
                // reconnect budget inside a session is bounded, so a warm
                // session eventually dies for good. Replace it promptly or
                // the next wake pays the cold-start latency or hits ICE
                // failures with no session ready.
                if warm_native_turn.is_none() && active_turn.is_none() {
                    if let Some(qwen) = qwen_voice.clone() {
                        if last_warm_respawn
                            .is_none_or(|at| at.elapsed() >= WARM_RESPAWN_MIN_INTERVAL)
                        {
                            last_warm_respawn = Some(Instant::now());
                            match spawn_native_session(
                                qwen,
                                native_idle_timeout,
                                device.clone(),
                                config.device.clone(),
                                status.clone(),
                            ) {
                                Ok(turn) => {
                                    info!("respawned warm native Qwen session");
                                    warm_spawned_at = Some(Instant::now());
                                    warm_native_turn = Some(turn);
                                }
                                Err(error) => {
                                    warn!("failed to respawn warm native Qwen session: {error:?}");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn start_kws_monitor(
    kws: &mut KwsMonitor,
    config: crate::config::RuntimeConfig,
    kws_tx: mpsc::Sender<KwsMonitorEvent>,
) {
    info!(pcm = %config.kws_pcm, "starting native VPM/FlexKWS monitor");
    kws.start(config, move |event| {
        let tx = kws_tx.clone();
        async move {
            tx.send(event).await.map_err(|err| err.to_string())?;
            Ok(())
        }
    })
    .await;
}

#[derive(Clone)]
struct TurnState {
    config: Arc<AppConfig>,
    device: Device,
    asr: AsrClient,
    agent: Arc<AgentRuntime>,
    music: Arc<MusicService>,
    airplay: AirPlayService,
}

async fn run_turn(state: TurnState) -> anyhow::Result<()> {
    let result = run_session(state.clone()).await;
    cleanup_turn_leds(&state.device, &state.config.device).await;
    state.music.restore_after_interruption().await;
    state.airplay.restore_after_interruption().await;
    result
}

async fn run_session(state: TurnState) -> anyhow::Result<()> {
    let led = &state.config.device;
    let mut is_first_turn = true;

    loop {
        state.device.show_led(led.led_listening).await;
        if is_first_turn {
            if let Some(text) = choose_acknowledge_text(&state.config.runtime.acknowledge_text) {
                let device = state.device.clone();
                tokio::spawn(async move {
                    if let Err(err) = device.speak(&text).await {
                        warn!("failed to speak acknowledge text: {err:?}");
                    }
                });
            }
            is_first_turn = false;
        }

        let device_for_speech = state.device.clone();
        let led_user_speaking = led.led_user_speaking;
        let idle_timeout =
            Duration::from_secs_f64(state.config.runtime.session_idle_timeout_s.max(1.0));
        let maybe_stream = match state
            .asr
            .start_streaming_transcription(state.config.capture.sample_rate)
            .await
        {
            Ok(stream) => stream,
            Err(err) => {
                speak_service_error(&state.device, led, ASR_SERVICE_ERROR_PROMPT).await;
                return Err(err.context("ASR failed after retries"));
            }
        };
        let text = if let Some(stream) = maybe_stream {
            let appender = stream.appender();
            let appender_for_chunk = appender.clone();
            let appender_for_reject = appender.clone();
            let _pcm = match record_utterance_streaming(
                state.config.capture.clone(),
                idle_timeout,
                move || {
                    let device = device_for_speech.clone();
                    async move {
                        device.show_led(led_user_speaking).await;
                    }
                },
                move |bytes| {
                    let appender = appender_for_chunk.clone();
                    async move { appender.append_pcm(bytes).await }
                },
                move || {
                    let appender = appender_for_reject.clone();
                    async move { appender.clear().await }
                },
            )
            .await
            {
                Ok(pcm) => pcm,
                Err(err) if is_capture_timeout(&err) => {
                    stream.close().await;
                    info!("session idle timeout");
                    state.agent.reset_session("session idle timeout").await;
                    return Ok(());
                }
                Err(err) => {
                    stream.close().await;
                    return Err(err);
                }
            };

            state.device.show_led(led.led_thinking).await;
            match stream.commit_and_transcribe().await {
                Ok(text) => text,
                Err(err) => {
                    speak_service_error(&state.device, led, ASR_SERVICE_ERROR_PROMPT).await;
                    return Err(err.context("ASR failed after retries"));
                }
            }
        } else {
            let pcm =
                match record_utterance(state.config.capture.clone(), idle_timeout, move || {
                    let device = device_for_speech.clone();
                    async move {
                        device.show_led(led_user_speaking).await;
                    }
                })
                .await
                {
                    Ok(pcm) => pcm,
                    Err(err) if is_capture_timeout(&err) => {
                        info!("session idle timeout");
                        state.agent.reset_session("session idle timeout").await;
                        return Ok(());
                    }
                    Err(err) => return Err(err),
                };

            state.device.show_led(led.led_thinking).await;
            match state
                .asr
                .transcribe_pcm(&pcm, state.config.capture.sample_rate)
                .await
            {
                Ok(text) => text,
                Err(err) => {
                    speak_service_error(&state.device, led, ASR_SERVICE_ERROR_PROMPT).await;
                    return Err(err.context("ASR failed after retries"));
                }
            }
        };
        let command = text.trim();
        if command.is_empty() {
            info!("empty ASR result; ending session");
            state.agent.reset_session("empty ASR result").await;
            return Ok(());
        }
        info!("USER_ASR text={command}");

        let reply = {
            let _thinking_sound = ThinkingSoundGuard(start_thinking_sound(&state));
            state.agent.run_turn(command).await
        };
        let reply = match reply {
            Ok(reply) => reply,
            Err(err) => {
                speak_service_error(&state.device, led, LLM_SERVICE_ERROR_PROMPT).await;
                return Err(err.context("LLM failed after retries"));
            }
        };
        state.device.shut_led(led.led_thinking).await;
        if reply.text.trim().is_empty() {
            continue;
        }
        state.device.show_led(led.led_speaking).await;
        state.device.speak(&reply.text).await?;
        if reply.should_end {
            info!("agent ended conversation: {}", reply.end_reason);
            state.agent.reset_session("agent ended conversation").await;
            return Ok(());
        }
    }
}

async fn speak_service_error(device: &Device, led: &DeviceConfig, text: &str) {
    device.shut_led(led.led_thinking).await;
    device.show_led(led.led_speaking).await;
    if let Err(err) = device.speak(text).await {
        warn!("failed to speak service error prompt: {err:?}");
    }
}

async fn cleanup_turn_leds(device: &Device, led: &DeviceConfig) {
    for id in [
        led.led_speaking,
        led.led_thinking,
        led.led_user_speaking,
        led.led_listening,
    ] {
        device.shut_led(id).await;
    }
}

/// Aborts the progress-sound ticker on every exit path (reply, error, or
/// session end); dropping a bare JoinHandle would detach it instead.
struct ThinkingSoundGuard(Option<JoinHandle<()>>);

impl Drop for ThinkingSoundGuard {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// Periodic progress tone while a slow remote brain is still working, so long
/// Hermes tool loops do not leave dead air. Only runs in hermes mode.
fn start_thinking_sound(state: &TurnState) -> Option<JoinHandle<()>> {
    let cfg = &state.config.agent.thinking_sound;
    if state.config.voice.runtime != VoiceRuntime::Hermes || !cfg.enabled {
        return None;
    }
    let delay = Duration::from_secs_f64(cfg.delay_s.max(1.0));
    let interval = Duration::from_secs_f64(cfg.interval_s.max(1.0));
    let device = state.device.clone();
    Some(tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        loop {
            if let Err(err) = device.play_thinking_sound().await {
                debug!("thinking sound playback failed: {err:?}");
            }
            tokio::time::sleep(interval).await;
        }
    }))
}

/// Writes the short two-note progress tone used by the thinking sound.
/// 16 kHz mono S16_LE with raised-cosine edges, matching the firmware path.
fn write_thinking_sound_wav(path: &std::path::Path) -> anyhow::Result<()> {
    const RATE: u32 = 16_000;
    const AMPLITUDE: f32 = 0.22;
    let mut pcm: Vec<i16> = Vec::new();
    for (freq, ms) in [(660.0_f32, 150_u32), (880.0_f32, 200_u32)] {
        let n = (RATE as u64 * ms as u64 / 1000) as usize;
        let fade = RATE as usize / 1000 * 30; // 30 ms edges avoid clicks
        for i in 0..n {
            let t = i as f32 / RATE as f32;
            let edge = if i < fade {
                i as f32 / fade as f32
            } else if i + fade > n {
                (n - i) as f32 / fade as f32
            } else {
                1.0
            };
            let envelope = 0.5 - 0.5 * (std::f32::consts::PI * edge).cos();
            pcm.push(
                (AMPLITUDE * envelope * (2.0 * std::f32::consts::PI * freq * t).sin()
                    * i16::MAX as f32) as i16,
            );
        }
    }
    let data_len = (pcm.len() * 2) as u32;
    let mut wav = Vec::with_capacity(44 + data_len as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_len).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes()); // PCM
    wav.extend_from_slice(&1_u16.to_le_bytes()); // mono
    wav.extend_from_slice(&RATE.to_le_bytes());
    wav.extend_from_slice(&(RATE * 2).to_le_bytes()); // byte rate
    wav.extend_from_slice(&2_u16.to_le_bytes()); // block align
    wav.extend_from_slice(&16_u16.to_le_bytes()); // bits per sample
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());
    for sample in pcm {
        wav.extend_from_slice(&sample.to_le_bytes());
    }
    std::fs::write(path, wav)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn is_capture_timeout(err: &anyhow::Error) -> bool {
    err.to_string()
        .contains("timed out waiting for user speech")
}

fn choose_acknowledge_text(options: &[String]) -> Option<String> {
    let choices = options
        .iter()
        .map(|text| text.trim())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>();
    choices
        .choose(&mut rand::thread_rng())
        .map(|text| (*text).to_string())
}

fn uses_native_qwen(runtime: VoiceRuntime) -> bool {
    matches!(runtime, VoiceRuntime::NativeQwen)
}

fn build_qwen_voice_service(
    runtime: VoiceRuntime,
    config: QwenRealtimeConfig,
    capture: CaptureConfig,
    tool_server: rig_core::tool::server::ToolServerHandle,
    native_mcp: crate::qwen_voice::NativeMcpProvider,
) -> Option<QwenVoiceService> {
    uses_native_qwen(runtime).then(|| {
        QwenVoiceService::new(config, capture, tool_server).with_native_mcp_provider(native_mcp)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn web_cli_defaults_to_lan_port_8080() {
        let cli = Cli::try_parse_from(["xiaoai-agent"]).unwrap();
        assert_eq!(cli.web_bind, "0.0.0.0".parse::<std::net::IpAddr>().unwrap());
        assert_eq!(cli.web_port, 8080);
    }

    #[test]
    fn web_cli_accepts_loopback_and_custom_port() {
        let cli = Cli::try_parse_from([
            "xiaoai-agent",
            "--web-bind",
            "127.0.0.1",
            "--web-port",
            "18080",
        ])
        .unwrap();
        assert_eq!(
            cli.web_bind,
            "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
        );
        assert_eq!(cli.web_port, 18080);
    }

    #[test]
    fn voice_runtime_routes_legacy_and_native_behavior_separately() {
        let config = QwenRealtimeConfig::default();
        let capture = CaptureConfig::default();
        let tools = rig_core::tool::server::ToolServer::new().run();
        assert!(build_qwen_voice_service(
            VoiceRuntime::Legacy,
            config.clone(),
            capture.clone(),
            tools.clone(),
            Arc::new(|| crate::qwen_voice::NativeMcpSnapshot {
                generation: 0,
                client: None,
            }),
        )
        .is_none());
        assert!(build_qwen_voice_service(
            VoiceRuntime::Hermes,
            config.clone(),
            capture.clone(),
            tools.clone(),
            Arc::new(|| crate::qwen_voice::NativeMcpSnapshot {
                generation: 0,
                client: None,
            }),
        )
        .is_none());
        assert!(build_qwen_voice_service(
            VoiceRuntime::NativeQwen,
            config,
            capture,
            tools,
            Arc::new(|| crate::qwen_voice::NativeMcpSnapshot {
                generation: 0,
                client: None,
            })
        )
        .is_some());
    }

    #[test]
    fn thinking_sound_wav_has_valid_header_and_expected_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("thinking.wav");
        write_thinking_sound_wav(&path).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[0..4], b"RIFF");
        assert_eq!(&bytes[8..12], b"WAVE");
        // 0.35 s of 16 kHz mono S16_LE audio.
        let data_len = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
        assert_eq!(data_len, 16_000 * 2 * 350 / 1000);
        assert_eq!(bytes.len(), 44 + data_len);
    }
}
