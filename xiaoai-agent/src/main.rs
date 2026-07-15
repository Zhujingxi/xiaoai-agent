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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, warn};
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

#[derive(Debug, Parser)]
#[command(name = "xiaoai-agent")]
#[command(about = "Standalone XiaoAI on-device agent: flexkws + cloud ASR + Rig agent")]
struct Cli {
    #[arg(short, long, default_value = "/data/open-xiaoai/agent.yaml")]
    config: PathBuf,
}

struct ActiveTurn {
    task: JoinHandle<()>,
    native_session: Option<SessionHandle>,
}

impl ActiveTurn {
    fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    async fn cancel(mut self) {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cli = Cli::parse();
    let config = Arc::new(
        AppConfig::load(&cli.config)
            .with_context(|| format!("failed to load config {}", cli.config.display()))?,
    );

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
        agent.native_mcp_client(),
    );

    let (kws_tx, mut kws_rx) = mpsc::channel::<KwsMonitorEvent>(16);
    let mut kws = KwsMonitor::new();
    start_kws_monitor(&mut kws, config.runtime.clone(), kws_tx.clone()).await;

    info!("xiaoai-agent ready");
    device
        .blink_ready(config.device.led_listening, Duration::from_millis(250))
        .await;

    let mut active_turn: Option<ActiveTurn> = None;
    let mut turn_check = interval(Duration::from_millis(250));
    turn_check.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            Some(event) = kws_rx.recv() => {
                match event {
                    KwsMonitorEvent::Started => info!("KWS monitor started"),
                    KwsMonitorEvent::Keyword(keyword) => {
                        info!("WAKE keyword={keyword}");
                        if let Some(turn) = active_turn.take() {
                            turn.cancel().await;
                        }
                        let _ = AudioRecorder::instance().stop_recording().await;
                        let music_interrupted = music.interrupt_for_wake().await;
                        let airplay_interrupted = airplay.interrupt_for_wake().await;
                        if !music_interrupted && !airplay_interrupted {
                            device.abort_current_output().await;
                        }
                        cleanup_turn_leds(&device, &config.device).await;
                        agent.reset_session("wake keyword").await;

                        active_turn = if let Some(qwen) = qwen_voice.clone() {
                            let device = device.clone();
                            let device_config = config.device.clone();
                            let idle_timeout = Duration::from_secs_f64(
                                config.runtime.session_idle_timeout_s.max(1.0),
                            );
                            let session = qwen.prepare_session(idle_timeout)?;
                            let native_session = session.handle();
                            Some(ActiveTurn {
                                task: tokio::spawn(async move {
                                    if let Err(err) = session.run().await {
                                        error!("native Qwen voice session failed: {err:?}");
                                    }
                                    cleanup_turn_leds(&device, &device_config).await;
                                }),
                                native_session: Some(native_session),
                            })
                        } else {
                            let state = TurnState {
                                config: config.clone(),
                                device: device.clone(),
                                asr: asr.clone(),
                                agent: agent.clone(),
                                music: music.clone(),
                                airplay: airplay.clone(),
                            };
                            Some(ActiveTurn {
                                task: tokio::spawn(async move {
                                    if let Err(err) = run_turn(state).await {
                                        error!("turn failed: {err:?}");
                                    }
                                }),
                                native_session: None,
                            })
                        };
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
                        handle.join().await;
                    }
                    device
                        .blink_ready(config.device.led_listening, Duration::from_millis(250))
                        .await;
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

        let reply = match state.agent.run_turn(command).await {
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
    native_mcp: Option<crate::mcp::NativeMcpClient>,
) -> Option<QwenVoiceService> {
    uses_native_qwen(runtime)
        .then(|| QwenVoiceService::new(config, capture, tool_server).with_native_mcp(native_mcp))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            None,
        )
        .is_none());
        assert!(
            build_qwen_voice_service(VoiceRuntime::NativeQwen, config, capture, tools, None)
                .is_some()
        );
    }
}
