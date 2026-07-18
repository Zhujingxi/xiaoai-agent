use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use opus::{Application, Channels, Decoder, Encoder};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, sleep_until, timeout, Instant, MissedTickBehavior};
use tracing::{debug, warn};
use url::Url;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_OPUS};
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice::mdns::MulticastDnsMode;
use webrtc::ice::network_type::NetworkType;
use webrtc::interceptor::registry::Registry;
use webrtc::media::Sample;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTPCodecType};
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc::track::track_local::TrackLocal;

use super::*;

const OPUS_FRAME_MS: u64 = 20;
const OPUS_SAMPLE_RATE: u32 = 48_000;
const OPUS_INPUT_SAMPLES: usize = 960;
const OPUS_UPSAMPLE_FACTOR: i32 = 3;
const MAX_OPUS_BUFFERED_SAMPLES: usize = 9_600;
const PLAYBACK_BLOCK_SAMPLES: usize = 240;
const OPUS_MAX_PACKET_BYTES: usize = 1_275;
const OPUS_MAX_DECODE_SAMPLES_PER_CHANNEL: usize = 5_760;
const WEBRTC_AUDIO_QUEUE_CAPACITY: usize = 64;
const WEBRTC_AUDIO_QUIET_PERIOD: Duration = Duration::from_millis(400);
const WEBRTC_TASK_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
// After the tool budget is exhausted the model is told to answer directly. A
// model that keeps requesting tools anyway gets this many extra refusals
// before the session hard-fails as a runaway loop.
const TOOL_BUDGET_EXHAUSTED_GRACE: usize = 2;
const TOOL_BUDGET_EXHAUSTED_MESSAGE: &str =
    "工具调用次数已达本轮上限，禁止继续调用工具，请立即根据已有信息直接回答用户";

fn tool_budget_exhausted(
    tools: &NativeToolRuntime,
    tool_iterations: usize,
    tool_calls: usize,
    requested_calls: usize,
) -> bool {
    tool_iterations >= tools.max_iterations || tool_calls + requested_calls > tools.max_calls
}

enum TransportEvent {
    Data(String),
    Connection(RTCPeerConnectionState),
    AudioError(String),
}

struct WebRtcConnection {
    peer: Arc<RTCPeerConnection>,
    audio_track: Arc<TrackLocalStaticSample>,
    data_channel_rx: mpsc::UnboundedReceiver<Arc<RTCDataChannel>>,
    event_rx: mpsc::UnboundedReceiver<TransportEvent>,
    audio_tasks_rx: mpsc::UnboundedReceiver<JoinHandle<anyhow::Result<()>>>,
    audio_activity_rx: watch::Receiver<u64>,
    rtcp_task: JoinHandle<()>,
}

struct AttemptResources {
    player: PcmPlayer,
    peer: Arc<RTCPeerConnection>,
    writer_cancel_tx: Option<watch::Sender<bool>>,
    writer_task: Option<JoinHandle<anyhow::Result<()>>>,
    capture_task: Option<AbortOnDropTask<anyhow::Result<()>>>,
    audio_tasks_rx: mpsc::UnboundedReceiver<JoinHandle<anyhow::Result<()>>>,
    rtcp_task: JoinHandle<()>,
    clean_finish: bool,
}

impl AttemptResources {
    async fn teardown(mut self) -> anyhow::Result<()> {
        if let Some(writer_cancel_tx) = &self.writer_cancel_tx {
            let _ = writer_cancel_tx.send(true);
        }
        let capture_result = match self.capture_task.as_mut() {
            Some(capture_task) => {
                capture_task.abort();
                match timeout(CAPTURE_SHUTDOWN_TIMEOUT, capture_task.join()).await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(error)) if error.is_cancelled() => Ok(()),
                    Ok(Err(error)) => {
                        Err(anyhow::Error::new(error).context("join WebRTC capture task"))
                    }
                    Err(_) => Err(anyhow::anyhow!("timed out stopping WebRTC capture task")),
                }
            }
            None => Ok(()),
        };

        let peer_result = match timeout(WEBSOCKET_CLOSE_TIMEOUT, self.peer.close()).await {
            Ok(result) => result.context("close Qwen WebRTC peer"),
            Err(_) => Err(anyhow::anyhow!("timed out closing Qwen WebRTC peer")),
        };

        let writer_result = match self.writer_task.as_mut() {
            Some(writer_task) => {
                match timeout(WEBRTC_TASK_CLEANUP_TIMEOUT, &mut *writer_task).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => {
                        Err(anyhow::Error::new(error).context("WebRTC Opus writer task panicked"))
                    }
                    Err(_) => {
                        writer_task.abort();
                        let _ = writer_task.await;
                        Err(anyhow::anyhow!("timed out stopping WebRTC Opus writer"))
                    }
                }
            }
            None => Ok(()),
        };

        self.rtcp_task.abort();
        let _ = self.rtcp_task.await;
        while let Ok(mut task) = self.audio_tasks_rx.try_recv() {
            task.abort();
            let _ = timeout(WEBRTC_TASK_CLEANUP_TIMEOUT, &mut task).await;
        }

        let player_result = if self.clean_finish {
            self.player.finish().await
        } else {
            self.player.shutdown().await
        };

        capture_result
            .and(peer_result)
            .and(writer_result)
            .and(player_result)
    }
}

async fn teardown_after<T>(
    result: anyhow::Result<T>,
    resources: AttemptResources,
) -> anyhow::Result<T> {
    let teardown = resources.teardown().await;
    match result {
        Ok(value) => {
            teardown?;
            Ok(value)
        }
        Err(error) => {
            if let Err(teardown_error) = teardown {
                return Err(
                    error.context(format!("WebRTC teardown also failed: {teardown_error:#}"))
                );
            }
            Err(error)
        }
    }
}

pub(super) async fn run(
    config: QwenRealtimeConfig,
    capture: CaptureConfig,
    tools: NativeToolRuntime,
    idle_timeout: Duration,
    control: &mut RealtimeSessionControl,
) -> anyhow::Result<()> {
    let mut machine = SessionMachine::new();
    machine.transition(SessionState::Connecting)?;
    let mut last_error = None;

    for attempt in 0..=RECONNECT_ATTEMPTS {
        if *control.cancel_rx.borrow() {
            machine.transition(SessionState::ShuttingDown)?;
            machine.transition(SessionState::Closed)?;
            return Ok(());
        }

        match run_once(
            &config,
            &capture,
            &tools,
            idle_timeout,
            control,
            &mut machine,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(error) => {
                warn!(attempt, "Qwen WebRTC session failed: {error:#}");
                if !transparent_reconnect_allowed(&tools) {
                    machine.transition(SessionState::Failed).ok();
                    return Err(error.context(
                        "transparent WebRTC reconnect disabled after a native tool request started",
                    ));
                }
                last_error = Some(error);
                if matches!(
                    machine.state(),
                    SessionState::ShuttingDown | SessionState::Closed
                ) {
                    break;
                }
                machine.transition(SessionState::Reconnecting).ok();
                let backoff = Duration::from_secs(1 << attempt.min(2));
                tokio::select! {
                    _ = sleep(backoff) => {}
                    changed = control.cancel_rx.changed() => {
                        if changed.is_err() || *control.cancel_rx.borrow() {
                            machine.transition(SessionState::ShuttingDown)?;
                            machine.transition(SessionState::Closed)?;
                            return Ok(());
                        }
                    }
                }
                machine.transition(SessionState::Connecting)?;
            }
        }
    }

    machine.transition(SessionState::Failed).ok();
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Qwen WebRTC session failed")))
}

async fn run_once(
    config: &QwenRealtimeConfig,
    capture: &CaptureConfig,
    tools: &NativeToolRuntime,
    idle_timeout: Duration,
    control: &mut RealtimeSessionControl,
    machine: &mut SessionMachine,
) -> anyhow::Result<()> {
    validate_pcm_contract(config, capture)?;
    let connect_started_at = Instant::now();
    let player = setup_if_running(&control.setup_phase, &control.cancel_rx, || {
        PcmPlayer::spawn(
            "/usr/bin/aplay".to_string(),
            "default".to_string(),
            config.output_sample_rate.0,
            control.setup_phase.clone(),
        )
    })?
    .context("Qwen WebRTC setup cancelled")?;
    let player_handle = player.handle();

    let connection =
        match connect_webrtc(config, player_handle.clone(), &mut control.cancel_rx).await {
            Ok(connection) => connection,
            Err(error) => {
                player.shutdown().await.ok();
                return Err(error);
            }
        };
    tracing::info!(
        target: "xiaoai_agent::qwen_latency",
        webrtc_connect_ms = connect_started_at.elapsed().as_millis() as u64,
        "QWEN_LATENCY webrtc_connected"
    );

    let WebRtcConnection {
        peer,
        audio_track,
        mut data_channel_rx,
        mut event_rx,
        audio_tasks_rx,
        mut audio_activity_rx,
        rtcp_task,
    } = connection;

    let mut resources = AttemptResources {
        player,
        peer,
        writer_cancel_tx: None,
        writer_task: None,
        capture_task: None,
        audio_tasks_rx,
        rtcp_task,
        clean_finish: false,
    };

    let event_timeout = timeout_duration(config.event_timeout_s);
    let setup_result = async {
        let txt = wait_for_txt_channel(&mut data_channel_rx, event_timeout, &mut control.cancel_rx)
            .await?;
        wait_for_first_server_event(&mut event_rx, event_timeout, &mut control.cancel_rx).await?;
        tracing::info!(
            tool_names = ?tools.definitions.iter().map(|definition| match definition {
                crate::qwen_realtime::ToolDefinition::Function { function } => function.name.as_str(),
            }).collect::<Vec<_>>(),
            "sending Qwen WebRTC session.update"
        );
        send_data_event(
            &txt,
            &ClientEvent::SessionUpdate {
                event_id: None,
                session: SessionUpdate {
                    modalities: vec![Modality::Text, Modality::Audio],
                    voice: config.voice.clone(),
                    input_audio_format: AudioFormat::Pcm,
                    output_audio_format: AudioFormat::Pcm,
                    turn_detection: Some(conversational_turn_detection(config)),
                    instructions: Some(
                        config
                            .speaker_instructions
                            .clone()
                            .unwrap_or_else(|| SPEAKER_AGENT_INSTRUCTIONS.to_string()),
                    ),
                    tools: tools.definitions.clone(),
                },
            },
            event_timeout,
            &mut control.cancel_rx,
        )
        .await?;
        wait_for_session_updated(&mut event_rx, event_timeout, &mut control.cancel_rx).await?;
        tracing::info!(
            target: "xiaoai_agent::qwen_latency",
            session_ready_ms = connect_started_at.elapsed().as_millis() as u64,
            "QWEN_LATENCY session_ready"
        );
        Ok::<_, anyhow::Error>(txt)
    }
    .await;
    let txt = match setup_result {
        Ok(txt) => txt,
        Err(error) => return teardown_after(Err(error), resources).await,
    };
    if let Err(error) = machine.transition(SessionState::Ready) {
        return teardown_after(Err(error.into()), resources).await;
    }

    let (pcm_tx, pcm_rx) = mpsc::channel(WEBRTC_AUDIO_QUEUE_CAPACITY);
    let (writer_cancel_tx, writer_cancel_rx) = watch::channel(false);
    resources.writer_cancel_tx = Some(writer_cancel_tx);
    resources.writer_task = Some(tokio::spawn(run_opus_writer(
        audio_track,
        pcm_rx,
        writer_cancel_rx,
    )));
    if !wait_for_activation(
        &mut event_rx,
        &mut control.activation_rx,
        &mut control.cancel_rx,
    )
    .await?
    {
        machine.transition(SessionState::Cancelling)?;
        machine.transition(SessionState::ShuttingDown)?;
        return teardown_after(Ok(()), resources).await;
    }
    tracing::info!(
        target: "xiaoai_agent::qwen_latency",
        "QWEN_LATENCY microphone_activated"
    );
    let upload = BoundedPcmSender { tx: pcm_tx };
    let capture_config = capture.clone();
    resources.capture_task = Some(AbortOnDropTask::new(tokio::spawn(async move {
        stream_audio_continuously(capture_config, move |bytes| {
            let upload = upload.clone();
            async move { upload.send(bytes) }
        })
        .await
    })));
    machine.transition(SessionState::Capturing)?;

    let mut capture_joined = false;
    let session_result = run_event_loop(EventLoopContext {
        channel: &txt,
        event_rx: &mut event_rx,
        audio_activity_rx: &mut audio_activity_rx,
        capture_task: resources
            .capture_task
            .as_mut()
            .context("WebRTC capture task missing")?,
        capture_joined: &mut capture_joined,
        player: &player_handle,
        tools,
        event_timeout,
        idle_timeout,
        control,
        machine,
    })
    .await;
    if capture_joined {
        // run_event_loop already consumed the JoinHandle output. Remove it so
        // teardown cannot poll the completed JoinHandle a second time.
        resources.capture_task.take();
    }
    resources.clean_finish = session_result.is_ok() && !*control.cancel_rx.borrow();
    let teardown_result = resources.teardown().await;
    tools.wait_for_tool_idle().await?;

    if matches!(machine.state(), SessionState::Cancelling) {
        machine.transition(SessionState::ShuttingDown).ok();
    }
    if matches!(machine.state(), SessionState::ShuttingDown) && teardown_result.is_ok() {
        machine.transition(SessionState::Closed)?;
    }
    session_result.and(teardown_result)
}

async fn wait_for_activation(
    event_rx: &mut mpsc::UnboundedReceiver<TransportEvent>,
    activation_rx: &mut watch::Receiver<bool>,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<bool> {
    if *activation_rx.borrow() {
        return Ok(true);
    }
    loop {
        tokio::select! {
            biased;
            changed = cancel_rx.changed() => {
                if changed.is_err() || *cancel_rx.borrow() {
                    return Ok(false);
                }
            }
            changed = activation_rx.changed() => {
                if changed.is_err() {
                    anyhow::bail!("Qwen activation channel closed");
                }
                if *activation_rx.borrow() {
                    return Ok(true);
                }
            }
            transport = event_rx.recv() => {
                match transport.context("Qwen WebRTC event channel closed while prewarmed")? {
                    TransportEvent::Connection(RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) => {
                        anyhow::bail!("Qwen WebRTC connection closed while prewarmed")
                    }
                    TransportEvent::Connection(_) => {}
                    TransportEvent::AudioError(error) => anyhow::bail!(error),
                    TransportEvent::Data(text) => match serde_json::from_str::<ServerEvent>(&text)
                        .context("decode Qwen prewarm event")?
                    {
                        ServerEvent::Error(error) => {
                            anyhow::bail!("Qwen realtime error while prewarmed: {}", error.error.message)
                        }
                        event => debug!(?event, "received Qwen event while waiting for wake activation"),
                    },
                }
            }
        }
    }
}

#[derive(Clone)]
struct BoundedPcmSender {
    tx: mpsc::Sender<Vec<u8>>,
}

impl BoundedPcmSender {
    fn send(&self, bytes: Vec<u8>) -> anyhow::Result<()> {
        match self.tx.try_send(bytes) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("WebRTC PCM queue full; dropping newest VPM chunk");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("WebRTC PCM writer stopped")
            }
        }
    }
}

async fn connect_webrtc(
    config: &QwenRealtimeConfig,
    player: PcmPlayerHandle,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<WebRtcConnection> {
    install_rustls_crypto_provider();
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;
    let registry = register_default_interceptors(Registry::new(), &mut media_engine)?;
    let mut setting_engine = SettingEngine::default();
    // The OH2P firmware exposes an IPv6 link-local address, but its 4.9 kernel
    // cannot bind the unscoped address returned by interface enumeration. The
    // failed IPv6 bind closes the whole ICE gatherer before its usable IPv4
    // candidate is emitted. Qwen's WebRTC endpoint works over IPv4 UDP, so keep
    // candidate gathering on the device's supported path and avoid an mDNS
    // listener that is unnecessary for server-mediated signaling.
    setting_engine.set_network_types(vec![NetworkType::Udp4]);
    setting_engine.set_ice_multicast_dns_mode(MulticastDnsMode::Disabled);
    let api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .with_setting_engine(setting_engine)
        .build();
    let peer = Arc::new(
        api.new_peer_connection(RTCConfiguration::default())
            .await
            .context("create Qwen WebRTC peer")?,
    );

    let audio_track = Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_string(),
            clock_rate: 48_000,
            channels: 2,
            sdp_fmtp_line: "minptime=10;useinbandfec=1".to_string(),
            ..Default::default()
        },
        "audio".to_string(),
        "xiaoai-agent".to_string(),
    ));
    let sender = match peer
        .add_track(Arc::clone(&audio_track) as Arc<dyn TrackLocal + Send + Sync>)
        .await
        .context("add Qwen WebRTC audio track")
    {
        Ok(sender) => sender,
        Err(error) => {
            let _ = timeout(WEBSOCKET_CLOSE_TIMEOUT, peer.close()).await;
            return Err(error);
        }
    };
    let mut rtcp_task = Some(tokio::spawn(async move {
        while sender.read_rtcp().await.is_ok() {}
    }));

    let setup_result = async {
        peer.create_data_channel("oai-events", None)
            .await
            .context("create Qwen WebRTC bootstrap DataChannel")?;

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (data_channel_tx, data_channel_rx) = mpsc::unbounded_channel();
        let (audio_tasks_tx, audio_tasks_rx) = mpsc::unbounded_channel();
        let (audio_activity_tx, audio_activity_rx) = watch::channel(0u64);

        let dc_event_tx = event_tx.clone();
        peer.on_data_channel(Box::new(move |channel: Arc<RTCDataChannel>| {
            let data_channel_tx = data_channel_tx.clone();
            let event_tx = dc_event_tx.clone();
            Box::pin(async move {
                if channel.label() != "txt" {
                    return;
                }
                let message_tx = event_tx.clone();
                channel.on_message(Box::new(move |message: DataChannelMessage| {
                    let message_tx = message_tx.clone();
                    Box::pin(async move {
                        match String::from_utf8(message.data.to_vec()) {
                            Ok(text) => {
                                let _ = message_tx.send(TransportEvent::Data(text));
                            }
                            Err(error) => {
                                let _ = message_tx.send(TransportEvent::AudioError(format!(
                                    "Qwen DataChannel sent invalid UTF-8: {error}"
                                )));
                            }
                        }
                    })
                }));
                let _ = data_channel_tx.send(channel);
            })
        }));

        let state_tx = event_tx.clone();
        peer.on_peer_connection_state_change(Box::new(move |state| {
            let state_tx = state_tx.clone();
            Box::pin(async move {
                let _ = state_tx.send(TransportEvent::Connection(state));
            })
        }));

        let audio_event_tx = event_tx;
        peer.on_track(Box::new(move |track, _, _| {
            let player = player.clone();
            let event_tx = audio_event_tx.clone();
            let audio_tasks_tx = audio_tasks_tx.clone();
            let audio_activity_tx = audio_activity_tx.clone();
            Box::pin(async move {
                let codec = track.codec().capability;
                debug!(
                    ssrc = track.ssrc(),
                    mime_type = %codec.mime_type,
                    clock_rate = codec.clock_rate,
                    channels = codec.channels,
                    "received Qwen WebRTC remote track"
                );
                if track.kind() != RTPCodecType::Audio
                    || !codec.mime_type.eq_ignore_ascii_case(MIME_TYPE_OPUS)
                {
                    return;
                }
                let task = tokio::spawn(async move {
                    let result = run_opus_decoder(track, player, audio_activity_tx).await;
                    if let Err(error) = &result {
                        let _ = event_tx.send(TransportEvent::AudioError(format!("{error:#}")));
                    }
                    result
                });
                let _ = audio_tasks_tx.send(task);
            })
        }));

        let offer = peer
            .create_offer(None)
            .await
            .context("create Qwen WebRTC offer")?;
        let mut gathering = peer.gathering_complete_promise().await;
        peer.set_local_description(offer)
            .await
            .context("set Qwen WebRTC local offer")?;
        cancellation_aware(
            cancel_rx,
            timeout_duration(config.connect_timeout_s),
            "Qwen WebRTC ICE gathering",
            wait_for_ice_gathering_complete(&mut gathering),
        )
        .await?
        .into_completed("Qwen WebRTC setup cancelled")?;
        let local = peer
            .local_description()
            .await
            .context("Qwen WebRTC local SDP missing")?;
        let answer = exchange_sdp(config, local.sdp, cancel_rx).await?;
        peer.set_remote_description(RTCSessionDescription::answer(answer)?)
            .await
            .context("set Qwen WebRTC remote answer")?;

        Ok(WebRtcConnection {
            peer: Arc::clone(&peer),
            audio_track,
            data_channel_rx,
            event_rx,
            audio_tasks_rx,
            audio_activity_rx,
            rtcp_task: rtcp_task.take().context("Qwen WebRTC RTCP task missing")?,
        })
    }
    .await;

    if setup_result.is_err() {
        if let Some(task) = rtcp_task.take() {
            task.abort();
            let _ = task.await;
        }
        let _ = timeout(WEBSOCKET_CLOSE_TIMEOUT, peer.close()).await;
    }
    setup_result
}

fn install_rustls_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        // The dependency graph enables both rustls providers, so automatic
        // selection panics when WebRTC starts DTLS. Ring is already required by
        // this binary and supports the OH2P ARMv7 target.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

fn conversational_turn_detection(config: &QwenRealtimeConfig) -> serde_json::Value {
    json!({
        "type": "semantic_vad",
        "threshold": config.turn_detection_threshold,
        "silence_duration_ms": config.turn_detection_silence_duration_ms
    })
}

async fn wait_for_ice_gathering_complete(gathering: &mut mpsc::Receiver<()>) -> anyhow::Result<()> {
    // webrtc-rs closes this channel to signal successful gather completion; it
    // does not send a value. Treat both closure and a future value-based signal
    // as completion. Cancellation and the setup timeout remain enforced by the
    // caller's cancellation_aware wrapper.
    let _ = gathering.recv().await;
    Ok(())
}

trait CompletedExt<T> {
    fn into_completed(self, cancelled: &'static str) -> anyhow::Result<T>;
}

impl<T> CompletedExt<T> for Cancellable<T> {
    fn into_completed(self, cancelled: &'static str) -> anyhow::Result<T> {
        match self {
            Cancellable::Completed(value) => Ok(value),
            Cancellable::Cancelled => anyhow::bail!(cancelled),
        }
    }
}

async fn exchange_sdp(
    config: &QwenRealtimeConfig,
    offer: String,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<String> {
    let mut url = Url::parse(&config.url).context("parse Qwen WebRTC signaling URL")?;
    url.query_pairs_mut()
        .clear()
        .append_pair("model", &config.model);
    let client = reqwest::Client::new();
    let api_key = config.api_key.clone();
    cancellation_aware(
        cancel_rx,
        timeout_duration(config.connect_timeout_s),
        "Qwen WebRTC SDP exchange",
        async move {
            let response = client
                .post(url)
                .bearer_auth(api_key)
                .header(reqwest::header::CONTENT_TYPE, "application/sdp")
                .body(offer)
                .send()
                .await
                .context("post Qwen WebRTC offer")?;
            let status = response.status();
            let body = response.text().await.context("read Qwen WebRTC answer")?;
            anyhow::ensure!(
                status.is_success(),
                "Qwen WebRTC SDP exchange failed ({status}): {}",
                bounded_text(&body)
            );
            Ok(body)
        },
    )
    .await?
    .into_completed("Qwen WebRTC SDP exchange cancelled")
}

fn bounded_text(text: &str) -> String {
    text.chars().take(256).collect()
}

async fn wait_for_txt_channel(
    rx: &mut mpsc::UnboundedReceiver<Arc<RTCDataChannel>>,
    deadline: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<Arc<RTCDataChannel>> {
    cancellation_aware(cancel_rx, deadline, "Qwen txt DataChannel", async {
        rx.recv()
            .await
            .context("Qwen txt DataChannel was not created")
    })
    .await?
    .into_completed("Qwen WebRTC setup cancelled")
}

async fn wait_for_first_server_event(
    rx: &mut mpsc::UnboundedReceiver<TransportEvent>,
    deadline: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    loop {
        let event =
            cancellation_aware(cancel_rx, deadline, "first Qwen DataChannel event", async {
                rx.recv().await.context("Qwen WebRTC event channel closed")
            })
            .await?
            .into_completed("Qwen WebRTC setup cancelled")?;
        match event {
            TransportEvent::Data(text) => {
                let event: ServerEvent =
                    serde_json::from_str(&text).context("decode first Qwen DataChannel event")?;
                debug!(event = ?event, "received first Qwen WebRTC event");
                return Ok(());
            }
            TransportEvent::Connection(
                RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed,
            ) => {
                anyhow::bail!("Qwen WebRTC connection failed during setup")
            }
            TransportEvent::Connection(_) => {}
            TransportEvent::AudioError(error) => anyhow::bail!(error),
        }
    }
}

async fn wait_for_session_updated(
    rx: &mut mpsc::UnboundedReceiver<TransportEvent>,
    deadline: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    loop {
        let event = cancellation_aware(cancel_rx, deadline, "Qwen session.updated", async {
            rx.recv().await.context("Qwen WebRTC event channel closed")
        })
        .await?
        .into_completed("Qwen WebRTC setup cancelled")?;
        match event {
            TransportEvent::Data(text) => match serde_json::from_str::<ServerEvent>(&text)
                .context("decode Qwen session setup event")?
            {
                ServerEvent::SessionUpdated(updated) => {
                    tracing::info!(
                        tools = updated.session.tools.len(),
                        "Qwen WebRTC session.updated acknowledged"
                    );
                    return Ok(());
                }
                ServerEvent::Error(error) => {
                    anyhow::bail!("Qwen session update failed: {}", error.error.message)
                }
                _ => {}
            },
            TransportEvent::Connection(
                RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed,
            ) => {
                anyhow::bail!("Qwen WebRTC connection failed during session setup")
            }
            TransportEvent::Connection(_) => {}
            TransportEvent::AudioError(error) => anyhow::bail!(error),
        }
    }
}

async fn send_data_event(
    channel: &RTCDataChannel,
    event: &ClientEvent,
    deadline: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let text = serde_json::to_string(event)?;
    cancellation_aware(cancel_rx, deadline, "send Qwen DataChannel event", async {
        channel
            .send_text(text)
            .await
            .context("send Qwen DataChannel event")?;
        Ok(())
    })
    .await?
    .into_completed("Qwen DataChannel send cancelled")
}

async fn send_data_event_bounded(
    channel: &RTCDataChannel,
    event: &ClientEvent,
    deadline: Duration,
) -> anyhow::Result<()> {
    let text = serde_json::to_string(event)?;
    timeout(deadline, channel.send_text(text))
        .await
        .context("send Qwen DataChannel event timed out")?
        .context("send Qwen DataChannel event")?;
    Ok(())
}

async fn run_opus_writer(
    track: Arc<TrackLocalStaticSample>,
    mut pcm_rx: mpsc::Receiver<Vec<u8>>,
    mut cancel_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut encoder = Encoder::new(OPUS_SAMPLE_RATE, Channels::Mono, Application::Voip)
        .context("create WebRTC Opus encoder")?;
    encoder.set_inband_fec(true)?;
    encoder.set_packet_loss_perc(10)?;
    let mut samples = VecDeque::<i16>::new();
    let mut previous_input_sample = None;
    let mut ticker = interval(Duration::from_millis(OPUS_FRAME_MS));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut pcm_open = true;

    loop {
        tokio::select! {
            biased;
            changed = cancel_rx.changed() => {
                if changed.is_err() || *cancel_rx.borrow() { break; }
            }
            chunk = pcm_rx.recv(), if pcm_open => {
                match chunk {
                    Some(chunk) => {
                        validate_s16le_frame(&chunk, "WebRTC VPM PCM")?;
                        enqueue_upsampled_pcm(&mut samples, &mut previous_input_sample, &chunk);
                        let dropped = trim_stale_input(&mut samples);
                        if dropped > 0 {
                            warn!(
                                dropped_samples = dropped,
                                "WebRTC input fell behind; dropped stale PCM"
                            );
                        }
                    }
                    None => {
                        flush_upsampled_pcm(&mut samples, &mut previous_input_sample);
                        pcm_open = false;
                    }
                }
            }
            _ = ticker.tick() => {
                let mut frame = [0i16; OPUS_INPUT_SAMPLES];
                for sample in &mut frame {
                    if let Some(value) = samples.pop_front() {
                        *sample = value;
                    } else {
                        break;
                    }
                }
                let packet = encoder.encode_vec(&frame, OPUS_MAX_PACKET_BYTES)?;
                track.write_sample(&Sample {
                    data: Bytes::from(packet),
                    duration: Duration::from_millis(OPUS_FRAME_MS),
                    ..Default::default()
                }).await.context("send Qwen WebRTC Opus sample")?;
            }
        }
    }
    Ok(())
}

fn enqueue_upsampled_pcm(output: &mut VecDeque<i16>, previous: &mut Option<i16>, pcm_16k: &[u8]) {
    for bytes in pcm_16k.chunks_exact(2) {
        let current = i16::from_le_bytes([bytes[0], bytes[1]]);
        if let Some(prior) = *previous {
            let prior = i32::from(prior);
            let delta = i32::from(current) - prior;
            for phase in 0..OPUS_UPSAMPLE_FACTOR {
                output.push_back((prior + delta * phase / OPUS_UPSAMPLE_FACTOR) as i16);
            }
        }
        *previous = Some(current);
    }
}

fn flush_upsampled_pcm(output: &mut VecDeque<i16>, previous: &mut Option<i16>) {
    if let Some(last) = previous.take() {
        output.extend(std::iter::repeat_n(last, OPUS_UPSAMPLE_FACTOR as usize));
    }
}

fn trim_stale_input(samples: &mut VecDeque<i16>) -> usize {
    let stale = samples.len().saturating_sub(MAX_OPUS_BUFFERED_SAMPLES);
    samples.drain(..stale);
    stale
}

async fn run_opus_decoder(
    track: Arc<webrtc::track::track_remote::TrackRemote>,
    player: PcmPlayerHandle,
    audio_activity_tx: watch::Sender<u64>,
) -> anyhow::Result<()> {
    let channels = if track.codec().capability.channels == 1 {
        Channels::Mono
    } else {
        Channels::Stereo
    };
    let channel_count = if channels == Channels::Mono { 1 } else { 2 };
    let mut decoder = Decoder::new(48_000, channels).context("create WebRTC Opus decoder")?;
    let mut decoded = vec![0i16; OPUS_MAX_DECODE_SAMPLES_PER_CHANNEL * channel_count];
    let mut last_sequence = None::<u16>;
    let mut received_packets = 0u64;

    loop {
        let (packet, _) = track
            .read_rtp()
            .await
            .context("read Qwen WebRTC audio RTP")?;
        if let Some(previous) = last_sequence {
            let delta = packet.header.sequence_number.wrapping_sub(previous);
            if delta == 0 || delta >= 0x8000 {
                continue;
            }
            let missing = delta.saturating_sub(1);
            for _ in 0..missing.min(3) {
                let count = decoder.decode(&[], &mut decoded, false)?;
                queue_decoded_pcm(&player, &decoded, count, channel_count)?;
            }
        }
        last_sequence = Some(packet.header.sequence_number);
        let count = decoder.decode(&packet.payload, &mut decoded, false)?;
        queue_decoded_pcm(&player, &decoded, count, channel_count)?;
        received_packets += 1;
        if received_packets == 1 {
            let peak = decoded[..count * channel_count]
                .iter()
                .map(|sample| sample.unsigned_abs())
                .max()
                .unwrap_or_default();
            debug!(
                payload_bytes = packet.payload.len(),
                samples_per_channel = count,
                channels = channel_count,
                peak,
                "decoded first Qwen WebRTC audio packet"
            );
        }
        audio_activity_tx.send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

fn queue_decoded_pcm(
    player: &PcmPlayerHandle,
    decoded: &[i16],
    samples_per_channel: usize,
    channels: usize,
) -> anyhow::Result<()> {
    let mut pcm = Vec::with_capacity(samples_per_channel * 2);
    if channels == 1 {
        for sample in &decoded[..samples_per_channel] {
            pcm.extend_from_slice(&sample.to_le_bytes());
        }
    } else {
        for frame in decoded[..samples_per_channel * channels].chunks_exact(channels) {
            let mixed = (frame.iter().map(|sample| i32::from(*sample)).sum::<i32>()
                / channels as i32) as i16;
            pcm.extend_from_slice(&mixed.to_le_bytes());
        }
    }
    for block in pcm.chunks(PLAYBACK_BLOCK_SAMPLES * std::mem::size_of::<i16>()) {
        match player.try_audio(block.to_vec()) {
            Ok(()) | Err(QueueError::Suppressed) => {}
            Err(QueueError::Full) => anyhow::bail!("WebRTC playback queue full"),
            Err(QueueError::Closed) => anyhow::bail!("WebRTC playback stopped"),
        }
    }
    Ok(())
}

struct EventLoopContext<'a> {
    channel: &'a RTCDataChannel,
    event_rx: &'a mut mpsc::UnboundedReceiver<TransportEvent>,
    audio_activity_rx: &'a mut watch::Receiver<u64>,
    capture_task: &'a mut AbortOnDropTask<anyhow::Result<()>>,
    capture_joined: &'a mut bool,
    player: &'a PcmPlayerHandle,
    tools: &'a NativeToolRuntime,
    event_timeout: Duration,
    idle_timeout: Duration,
    control: &'a mut RealtimeSessionControl,
    machine: &'a mut SessionMachine,
}

async fn run_event_loop(context: EventLoopContext<'_>) -> anyhow::Result<()> {
    let EventLoopContext {
        channel,
        event_rx,
        audio_activity_rx,
        capture_task,
        capture_joined,
        player,
        tools,
        event_timeout,
        idle_timeout,
        control,
        machine,
    } = context;
    let cancel_rx = &mut control.cancel_rx;
    let interrupt_rx = &mut control.interrupt_rx;
    let mut active_response: Option<ResponseId> = None;
    let mut response_deadline: Option<Instant> = None;
    let mut response_calls = HashMap::<CallId, PendingFunctionCall>::new();
    let mut seen_call_ids = HashSet::<CallId>::new();
    let mut pending_tools = FuturesUnordered::<ToolFuture>::new();
    let mut waiting_for_tools = false;
    let mut tool_iterations = 0usize;
    let mut tool_calls = 0usize;
    let mut budget_refusals = 0usize;
    let mut session_idle_deadline = Some(Instant::now() + idle_timeout);
    let mut speech_stopped_at = None::<Instant>;
    // Set when the model calls the end_conversation tool; the session shuts
    // down after the farewell response finishes playing.
    let mut end_after_response = false;
    // After response.done the RTP audio tail may keep streaming for seconds.
    // Track the drain as a deadline inside the select loop so wake-word
    // interrupts and cancellation stay responsive during playback.
    let mut drain_deadline = None::<Instant>;

    loop {
        let tools_pending = !pending_tools.is_empty();
        tokio::select! {
            biased;
            changed = cancel_rx.changed() => {
                if changed.is_err() || *cancel_rx.borrow() {
                    machine.transition(SessionState::Cancelling)?;
                    if let Some(response_id) = active_response.take() {
                        let _ = send_data_event_bounded(channel, &ClientEvent::ResponseCancel {
                            event_id: None,
                            response_id: Some(response_id),
                        }, CANCEL_EVENT_TIMEOUT).await;
                    }
                    machine.transition(SessionState::ShuttingDown)?;
                    return Ok(());
                }
            }
            changed = interrupt_rx.changed() => {
                if changed.is_ok() {
                    player.interrupt_playback();
                    drain_deadline = None;
                    // The user is expected to speak next; keep the idle
                    // timeout armed so a silent interrupt cannot leave the
                    // session capturing forever.
                    session_idle_deadline = Some(Instant::now() + idle_timeout);
                    if machine.state() == SessionState::Responding {
                        if active_response.is_some() {
                            let _ = send_data_event_bounded(channel, &ClientEvent::ResponseCancel {
                                event_id: None,
                                response_id: None,
                            }, CANCEL_EVENT_TIMEOUT).await;
                        }
                        active_response = None;
                        response_calls.clear();
                        response_deadline = None;
                        machine.transition(SessionState::Capturing)?;
                    } else if machine.state() == SessionState::Ready {
                        machine.transition(SessionState::Capturing)?;
                    }
                }
            }
            changed = audio_activity_rx.changed(), if drain_deadline.is_some() => {
                if changed.is_ok() {
                    drain_deadline = Some(Instant::now() + WEBRTC_AUDIO_QUIET_PERIOD);
                }
            }
            _ = async {
                match drain_deadline {
                    Some(deadline) => sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                drain_deadline = None;
                if end_after_response {
                    tracing::info!("native Qwen conversation ended by end_conversation tool");
                    machine.transition(SessionState::ShuttingDown)?;
                    return Ok(());
                }
                machine.transition(SessionState::Ready)?;
                session_idle_deadline = Some(Instant::now() + idle_timeout);
            }
            _ = async {
                match session_idle_deadline {
                    Some(deadline) => sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                debug!("Qwen WebRTC conversation idle timeout");
                machine.transition(SessionState::ShuttingDown)?;
                return Ok(());
            }
            _ = async {
                match response_deadline {
                    Some(deadline) => sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => anyhow::bail!("timed out waiting for Qwen WebRTC response"),
            captured = capture_task.join(), if !*capture_joined => {
                *capture_joined = true;
                captured.context("Qwen WebRTC capture task panicked")??;
                anyhow::bail!("continuous WebRTC capture stopped unexpectedly");
            }
            Some(execution) = pending_tools.next(), if tools_pending => {
                send_data_event(channel, &ClientEvent::ConversationItemCreate {
                    event_id: None,
                    item: ConversationItem::FunctionCallOutput {
                        call_id: execution.call_id,
                        output: FunctionCallOutput(execution.output),
                    },
                }, event_timeout, cancel_rx).await?;
                if waiting_for_tools && pending_tools.is_empty() {
                    anyhow::ensure!(tool_iterations <= tools.max_iterations, "native Qwen tool iteration limit reached");
                    send_data_event(channel, &ClientEvent::ResponseCreate {
                        event_id: None,
                        response: None,
                    }, event_timeout, cancel_rx).await?;
                    waiting_for_tools = false;
                    active_response = None;
                    response_deadline = Some(Instant::now() + event_timeout);
                }
            }
            transport = event_rx.recv() => {
                let transport = transport.context("Qwen WebRTC event channel closed")?;
                match transport {
                    TransportEvent::Connection(RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) => {
                        anyhow::bail!("Qwen WebRTC connection closed")
                    }
                    TransportEvent::Connection(_) => continue,
                    TransportEvent::AudioError(error) => anyhow::bail!(error),
                    TransportEvent::Data(text) => {
                        if response_deadline.is_some() {
                            response_deadline = Some(Instant::now() + event_timeout);
                        }
                        let event: ServerEvent = serde_json::from_str(&text)
                            .context("decode Qwen WebRTC DataChannel event")?;
                        match event {
                            ServerEvent::ResponseFunctionCallArgumentsDone(call) => {
                                anyhow::ensure!(!waiting_for_tools && pending_tools.is_empty(), "received a function call while prior tools were running");
                                anyhow::ensure!(active_response.as_ref().is_none_or(|id| id == &call.response_id), "function call belongs to a conflicting response");
                                anyhow::ensure!(!seen_call_ids.contains(&call.call_id), "replayed Qwen function call ID");
                                anyhow::ensure!(!call.call_id.0.is_empty() && call.call_id.0.len() <= MAX_CALL_ID_BYTES, "invalid Qwen function call ID");
                                anyhow::ensure!(!call.name.is_empty() && call.name.len() <= MAX_TOOL_NAME_BYTES, "invalid Qwen function name");
                                anyhow::ensure!(call.arguments.0.len() <= MAX_TOOL_ARGUMENT_BYTES, "Qwen function arguments exceed the safety limit");
                                active_response = Some(call.response_id);
                                seen_call_ids.insert(call.call_id.clone());
                                response_calls.insert(call.call_id, PendingFunctionCall {
                                    item_id: call.item_id.0,
                                    output_index: call.output_index,
                                    name: call.name,
                                    arguments: call.arguments.0,
                                });
                            }
                            ServerEvent::ResponseDone(done) => {
                                debug!(
                                    status = ?done.response.status,
                                    output_items = done.response.output.len(),
                                    "received Qwen WebRTC response.done"
                                );
                                if done.response.status == ResponseStatus::Canceled {
                                    if active_response.as_ref().is_none_or(|id| id == &done.response.id) {
                                        active_response = None;
                                        response_calls.clear();
                                        response_deadline = None;
                                        if machine.state() == SessionState::Responding {
                                            machine.transition(SessionState::Capturing)?;
                                        }
                                    }
                                    continue;
                                }
                                anyhow::ensure!(done.response.status == ResponseStatus::Completed, "Qwen response ended with status {:?}", done.response.status);
                                anyhow::ensure!(active_response.as_ref().is_none_or(|id| id == &done.response.id), "response.done belongs to a conflicting response");
                                let done_calls = done.response.output.iter().enumerate().filter_map(|(output_index, item)| match item {
                                    ResponseOutputItem::FunctionCall { id, status, call_id, name, arguments } => Some((output_index, id, status, call_id, name, arguments)),
                                    _ => None,
                                }).collect::<Vec<_>>();
                                if done_calls.is_empty() {
                                    anyhow::ensure!(response_calls.is_empty(), "response.done omitted a received function call");
                                    if machine.state() == SessionState::Capturing {
                                        machine.transition(SessionState::Responding)?;
                                    }
                                    active_response = None;
                                    response_deadline = None;
                                    drain_deadline = Some(Instant::now() + WEBRTC_AUDIO_QUIET_PERIOD);
                                    continue;
                                }
                                anyhow::ensure!(done_calls.len() <= tools.max_calls && done_calls.len() == response_calls.len(), "response.done function calls do not match arguments events");
                                if done_calls.iter().any(|(_, _, _, _, name, _)| *name == crate::tools::END_CONVERSATION_TOOL_NAME) {
                                    end_after_response = true;
                                }
                                let mut done_ids = HashSet::new();
                                for (output_index, id, status, call_id, name, arguments) in done_calls {
                                    anyhow::ensure!(*status == ResponseStatus::Completed && done_ids.insert(call_id.clone()), "duplicate or incomplete response.done function call");
                                    let received = response_calls.get(call_id).context("response.done function call is missing arguments")?;
                                    anyhow::ensure!(received.output_index as usize == output_index && received.item_id == id.0 && received.name == *name && received.arguments == arguments.0, "conflicting function call data across Qwen events");
                                }
                                if tool_budget_exhausted(tools, tool_iterations, tool_calls, response_calls.len()) {
                                    // Refuse the calls but keep the conversation alive:
                                    // tell the model to answer with what it has instead
                                    // of killing the live session mid-interaction.
                                    budget_refusals += 1;
                                    anyhow::ensure!(
                                        budget_refusals <= TOOL_BUDGET_EXHAUSTED_GRACE,
                                        "model kept requesting tools after the budget was exhausted"
                                    );
                                    warn!(
                                        tool_iterations,
                                        tool_calls,
                                        budget_refusals,
                                        "native Qwen tool budget exhausted; forcing a direct answer"
                                    );
                                    for (call_id, _) in response_calls.drain() {
                                        send_data_event(channel, &ClientEvent::ConversationItemCreate {
                                            event_id: None,
                                            item: ConversationItem::FunctionCallOutput {
                                                call_id,
                                                output: FunctionCallOutput(structured_tool_error(
                                                    "tool_budget_exhausted",
                                                    TOOL_BUDGET_EXHAUSTED_MESSAGE,
                                                )),
                                            },
                                        }, event_timeout, cancel_rx).await?;
                                    }
                                    send_data_event(channel, &ClientEvent::ResponseCreate {
                                        event_id: None,
                                        response: None,
                                    }, event_timeout, cancel_rx).await?;
                                    active_response = None;
                                    response_deadline = Some(Instant::now() + event_timeout);
                                    continue;
                                }
                                tool_iterations += 1;
                                tool_calls += response_calls.len();
                                waiting_for_tools = true;
                                response_deadline = None;
                                for (call_id, call) in response_calls.drain() {
                                    pending_tools.push(tools.execute(call_id, call.name, call.arguments));
                                }
                            }
                            ServerEvent::ResponseAudioDelta(_) => anyhow::bail!("Qwen returned WebSocket audio on a WebRTC session"),
                            ServerEvent::ResponseAudioTranscriptDone(done) => {
                                active_response = Some(done.response_id);
                                debug!(characters = done.transcript.chars().count(), "Qwen WebRTC transcript completed");
                            }
                            ServerEvent::Error(error) => anyhow::bail!("Qwen realtime error: {}", error.error.message),
                            ServerEvent::ResponseCreated(created) => {
                                player.begin_response();
                                active_response = Some(created.response.id);
                                if matches!(machine.state(), SessionState::Capturing | SessionState::Ready) {
                                    machine.transition(SessionState::Responding)?;
                                }
                                session_idle_deadline = None;
                                response_deadline = Some(Instant::now() + event_timeout);
                                if let Some(stopped_at) = speech_stopped_at.take() {
                                    tracing::info!(
                                        target: "xiaoai_agent::qwen_latency",
                                        turn_end_to_response_ms = stopped_at.elapsed().as_millis() as u64,
                                        "QWEN_LATENCY response_created"
                                    );
                                }
                            }
                            ServerEvent::InputAudioSpeechStarted(started) => {
                                player.interrupt_playback();
                                drain_deadline = None;
                                session_idle_deadline = None;
                                response_deadline = None;
                                if matches!(machine.state(), SessionState::Responding | SessionState::Ready) {
                                    machine.transition(SessionState::Capturing)?;
                                }
                                debug!(
                                    audio_start_ms = started.audio_start_ms,
                                    item_id = %started.item_id.0,
                                    "Qwen WebRTC user speech started; playback interrupted"
                                );
                            }
                            ServerEvent::InputAudioSpeechStopped(stopped) => {
                                speech_stopped_at = Some(Instant::now());
                                response_deadline = Some(Instant::now() + event_timeout);
                                debug!(
                                    audio_end_ms = stopped.audio_end_ms,
                                    item_id = %stopped.item_id.0,
                                    "Qwen WebRTC user speech stopped"
                                );
                            }
                            ServerEvent::Unknown { event_type } => {
                                debug!(%event_type, "received Qwen WebRTC event");
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixes_stereo_opus_pcm_to_mono_s16le() {
        let (audio_tx, mut audio_rx) = mpsc::channel(1);
        let (control_tx, _control_rx) = watch::channel(PlaybackControl::Running);
        let player = PcmPlayerHandle {
            audio_tx,
            control_tx,
            clear_tx: watch::channel(0).0,
            accepting_audio: Arc::new(AtomicBool::new(true)),
            response_started_at: Arc::new(StdMutex::new(None)),
            spawn_gate: Arc::new(StdMutex::new(PlaybackControl::Running)),
        };
        let decoded = [1000i16, -1000, 3000, 1000];
        queue_decoded_pcm(&player, &decoded, 2, 2).unwrap();
        assert_eq!(audio_rx.try_recv().unwrap(), [0, 0, 0xd0, 0x07]);
    }

    #[test]
    fn bounds_signaling_error_text() {
        assert_eq!(bounded_text(&"x".repeat(300)).len(), 256);
    }

    #[tokio::test]
    async fn closed_ice_gathering_channel_signals_completion() {
        let (tx, mut rx) = mpsc::channel(1);
        drop(tx);
        wait_for_ice_gathering_complete(&mut rx).await.unwrap();
    }

    #[test]
    fn installs_explicit_rustls_crypto_provider() {
        install_rustls_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[test]
    fn linearly_upsamples_streaming_pcm_from_16k_to_48k() {
        let mut output = VecDeque::new();
        let mut previous = None;
        enqueue_upsampled_pcm(&mut output, &mut previous, &0i16.to_le_bytes());
        enqueue_upsampled_pcm(&mut output, &mut previous, &3000i16.to_le_bytes());
        flush_upsampled_pcm(&mut output, &mut previous);
        assert_eq!(
            output.into_iter().collect::<Vec<_>>(),
            vec![0, 1000, 2000, 3000, 3000, 3000]
        );
    }

    #[test]
    fn drops_oldest_pcm_when_the_opus_writer_falls_behind() {
        let mut samples = (0..MAX_OPUS_BUFFERED_SAMPLES + 3)
            .map(|sample| sample as i16)
            .collect::<VecDeque<_>>();

        assert_eq!(trim_stale_input(&mut samples), 3);
        assert_eq!(samples.len(), MAX_OPUS_BUFFERED_SAMPLES);
        assert_eq!(samples.front(), Some(&3));
    }

    #[test]
    fn tool_budget_exhaustion_is_reached_at_the_configured_limits() {
        let mut config = QwenRealtimeConfig::default();
        config.max_tool_iterations = 2;
        config.max_tool_calls = 3;
        let server = rig_core::tool::server::ToolServer::new().run();
        let tools = NativeToolRuntime::from_definitions(&config, server, None, Vec::new());

        assert!(!tool_budget_exhausted(&tools, 0, 0, 1));
        assert!(!tool_budget_exhausted(&tools, 1, 1, 2));
        // Iteration limit reached.
        assert!(tool_budget_exhausted(&tools, 2, 2, 1));
        // Call limit would be exceeded by the requested batch.
        assert!(tool_budget_exhausted(&tools, 1, 3, 1));
    }

    #[test]
    fn uses_the_documented_qwen_web_rtc_semantic_vad_shape() {
        let config = QwenRealtimeConfig::default();
        let vad = conversational_turn_detection(&config);
        assert_eq!(vad["type"], "semantic_vad");
        assert_eq!(vad["threshold"], 0.5);
        assert_eq!(vad["silence_duration_ms"], 500);
        assert!(vad.get("create_response").is_none());
        assert!(vad.get("interrupt_response").is_none());
    }
}
