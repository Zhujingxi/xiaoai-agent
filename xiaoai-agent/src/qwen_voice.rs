use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use base64::Engine as _;
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};
use url::Url;

use crate::capture::record_utterance_streaming;
use crate::config::{timeout_duration, CaptureConfig, QwenRealtimeConfig};
use crate::qwen_realtime::{
    AudioFormat, Base64Pcm, ClientEvent, Modality, ResponseId, ServerEvent, SessionUpdate,
};

const UPLOAD_QUEUE_CAPACITY: usize = 32;
const PLAYBACK_QUEUE_CAPACITY: usize = 64;
const PLAYER_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const PLAYER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const RECONNECT_ATTEMPTS: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Disconnected,
    Connecting,
    Ready,
    Capturing,
    Responding,
    Cancelling,
    Reconnecting,
    ShuttingDown,
    Closed,
    Failed,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StateTransitionError {
    #[error("invalid realtime session transition from {from:?} to {to:?}")]
    Invalid {
        from: SessionState,
        to: SessionState,
    },
}

#[derive(Debug)]
struct SessionMachine {
    state: SessionState,
}

impl SessionMachine {
    fn new() -> Self {
        Self {
            state: SessionState::Disconnected,
        }
    }

    fn state(&self) -> SessionState {
        self.state
    }

    fn transition(&mut self, to: SessionState) -> Result<(), StateTransitionError> {
        use SessionState::*;
        let valid = matches!(
            (self.state, to),
            (Disconnected, Connecting)
                | (Connecting, Ready | Reconnecting | Failed | ShuttingDown)
                | (
                    Ready,
                    Capturing | Cancelling | ShuttingDown | Reconnecting | Failed
                )
                | (
                    Capturing,
                    Responding | Cancelling | ShuttingDown | Reconnecting | Failed
                )
                | (
                    Responding,
                    Capturing | Ready | Cancelling | ShuttingDown | Reconnecting | Failed
                )
                | (Cancelling, Ready | ShuttingDown | Reconnecting | Failed)
                | (Reconnecting, Connecting | ShuttingDown | Failed)
                | (Failed, Reconnecting | ShuttingDown | Closed)
                | (ShuttingDown, Closed)
        );
        if !valid {
            return Err(StateTransitionError::Invalid {
                from: self.state,
                to,
            });
        }
        self.state = to;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressurePolicy {
    DropNewest,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QueueError {
    #[error("queue is full; newest item was dropped")]
    Full,
    #[error("queue is closed")]
    Closed,
}

#[derive(Clone)]
struct BoundedAudioSender {
    tx: mpsc::Sender<UploadCommand>,
    policy: BackpressurePolicy,
}

impl BoundedAudioSender {
    fn try_send(&self, bytes: Vec<u8>) -> Result<(), QueueError> {
        match self.tx.try_send(UploadCommand::Audio(bytes)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(QueueError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(QueueError::Closed),
        }
    }
}

#[derive(Debug)]
enum UploadCommand {
    Audio(Vec<u8>),
    Clear,
    Commit,
}

#[derive(Debug)]
enum PlayerCommand {
    Audio(Vec<u8>),
    Clear(tokio::sync::oneshot::Sender<()>),
    Shutdown,
}

#[derive(Clone)]
pub struct PcmPlayerHandle {
    tx: mpsc::Sender<PlayerCommand>,
}

impl PcmPlayerHandle {
    fn try_audio(&self, bytes: Vec<u8>) -> Result<(), QueueError> {
        match self.tx.try_send(PlayerCommand::Audio(bytes)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(QueueError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(QueueError::Closed),
        }
    }

    async fn clear(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.tx.send(PlayerCommand::Clear(tx)).await.is_ok() {
            let _ = timeout(PLAYER_SHUTDOWN_TIMEOUT, rx).await;
        }
    }

    async fn shutdown(&self) {
        let _ = self.tx.send(PlayerCommand::Shutdown).await;
    }
}

pub struct PcmPlayer {
    handle: PcmPlayerHandle,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}

impl PcmPlayer {
    pub fn spawn(path: String, device: String, sample_rate: u32) -> Self {
        let (tx, rx) = mpsc::channel(PLAYBACK_QUEUE_CAPACITY);
        let task = tokio::spawn(run_player(path, device, sample_rate, rx));
        Self {
            handle: PcmPlayerHandle { tx },
            task: Some(task),
        }
    }

    fn handle(&self) -> PcmPlayerHandle {
        self.handle.clone()
    }

    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.handle.shutdown().await;
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match timeout(PLAYER_SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(result) => result.context("realtime PCM player task panicked")?,
            Err(_) => {
                task.abort();
                let _ = task.await;
                anyhow::bail!("timed out shutting down realtime PCM player");
            }
        }
    }
}

impl Drop for PcmPlayer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn spawn_aplay(
    path: &str,
    device: &str,
    sample_rate: u32,
) -> anyhow::Result<(Child, ChildStdin)> {
    let mut child = Command::new(path)
        .args([
            "-q",
            "-D",
            device,
            "-t",
            "raw",
            "-f",
            "S16_LE",
            "-r",
            &sample_rate.to_string(),
            "-c",
            "1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawn realtime player {path}"))?;
    let stdin = child
        .stdin
        .take()
        .context("realtime player stdin missing")?;
    Ok((child, stdin))
}

async fn stop_child(child: &mut Child) {
    let _ = child.start_kill();
    let _ = timeout(PLAYER_SHUTDOWN_TIMEOUT, child.wait()).await;
}

async fn run_player(
    path: String,
    device: String,
    sample_rate: u32,
    mut rx: mpsc::Receiver<PlayerCommand>,
) -> anyhow::Result<()> {
    let (mut child, mut stdin) = spawn_aplay(&path, &device, sample_rate).await?;
    while let Some(command) = rx.recv().await {
        match command {
            PlayerCommand::Audio(bytes) => {
                timeout(PLAYER_WRITE_TIMEOUT, stdin.write_all(&bytes))
                    .await
                    .context("realtime player write timed out")?
                    .context("write realtime PCM")?;
            }
            PlayerCommand::Clear(ack) => {
                stop_child(&mut child).await;
                while rx.try_recv().is_ok() {}
                (child, stdin) = spawn_aplay(&path, &device, sample_rate).await?;
                let _ = ack.send(());
            }
            PlayerCommand::Shutdown => break,
        }
    }
    drop(stdin);
    stop_child(&mut child).await;
    Ok(())
}

#[derive(Debug)]
enum SessionCommand {
    Cancel(tokio::sync::oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct SessionHandle {
    tx: mpsc::Sender<SessionCommand>,
}

impl SessionHandle {
    pub async fn cancel(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.tx.send(SessionCommand::Cancel(tx)).await.is_ok() {
            let _ = timeout(PLAYER_SHUTDOWN_TIMEOUT, rx).await;
        }
    }
}

#[derive(Clone)]
pub struct QwenVoiceService {
    config: QwenRealtimeConfig,
    capture: CaptureConfig,
    active: Arc<Mutex<Option<SessionHandle>>>,
}

impl QwenVoiceService {
    pub fn new(config: QwenRealtimeConfig, capture: CaptureConfig) -> Self {
        Self {
            config,
            capture,
            active: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn cancel_active(&self) {
        let handle = self.active.lock().await.clone();
        if let Some(handle) = handle {
            handle.cancel().await;
        }
    }

    pub async fn run_session(&self, idle_timeout: Duration) -> anyhow::Result<()> {
        let (command_tx, command_rx) = mpsc::channel(4);
        let handle = SessionHandle { tx: command_tx };
        *self.active.lock().await = Some(handle);
        let result = run_realtime_session(
            self.config.clone(),
            self.capture.clone(),
            idle_timeout,
            command_rx,
        )
        .await;
        self.active.lock().await.take();
        result
    }
}

async fn connect(
    config: &QwenRealtimeConfig,
) -> anyhow::Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let mut url = Url::parse(&config.url).context("parse Qwen realtime URL")?;
    url.query_pairs_mut().append_pair("model", &config.model);
    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", config.api_key))
            .context("invalid Qwen API key header")?,
    );
    timeout(
        timeout_duration(config.connect_timeout_s),
        connect_async(request),
    )
    .await
    .context("Qwen realtime connect timed out")?
    .context("connect Qwen realtime websocket")
    .map(|(ws, _)| ws)
}

async fn send_event<S>(sink: &mut S, event: &ClientEvent) -> anyhow::Result<()>
where
    S: futures::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let text = serde_json::to_string(event)?;
    sink.send(Message::Text(text.into()))
        .await
        .context("send Qwen realtime event")
}

async fn run_realtime_session(
    config: QwenRealtimeConfig,
    capture: CaptureConfig,
    idle_timeout: Duration,
    mut command_rx: mpsc::Receiver<SessionCommand>,
) -> anyhow::Result<()> {
    let mut machine = SessionMachine::new();
    machine.transition(SessionState::Connecting)?;
    let mut last_error = None;
    for attempt in 0..=RECONNECT_ATTEMPTS {
        match run_connected_session(
            &config,
            &capture,
            idle_timeout,
            &mut command_rx,
            &mut machine,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) => {
                warn!(attempt, "Qwen realtime session failed: {err:#}");
                last_error = Some(err);
                if matches!(
                    machine.state(),
                    SessionState::ShuttingDown | SessionState::Closed
                ) {
                    break;
                }
                if !matches!(machine.state(), SessionState::Reconnecting) {
                    machine.transition(SessionState::Reconnecting)?;
                    machine.transition(SessionState::Connecting)?;
                }
            }
        }
    }
    machine.transition(SessionState::Failed).ok();
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Qwen realtime session failed")))
}

async fn run_connected_session(
    config: &QwenRealtimeConfig,
    capture: &CaptureConfig,
    idle_timeout: Duration,
    command_rx: &mut mpsc::Receiver<SessionCommand>,
    machine: &mut SessionMachine,
) -> anyhow::Result<()> {
    let ws = connect(config).await?;
    let (mut sink, mut stream) = ws.split();
    send_event(
        &mut sink,
        &ClientEvent::SessionUpdate {
            event_id: None,
            session: SessionUpdate {
                modalities: vec![Modality::Text, Modality::Audio],
                voice: config.voice.clone(),
                input_audio_format: AudioFormat::Pcm,
                output_audio_format: AudioFormat::Pcm,
                instructions: None,
                tools: Vec::new(),
            },
        },
    )
    .await?;
    machine.transition(SessionState::Ready)?;

    let player = PcmPlayer::spawn(
        "/usr/bin/aplay".to_string(),
        "default".to_string(),
        config.output_sample_rate.0,
    );
    let player_handle = player.handle();
    let (audio_tx, mut audio_rx) = mpsc::channel(UPLOAD_QUEUE_CAPACITY);
    let bounded = BoundedAudioSender {
        tx: audio_tx.clone(),
        policy: BackpressurePolicy::DropNewest,
    };
    let clear_tx = audio_tx.clone();
    let commit_tx = audio_tx;
    debug!(?bounded.policy, "Qwen upload backpressure policy");
    let capture_config = capture.clone();
    let capture_task = tokio::spawn(async move {
        let result = record_utterance_streaming(
            capture_config,
            idle_timeout,
            || async {},
            move |bytes| {
                let bounded = bounded.clone();
                async move {
                    if let Err(QueueError::Full) = bounded.try_send(bytes) {
                        warn!("Qwen upload queue full; dropping newest PCM chunk");
                    }
                    Ok(())
                }
            },
            move || {
                let clear_tx = clear_tx.clone();
                async move {
                    clear_tx
                        .send(UploadCommand::Clear)
                        .await
                        .context("Qwen upload queue closed while clearing rejected speech")
                }
            },
        )
        .await;
        if result.is_ok() {
            commit_tx
                .send(UploadCommand::Commit)
                .await
                .context("Qwen upload queue closed before commit")?;
        }
        result
    });
    machine.transition(SessionState::Capturing)?;
    tokio::pin!(capture_task);
    let mut active_response: Option<ResponseId> = None;
    let mut capture_done = false;

    loop {
        tokio::select! {
            command = command_rx.recv() => {
                let Some(command) = command else {
                    machine.transition(SessionState::ShuttingDown)?;
                    break;
                };
                match command {
                    SessionCommand::Cancel(ack) => {
                        machine.transition(SessionState::Cancelling)?;
                        send_event(&mut sink, &ClientEvent::ResponseCancel {
                            event_id: None,
                            response_id: active_response.take(),
                        }).await?;
                        player_handle.clear().await;
                        if !capture_done {
                            capture_task.as_mut().abort();
                        }
                        machine.transition(SessionState::ShuttingDown)?;
                        let _ = ack.send(());
                        break;
                    }
                }
            }
            Some(upload) = audio_rx.recv() => {
                match upload {
                    UploadCommand::Audio(bytes) => {
                        send_event(&mut sink, &ClientEvent::InputAudioBufferAppend {
                            event_id: None,
                            audio: Base64Pcm::new(base64::engine::general_purpose::STANDARD.encode(bytes)),
                        }).await?;
                    }
                    UploadCommand::Clear => {
                        send_event(&mut sink, &ClientEvent::InputAudioBufferClear { event_id: None }).await?;
                    }
                    UploadCommand::Commit => {
                        send_event(&mut sink, &ClientEvent::InputAudioBufferCommit { event_id: None }).await?;
                        send_event(&mut sink, &ClientEvent::ResponseCreate { event_id: None, response: None }).await?;
                        machine.transition(SessionState::Responding)?;
                    }
                }
            }
            captured = &mut capture_task, if !capture_done => {
                captured.context("Qwen capture task panicked")??;
                capture_done = true;
            }
            message = stream.next() => {
                let message = message.context("Qwen websocket closed")??;
                if message.is_close() {
                    anyhow::bail!("Qwen websocket closed");
                }
                let Message::Text(text) = message else { continue; };
                match serde_json::from_str::<ServerEvent>(&text).context("decode Qwen server event")? {
                    ServerEvent::ResponseAudioDelta(delta) => {
                        active_response = Some(delta.response_id);
                        let pcm = base64::engine::general_purpose::STANDARD
                            .decode(delta.delta.0)
                            .context("decode Qwen audio delta")?;
                        match player_handle.try_audio(pcm) {
                            Ok(()) => {}
                            Err(QueueError::Full) => anyhow::bail!("realtime playback queue full"),
                            Err(QueueError::Closed) => anyhow::bail!("realtime playback task stopped"),
                        }
                    }
                    ServerEvent::ResponseDone(done) => {
                        debug!(status = ?done.response.status, "Qwen response completed");
                        machine.transition(SessionState::Ready)?;
                        machine.transition(SessionState::ShuttingDown)?;
                        break;
                    }
                    ServerEvent::Error(error) => anyhow::bail!("Qwen realtime error: {}", error.error.message),
                    _ => {}
                }
            }
        }
    }

    let _ = sink.close().await;
    player.shutdown().await?;
    machine.transition(SessionState::Closed)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_machine_accepts_session_lifecycle() {
        let mut machine = SessionMachine::new();
        for state in [
            SessionState::Connecting,
            SessionState::Ready,
            SessionState::Capturing,
            SessionState::Responding,
            SessionState::Cancelling,
            SessionState::ShuttingDown,
            SessionState::Closed,
        ] {
            machine.transition(state).unwrap();
        }
        assert_eq!(machine.state(), SessionState::Closed);
    }

    #[test]
    fn state_machine_rejects_invalid_transition() {
        let mut machine = SessionMachine::new();
        assert_eq!(
            machine.transition(SessionState::Responding),
            Err(StateTransitionError::Invalid {
                from: SessionState::Disconnected,
                to: SessionState::Responding,
            })
        );
    }

    #[tokio::test]
    async fn upload_queue_drops_newest_without_blocking() {
        let (tx, mut rx) = mpsc::channel(1);
        let sender = BoundedAudioSender {
            tx,
            policy: BackpressurePolicy::DropNewest,
        };
        sender.try_send(vec![1]).unwrap();
        assert_eq!(sender.try_send(vec![2]), Err(QueueError::Full));
        assert!(matches!(
            rx.recv().await,
            Some(UploadCommand::Audio(bytes)) if bytes == vec![1]
        ));
    }

    #[tokio::test]
    async fn player_reports_spawn_failure() {
        let player = PcmPlayer::spawn(
            "/definitely/missing/aplay".to_string(),
            "default".to_string(),
            24_000,
        );
        assert!(player.shutdown().await.is_err());
    }

    #[tokio::test]
    async fn dropping_player_aborts_worker() {
        let player = PcmPlayer::spawn(
            "/definitely/missing/aplay".to_string(),
            "default".to_string(),
            24_000,
        );
        let handle = player.handle();
        drop(player);
        tokio::task::yield_now().await;
        assert_eq!(handle.try_audio(vec![1]), Err(QueueError::Closed));
    }

    #[tokio::test]
    async fn cancellation_handle_is_bounded_and_delivered() {
        let (tx, mut rx) = mpsc::channel(1);
        let handle = SessionHandle { tx };
        let waiter = tokio::spawn(async move { handle.cancel().await });
        let Some(SessionCommand::Cancel(ack)) = rx.recv().await else {
            panic!("expected cancellation command");
        };
        ack.send(()).unwrap();
        waiter.await.unwrap();
    }
}
