use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::Context;
use base64::Engine as _;
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
#[cfg(test)]
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};
use url::Url;

use crate::audio::record::AudioRecorder;
use crate::capture::record_utterance_streaming;
use crate::config::{timeout_duration, CaptureConfig, QwenRealtimeConfig};
use crate::qwen_realtime::{
    AudioFormat, Base64Pcm, ClientEvent, Modality, ResponseId, ServerEvent, SessionUpdate,
};

const UPLOAD_QUEUE_CAPACITY: usize = 32;
const PLAYBACK_QUEUE_CAPACITY: usize = 64;
const PLAYER_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const PLAYER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const CAPTURE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const WEBSOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const CANCEL_EVENT_TIMEOUT: Duration = Duration::from_millis(250);
const SESSION_CANCEL_TIMEOUT: Duration = Duration::from_secs(5);
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
        if self.state == to {
            return Ok(());
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaybackControl {
    Running,
    Shutdown,
}

#[derive(Clone)]
pub struct PcmPlayerHandle {
    audio_tx: mpsc::Sender<Vec<u8>>,
    control_tx: watch::Sender<PlaybackControl>,
    spawn_gate: Arc<StdMutex<PlaybackControl>>,
}

impl PcmPlayerHandle {
    fn try_audio(&self, bytes: Vec<u8>) -> Result<(), QueueError> {
        if *self.control_tx.borrow() == PlaybackControl::Shutdown {
            return Err(QueueError::Closed);
        }
        match self.audio_tx.try_send(bytes) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(QueueError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(QueueError::Closed),
        }
    }

    fn stop(&self) {
        let mut state = match self.spawn_gate.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        *state = PlaybackControl::Shutdown;
        let _ = self.control_tx.send(PlaybackControl::Shutdown);
    }
}

trait SessionPlayer {
    fn try_audio(&self, bytes: Vec<u8>) -> Result<(), QueueError>;
    fn stop(&self);
    fn shutdown(self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
}

pub struct PcmPlayer {
    handle: PcmPlayerHandle,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}

struct PlayerSetup {
    path: String,
    device: String,
    sample_rate: u32,
    setup_phase: Option<Arc<StdMutex<SetupPhase>>>,
}

impl PcmPlayer {
    fn spawn(
        path: String,
        device: String,
        sample_rate: u32,
        setup_phase: Arc<StdMutex<SetupPhase>>,
    ) -> Self {
        Self::spawn_with_setup_gate(path, device, sample_rate, Some(setup_phase), spawn_aplay)
    }

    #[cfg(test)]
    fn spawn_with<F>(path: String, device: String, sample_rate: u32, spawn: F) -> Self
    where
        F: FnOnce(&str, &str, u32) -> anyhow::Result<(Child, ChildStdin)> + Send + 'static,
    {
        Self::spawn_with_setup_gate(path, device, sample_rate, None, spawn)
    }

    fn spawn_with_setup_gate<F>(
        path: String,
        device: String,
        sample_rate: u32,
        setup_phase: Option<Arc<StdMutex<SetupPhase>>>,
        spawn: F,
    ) -> Self
    where
        F: FnOnce(&str, &str, u32) -> anyhow::Result<(Child, ChildStdin)> + Send + 'static,
    {
        let (audio_tx, audio_rx) = mpsc::channel(PLAYBACK_QUEUE_CAPACITY);
        let (control_tx, control_rx) = watch::channel(PlaybackControl::Running);
        let spawn_gate = Arc::new(StdMutex::new(PlaybackControl::Running));
        let task = tokio::spawn(run_player(
            PlayerSetup {
                path,
                device,
                sample_rate,
                setup_phase,
            },
            audio_rx,
            control_rx,
            spawn_gate.clone(),
            spawn,
        ));
        Self {
            handle: PcmPlayerHandle {
                audio_tx,
                control_tx,
                spawn_gate,
            },
            task: Some(task),
        }
    }

    #[cfg(test)]
    fn handle(&self) -> PcmPlayerHandle {
        self.handle.clone()
    }

    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.handle.stop();
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

impl SessionPlayer for PcmPlayer {
    fn try_audio(&self, bytes: Vec<u8>) -> Result<(), QueueError> {
        self.handle.try_audio(bytes)
    }

    fn stop(&self) {
        self.handle.stop();
    }

    fn shutdown(self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> {
        Box::pin(PcmPlayer::shutdown(self))
    }
}

impl Drop for PcmPlayer {
    fn drop(&mut self) {
        self.handle.stop();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct AbortOnDropTask<T> {
    task: JoinHandle<T>,
}

impl<T> AbortOnDropTask<T> {
    fn new(task: JoinHandle<T>) -> Self {
        Self { task }
    }

    fn abort(&self) {
        self.task.abort();
    }

    async fn join(&mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.task).await
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn spawn_aplay(path: &str, device: &str, sample_rate: u32) -> anyhow::Result<(Child, ChildStdin)> {
    let mut child = Command::new(path)
        .args(aplay_args(device, sample_rate))
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

fn aplay_args(device: &str, sample_rate: u32) -> Vec<String> {
    vec![
        "-q".to_string(),
        "-D".to_string(),
        device.to_string(),
        "-t".to_string(),
        "raw".to_string(),
        "-f".to_string(),
        "S16_LE".to_string(),
        "-r".to_string(),
        sample_rate.to_string(),
        "-c".to_string(),
        "1".to_string(),
    ]
}

async fn stop_child(child: &mut Child) {
    let _ = child.start_kill();
    let _ = timeout(PLAYER_SHUTDOWN_TIMEOUT, child.wait()).await;
}

fn construct_player_if_running<T>(
    spawn_gate: &StdMutex<PlaybackControl>,
    setup_phase: Option<&StdMutex<SetupPhase>>,
    construct: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<Option<T>> {
    let state = spawn_gate
        .lock()
        .map_err(|_| anyhow::anyhow!("realtime player spawn gate was poisoned"))?;
    if *state == PlaybackControl::Shutdown {
        return Ok(None);
    }
    let session_phase = setup_phase
        .map(|phase| {
            phase
                .lock()
                .map_err(|_| anyhow::anyhow!("native Qwen session setup gate was poisoned"))
        })
        .transpose()?;
    if session_phase
        .as_deref()
        .is_some_and(|phase| *phase == SetupPhase::Cancelled)
    {
        return Ok(None);
    }
    construct().map(Some)
}

async fn run_player<F>(
    setup: PlayerSetup,
    mut audio_rx: mpsc::Receiver<Vec<u8>>,
    mut control_rx: watch::Receiver<PlaybackControl>,
    spawn_gate: Arc<StdMutex<PlaybackControl>>,
    spawn: F,
) -> anyhow::Result<()>
where
    F: FnOnce(&str, &str, u32) -> anyhow::Result<(Child, ChildStdin)>,
{
    let Some((mut child, mut stdin)) =
        construct_player_if_running(&spawn_gate, setup.setup_phase.as_deref(), || {
            spawn(&setup.path, &setup.device, setup.sample_rate)
        })?
    else {
        return Ok(());
    };
    'player: loop {
        tokio::select! {
            biased;
            changed = control_rx.changed() => {
                if changed.is_err() || *control_rx.borrow() == PlaybackControl::Shutdown {
                    break;
                }
            }
            audio = audio_rx.recv() => {
                let Some(bytes) = audio else { break; };
                tokio::select! {
                    biased;
                    changed = control_rx.changed() => {
                        if changed.is_err() || *control_rx.borrow() == PlaybackControl::Shutdown {
                            break 'player;
                        }
                    }
                    result = timeout(PLAYER_WRITE_TIMEOUT, stdin.write_all(&bytes)) => {
                        result
                            .context("realtime player write timed out")?
                            .context("write realtime PCM")?;
                    }
                }
            }
        }
    }
    drop(stdin);
    stop_child(&mut child).await;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionTerminalOutcome {
    Clean,
    Failed(Arc<str>),
    ForcedTimeout,
}

impl SessionTerminalOutcome {
    fn from_worker_result(result: anyhow::Result<()>) -> Self {
        match result {
            Ok(()) => Self::Clean,
            Err(err) => Self::Failed(format!("{err:#}").into()),
        }
    }

    fn into_result(self) -> anyhow::Result<()> {
        match self {
            Self::Clean => Ok(()),
            Self::Failed(message) => Err(anyhow::anyhow!(message.to_string())),
            Self::ForcedTimeout => {
                anyhow::bail!("native Qwen session teardown exceeded its cancellation deadline")
            }
        }
    }
}

#[derive(Clone)]
pub struct SessionHandle {
    cancel_tx: watch::Sender<bool>,
    terminal_rx: watch::Receiver<Option<SessionTerminalOutcome>>,
    setup_phase: Arc<StdMutex<SetupPhase>>,
}

struct PreparedSessionControl {
    id: u64,
    handle: SessionHandle,
    cancel_rx: watch::Receiver<bool>,
    terminal_tx: watch::Sender<Option<SessionTerminalOutcome>>,
    terminal_rx: watch::Receiver<Option<SessionTerminalOutcome>>,
    setup_phase: Arc<StdMutex<SetupPhase>>,
}

pub struct QwenSessionTurn {
    service: QwenVoiceService,
    idle_timeout: Duration,
    control: PreparedSessionControl,
}

impl QwenSessionTurn {
    pub fn handle(&self) -> SessionHandle {
        self.control.handle.clone()
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let config = self.service.config.clone();
        let capture = self.service.capture.clone();
        self.service
            .run_prepared_supervised_session(
                self.control,
                move |cancel_rx, setup_phase| {
                    run_realtime_session(config, capture, self.idle_timeout, cancel_rx, setup_phase)
                },
                SESSION_CANCEL_TIMEOUT,
            )
            .await
    }
}

impl SessionHandle {
    fn request_cancel(&self) {
        if let Ok(mut phase) = self.setup_phase.lock() {
            *phase = SetupPhase::Cancelled;
        }
        let _ = self.cancel_tx.send(true);
    }

    fn is_completed(&self) -> bool {
        self.terminal_rx.borrow().is_some()
    }

    pub async fn cancel(&self) -> SessionTerminalOutcome {
        self.request_cancel();
        let mut terminal_rx = self.terminal_rx.clone();
        loop {
            if let Some(outcome) = terminal_rx.borrow().clone() {
                return outcome;
            }
            if terminal_rx.changed().await.is_err() {
                return SessionTerminalOutcome::Failed(
                    "native Qwen session supervisor stopped without an outcome".into(),
                );
            }
        }
    }
}

#[derive(Clone)]
struct ActiveSession {
    id: u64,
    handle: SessionHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupPhase {
    Connecting,
    ResourcesOwned,
    Cancelled,
}

fn setup_if_running<T>(
    setup_phase: &StdMutex<SetupPhase>,
    cancel_rx: &watch::Receiver<bool>,
    setup: impl FnOnce() -> T,
) -> anyhow::Result<Option<T>> {
    let mut phase = setup_phase
        .lock()
        .map_err(|_| anyhow::anyhow!("native Qwen session setup gate was poisoned"))?;
    if *phase == SetupPhase::Cancelled || *cancel_rx.borrow() {
        *phase = SetupPhase::Cancelled;
        return Ok(None);
    }
    let resources = setup();
    *phase = SetupPhase::ResourcesOwned;
    Ok(Some(resources))
}

async fn clear_active_if_owned(active: &Mutex<Option<ActiveSession>>, id: u64) {
    let mut slot = active.lock().await;
    if slot.as_ref().is_some_and(|session| session.id == id) {
        slot.take();
    }
}

struct SessionWaiterGuard {
    handle: SessionHandle,
    armed: bool,
}

impl SessionWaiterGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SessionWaiterGuard {
    fn drop(&mut self) {
        if self.armed {
            self.handle.request_cancel();
        }
    }
}

#[derive(Clone)]
pub struct QwenVoiceService {
    config: QwenRealtimeConfig,
    capture: CaptureConfig,
    active: Arc<Mutex<Option<ActiveSession>>>,
    next_session_id: Arc<AtomicU64>,
}

impl QwenVoiceService {
    pub fn new(config: QwenRealtimeConfig, capture: CaptureConfig) -> Self {
        Self {
            config,
            capture,
            active: Arc::new(Mutex::new(None)),
            next_session_id: Arc::new(AtomicU64::new(1)),
        }
    }

    #[cfg(test)]
    async fn cancel_active(&self) -> Option<SessionTerminalOutcome> {
        let active = self.active.lock().await.clone();
        if let Some(active) = active {
            Some(active.handle.cancel().await)
        } else {
            None
        }
    }

    pub fn prepare_session(&self, idle_timeout: Duration) -> anyhow::Result<QwenSessionTurn> {
        validate_pcm_contract(&self.config, &self.capture)?;
        Ok(QwenSessionTurn {
            service: self.clone(),
            idle_timeout,
            control: self.prepare_session_control(),
        })
    }

    fn prepare_session_control(&self) -> PreparedSessionControl {
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let setup_phase = Arc::new(StdMutex::new(SetupPhase::Connecting));
        let handle = SessionHandle {
            cancel_tx,
            terminal_rx: terminal_rx.clone(),
            setup_phase: setup_phase.clone(),
        };
        PreparedSessionControl {
            id,
            handle,
            cancel_rx,
            terminal_tx,
            terminal_rx,
            setup_phase,
        }
    }

    #[cfg(test)]
    async fn run_supervised_session<F, Fut>(&self, worker_factory: F) -> anyhow::Result<()>
    where
        F: FnOnce(watch::Receiver<bool>, Arc<StdMutex<SetupPhase>>) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.run_supervised_session_with_cancel_timeout(worker_factory, SESSION_CANCEL_TIMEOUT)
            .await
    }

    #[cfg(test)]
    async fn run_supervised_session_with_cancel_timeout<F, Fut>(
        &self,
        worker_factory: F,
        cancel_timeout: Duration,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(watch::Receiver<bool>, Arc<StdMutex<SetupPhase>>) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let control = self.prepare_session_control();
        self.run_prepared_supervised_session(control, worker_factory, cancel_timeout)
            .await
    }

    async fn run_prepared_supervised_session<F, Fut>(
        &self,
        control: PreparedSessionControl,
        worker_factory: F,
        cancel_timeout: Duration,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(watch::Receiver<bool>, Arc<StdMutex<SetupPhase>>) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let PreparedSessionControl {
            id,
            handle,
            cancel_rx,
            terminal_tx,
            terminal_rx,
            setup_phase,
        } = control;
        let mut waiter_guard = SessionWaiterGuard {
            handle: handle.clone(),
            armed: true,
        };

        {
            let mut active = self.active.lock().await;
            if active
                .as_ref()
                .is_some_and(|session| !session.handle.is_completed())
            {
                anyhow::bail!("native Qwen voice session is already active");
            }
            *active = Some(ActiveSession {
                id,
                handle: handle.clone(),
            });
        }

        let active = self.active.clone();
        let mut supervisor_cancel_rx = cancel_rx.clone();
        let mut worker = tokio::spawn(worker_factory(cancel_rx, setup_phase));
        tokio::spawn(async move {
            let cancel_requested = async {
                loop {
                    if *supervisor_cancel_rx.borrow() {
                        break;
                    }
                    if supervisor_cancel_rx.changed().await.is_err() {
                        std::future::pending::<()>().await;
                    }
                }
            };
            tokio::pin!(cancel_requested);
            let outcome = tokio::select! {
                result = &mut worker => match result {
                    Ok(result) => SessionTerminalOutcome::from_worker_result(result),
                    Err(err) => SessionTerminalOutcome::Failed(
                        format!("native Qwen session worker panicked: {err}").into(),
                    ),
                },
                _ = &mut cancel_requested => {
                    match timeout(cancel_timeout, &mut worker).await {
                        Ok(Ok(result)) => SessionTerminalOutcome::from_worker_result(result),
                        Ok(Err(err)) => SessionTerminalOutcome::Failed(
                            format!("native Qwen session worker panicked: {err}").into(),
                        ),
                        Err(_) => {
                            // Every production operation below the worker has its own deadline.
                            // Crossing this aggregate deadline changes the shared outcome, but
                            // resource ownership remains with the worker until all child tasks
                            // have been joined. Active-session replacement stays blocked meanwhile.
                            let _ = (&mut worker).await;
                            SessionTerminalOutcome::ForcedTimeout
                        }
                    }
                }
            };
            clear_active_if_owned(&active, id).await;
            let _ = terminal_tx.send(Some(outcome));
        });

        let mut terminal_rx = terminal_rx;
        let outcome = loop {
            if let Some(outcome) = terminal_rx.borrow().clone() {
                break outcome;
            }
            terminal_rx
                .changed()
                .await
                .context("native Qwen session supervisor stopped unexpectedly")?;
        };
        waiter_guard.disarm();
        outcome.into_result()
    }
}

enum Cancellable<T> {
    Completed(T),
    Cancelled,
}

async fn cancellation_aware<F, T>(
    cancel_rx: &mut watch::Receiver<bool>,
    deadline: Duration,
    operation_name: &'static str,
    operation: F,
) -> anyhow::Result<Cancellable<T>>
where
    F: Future<Output = anyhow::Result<T>>,
{
    if *cancel_rx.borrow() {
        return Ok(Cancellable::Cancelled);
    }
    tokio::pin!(operation);
    loop {
        tokio::select! {
            biased;
            changed = cancel_rx.changed() => {
                if changed.is_err() || *cancel_rx.borrow() {
                    return Ok(Cancellable::Cancelled);
                }
            }
            result = timeout(deadline, &mut operation) => {
                return Ok(Cancellable::Completed(
                    result.with_context(|| format!("{operation_name} timed out"))??,
                ));
            }
        }
    }
}

async fn connect(
    config: &QwenRealtimeConfig,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<
    Cancellable<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
> {
    let mut url = Url::parse(&config.url).context("parse Qwen realtime URL")?;
    url.query_pairs_mut().append_pair("model", &config.model);
    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", config.api_key))
            .context("invalid Qwen API key header")?,
    );
    cancellation_aware(
        cancel_rx,
        timeout_duration(config.connect_timeout_s),
        "Qwen realtime connect",
        async {
            connect_async(request)
                .await
                .context("connect Qwen realtime websocket")
                .map(|(ws, _)| ws)
        },
    )
    .await
}

async fn send_event_bounded<S, E>(
    sink: &mut S,
    event: &ClientEvent,
    event_timeout: Duration,
) -> anyhow::Result<()>
where
    S: futures::Sink<Message, Error = E> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let text = serde_json::to_string(event)?;
    timeout(event_timeout, sink.send(Message::Text(text.into())))
        .await
        .context("send Qwen realtime event timed out")?
        .context("send Qwen realtime event")
}

async fn send_event_cancellable<S, E>(
    sink: &mut S,
    event: &ClientEvent,
    event_timeout: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<Cancellable<()>>
where
    S: futures::Sink<Message, Error = E> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    cancellation_aware(
        cancel_rx,
        event_timeout,
        "send Qwen realtime event",
        async {
            let text = serde_json::to_string(event)?;
            sink.send(Message::Text(text.into()))
                .await
                .context("send Qwen realtime event")
        },
    )
    .await
}

async fn run_realtime_session(
    config: QwenRealtimeConfig,
    capture: CaptureConfig,
    idle_timeout: Duration,
    mut cancel_rx: watch::Receiver<bool>,
    setup_phase: Arc<StdMutex<SetupPhase>>,
) -> anyhow::Result<()> {
    let mut machine = SessionMachine::new();
    machine.transition(SessionState::Connecting)?;
    let mut last_error = None;
    for attempt in 0..=RECONNECT_ATTEMPTS {
        if *cancel_rx.borrow() {
            machine.transition(SessionState::ShuttingDown)?;
            machine.transition(SessionState::Closed)?;
            return Ok(());
        }
        match run_connected_session(
            &config,
            &capture,
            idle_timeout,
            &mut cancel_rx,
            &setup_phase,
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
    cancel_rx: &mut watch::Receiver<bool>,
    setup_phase: &Arc<StdMutex<SetupPhase>>,
    machine: &mut SessionMachine,
) -> anyhow::Result<()> {
    validate_pcm_contract(config, capture)?;
    let ws = match connect(config, cancel_rx).await? {
        Cancellable::Completed(ws) => ws,
        Cancellable::Cancelled => {
            machine.transition(SessionState::ShuttingDown)?;
            machine.transition(SessionState::Closed)?;
            return Ok(());
        }
    };
    let (mut sink, mut stream) = ws.split();
    let event_timeout = timeout_duration(config.event_timeout_s);
    let update_result = send_event_cancellable(
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
        event_timeout,
        cancel_rx,
    )
    .await;
    match update_result {
        Ok(Cancellable::Completed(())) => {}
        Ok(Cancellable::Cancelled) => {
            machine.transition(SessionState::ShuttingDown)?;
            let close_result = timeout(WEBSOCKET_CLOSE_TIMEOUT, sink.close()).await;
            if close_result.is_err() {
                anyhow::bail!("timed out closing cancelled Qwen realtime websocket setup");
            }
            machine.transition(SessionState::Closed)?;
            return Ok(());
        }
        Err(err) => {
            let _ = timeout(WEBSOCKET_CLOSE_TIMEOUT, sink.close()).await;
            return Err(err);
        }
    }
    if *cancel_rx.borrow() {
        machine.transition(SessionState::ShuttingDown)?;
        let _ = timeout(WEBSOCKET_CLOSE_TIMEOUT, sink.close()).await;
        machine.transition(SessionState::Closed)?;
        return Ok(());
    }
    machine.transition(SessionState::Ready)?;

    let resources = setup_if_running(setup_phase, cancel_rx, || {
        let player = PcmPlayer::spawn(
            "/usr/bin/aplay".to_string(),
            "default".to_string(),
            config.output_sample_rate.0,
            setup_phase.clone(),
        );
        let (audio_tx, audio_rx) = mpsc::channel(UPLOAD_QUEUE_CAPACITY);
        let bounded = BoundedAudioSender {
            tx: audio_tx.clone(),
            policy: BackpressurePolicy::DropNewest,
        };
        let clear_tx = audio_tx.clone();
        let commit_tx = audio_tx;
        debug!(?bounded.policy, "Qwen upload backpressure policy");
        let capture_config = capture.clone();
        let capture_task = AbortOnDropTask::new(tokio::spawn(async move {
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
        }));
        (player, audio_rx, capture_task)
    })?;
    let Some((player, mut audio_rx, mut capture_task)) = resources else {
        machine.transition(SessionState::ShuttingDown)?;
        let _ = timeout(WEBSOCKET_CLOSE_TIMEOUT, sink.close()).await;
        machine.transition(SessionState::Closed)?;
        return Ok(());
    };
    machine.transition(SessionState::Capturing)?;

    run_connected_resources_with_timeout(
        &mut sink,
        &mut stream,
        player,
        &mut audio_rx,
        &mut capture_task,
        (cancel_rx, event_timeout, machine),
    )
    .await
}

async fn run_connected_resources_with_timeout<S, St, SinkError, StreamError, P, CaptureOutput>(
    sink: &mut S,
    stream: &mut St,
    player: P,
    audio_rx: &mut mpsc::Receiver<UploadCommand>,
    capture_task: &mut AbortOnDropTask<anyhow::Result<CaptureOutput>>,
    control: (&mut watch::Receiver<bool>, Duration, &mut SessionMachine),
) -> anyhow::Result<()>
where
    S: futures::Sink<Message, Error = SinkError> + Unpin,
    St: futures::Stream<Item = Result<Message, StreamError>> + Unpin,
    SinkError: std::error::Error + Send + Sync + 'static,
    StreamError: std::error::Error + Send + Sync + 'static,
    P: SessionPlayer,
{
    let (cancel_rx, event_timeout, machine) = control;
    let mut active_response: Option<ResponseId> = None;
    let mut capture_done = false;
    let mut cancel_observed = *cancel_rx.borrow();

    let session_result: anyhow::Result<()> = async {
        loop {
            if cancel_observed || *cancel_rx.borrow() {
                cancel_observed = true;
                break;
            }
            tokio::select! {
                biased;
                changed = cancel_rx.changed() => {
                    if changed.is_err() || *cancel_rx.borrow() {
                        cancel_observed = true;
                        break;
                    }
                }
                Some(upload) = audio_rx.recv() => {
                    let send_result = match upload {
                        UploadCommand::Audio(bytes) => {
                            validate_s16le_frame(&bytes, "Qwen capture PCM")?;
                            send_event_cancellable(sink, &ClientEvent::InputAudioBufferAppend {
                                event_id: None,
                                audio: Base64Pcm::new(base64::engine::general_purpose::STANDARD.encode(bytes)),
                            }, event_timeout, cancel_rx).await
                            .context("append Qwen capture PCM")?
                        }
                        UploadCommand::Clear => {
                            send_event_cancellable(
                                sink,
                                &ClientEvent::InputAudioBufferClear { event_id: None },
                                event_timeout,
                                cancel_rx,
                            ).await.context("clear Qwen input audio buffer")?
                        }
                        UploadCommand::Commit => {
                            let committed = send_event_cancellable(
                                sink,
                                &ClientEvent::InputAudioBufferCommit { event_id: None },
                                event_timeout,
                                cancel_rx,
                            ).await.context("commit Qwen input audio buffer")?;
                            if matches!(committed, Cancellable::Cancelled) {
                                Cancellable::Cancelled
                            } else {
                                let created = send_event_cancellable(
                                    sink,
                                    &ClientEvent::ResponseCreate { event_id: None, response: None },
                                    event_timeout,
                                    cancel_rx,
                                ).await.context("create Qwen response")?;
                                if matches!(created, Cancellable::Completed(())) {
                                    machine.transition(SessionState::Responding)?;
                                }
                                created
                            }
                        }
                    };
                    if matches!(send_result, Cancellable::Cancelled) {
                        cancel_observed = true;
                        break;
                    }
                }
                captured = capture_task.join(), if !capture_done => {
                    capture_done = true;
                    captured.context("Qwen capture task panicked")??;
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
                            validate_s16le_frame(&pcm, "Qwen playback PCM")?;
                            match player.try_audio(pcm) {
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
        if cancel_observed {
            machine.transition(SessionState::Cancelling)?;
            player.stop();
            let cancel_result = send_event_bounded(
                sink,
                &ClientEvent::ResponseCancel {
                    event_id: None,
                    response_id: active_response.take(),
                },
                event_timeout.min(CANCEL_EVENT_TIMEOUT),
            )
            .await
            .context("cancel Qwen response");
            machine.transition(SessionState::ShuttingDown)?;
            cancel_result?;
        }
        Ok(())
    }.await;

    let capture_result = if !capture_done {
        capture_task.abort();
        match timeout(CAPTURE_SHUTDOWN_TIMEOUT, capture_task.join()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) if err.is_cancelled() => Ok(()),
            Ok(Err(err)) => Err(anyhow::Error::new(err).context("join Qwen capture task")),
            Err(_) => match capture_task.join().await {
                Ok(_) => Err(anyhow::anyhow!("timed out joining Qwen capture task")),
                Err(err) if err.is_cancelled() => {
                    Err(anyhow::anyhow!("timed out joining Qwen capture task"))
                }
                Err(err) => Err(anyhow::Error::new(err)
                    .context("join Qwen capture task after shutdown timeout")),
            },
        }
    } else {
        Ok(())
    };
    let recorder_result = match timeout(
        CAPTURE_SHUTDOWN_TIMEOUT,
        AudioRecorder::instance().stop_recording(),
    )
    .await
    {
        Ok(result) => result.map_err(|err| anyhow::anyhow!("stop Qwen audio capture: {err}")),
        Err(_) => Err(anyhow::anyhow!("timed out stopping Qwen audio capture")),
    };
    let close_result = match timeout(WEBSOCKET_CLOSE_TIMEOUT, sink.close()).await {
        Ok(result) => result.context("close Qwen realtime websocket"),
        Err(_) => Err(anyhow::anyhow!("timed out closing Qwen realtime websocket")),
    };
    let player_result = player.shutdown().await;
    let teardown_result = capture_result
        .and(recorder_result)
        .and(close_result)
        .and(player_result);
    if matches!(machine.state(), SessionState::Cancelling) {
        machine.transition(SessionState::ShuttingDown).ok();
    }
    if matches!(machine.state(), SessionState::ShuttingDown) && teardown_result.is_ok() {
        machine.transition(SessionState::Closed)?;
    }
    session_result.and(teardown_result)
}

#[cfg(test)]
async fn run_connected_resources<S, St, SinkError, StreamError, P, CaptureOutput>(
    sink: &mut S,
    stream: &mut St,
    player: P,
    audio_rx: &mut mpsc::Receiver<UploadCommand>,
    capture_task: &mut AbortOnDropTask<anyhow::Result<CaptureOutput>>,
    cancel_rx: &mut watch::Receiver<bool>,
    machine: &mut SessionMachine,
) -> anyhow::Result<()>
where
    S: futures::Sink<Message, Error = SinkError> + Unpin,
    St: futures::Stream<Item = Result<Message, StreamError>> + Unpin,
    SinkError: std::error::Error + Send + Sync + 'static,
    StreamError: std::error::Error + Send + Sync + 'static,
    P: SessionPlayer,
{
    run_connected_resources_with_timeout(
        sink,
        stream,
        player,
        audio_rx,
        capture_task,
        (cancel_rx, Duration::from_secs(1), machine),
    )
    .await
}

fn validate_pcm_contract(
    config: &QwenRealtimeConfig,
    capture: &CaptureConfig,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        capture.sample_rate == 16_000 && capture.channels == 1 && capture.bits_per_sample == 16,
        "native Qwen capture must be 16 kHz mono S16_LE; got {} Hz, {} channel(s), {} bits",
        capture.sample_rate,
        capture.channels,
        capture.bits_per_sample
    );
    anyhow::ensure!(
        config.output_sample_rate.0 == 24_000,
        "native Qwen playback must be 24 kHz mono S16_LE"
    );
    Ok(())
}

fn validate_s16le_frame(bytes: &[u8], label: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        bytes.len().is_multiple_of(std::mem::size_of::<i16>()),
        "{label} byte length {} is not aligned to S16_LE samples",
        bytes.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::task::{Context as TaskContext, Poll};

    #[derive(Debug, Clone)]
    struct MockWsError(&'static str);

    impl std::fmt::Display for MockWsError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl std::error::Error for MockWsError {}

    #[derive(Default)]
    struct MockSinkState {
        closed: bool,
        dropped: bool,
        fail_send: bool,
        fail_after: Option<usize>,
        block_next_send: bool,
        block_all_sends: bool,
        blocked_send_polled: Option<Arc<tokio::sync::Notify>>,
        sent: Vec<Message>,
        ledger: Option<Arc<StdMutex<Vec<&'static str>>>>,
    }

    struct MockSink {
        state: Arc<StdMutex<MockSinkState>>,
    }

    impl futures::Sink<Message> for MockSink {
        type Error = MockWsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().unwrap();
            if state.block_all_sends {
                if let Some(polled) = &state.blocked_send_polled {
                    polled.notify_one();
                }
                return Poll::Pending;
            }
            if state.block_next_send {
                state.block_next_send = false;
                if let Some(polled) = &state.blocked_send_polled {
                    polled.notify_one();
                }
                return Poll::Pending;
            }
            if state.fail_send
                || state
                    .fail_after
                    .is_some_and(|limit| state.sent.len() >= limit)
            {
                Poll::Ready(Err(MockWsError("send failed")))
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.state.lock().unwrap().sent.push(item);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            if let Some(ledger) = &state.ledger {
                ledger.lock().unwrap().push("websocket_close");
            }
            Poll::Ready(Ok(()))
        }
    }

    impl Drop for MockSink {
        fn drop(&mut self) {
            self.state.lock().unwrap().dropped = true;
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum MockPlayback {
        Accept,
        Full,
        Closed,
    }

    #[derive(Default)]
    struct MockPlayerState {
        cleared: AtomicBool,
        shutdown: AtomicBool,
        dropped: AtomicBool,
        stop_notified: tokio::sync::Notify,
        shutdown_started: tokio::sync::Notify,
        played: StdMutex<Vec<Vec<u8>>>,
        ledger: Option<Arc<StdMutex<Vec<&'static str>>>>,
    }

    struct MockPlayer {
        state: Arc<MockPlayerState>,
        playback: MockPlayback,
        shutdown_gate: Option<tokio::sync::oneshot::Receiver<()>>,
    }

    impl Drop for MockPlayer {
        fn drop(&mut self) {
            self.state.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl SessionPlayer for MockPlayer {
        fn try_audio(&self, bytes: Vec<u8>) -> Result<(), QueueError> {
            match self.playback {
                MockPlayback::Accept => {
                    self.state.played.lock().unwrap().push(bytes);
                    Ok(())
                }
                MockPlayback::Full => Err(QueueError::Full),
                MockPlayback::Closed => Err(QueueError::Closed),
            }
        }

        fn stop(&self) {
            self.state.cleared.store(true, Ordering::SeqCst);
            if let Some(ledger) = &self.state.ledger {
                ledger.lock().unwrap().push("player_stop");
            }
            self.state.stop_notified.notify_one();
        }

        fn shutdown(mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> {
            Box::pin(async move {
                self.state.shutdown_started.notify_one();
                if let Some(gate) = self.shutdown_gate.take() {
                    let _ = gate.await;
                }
                if let Some(ledger) = &self.state.ledger {
                    ledger.lock().unwrap().push("player_shutdown");
                }
                self.state.shutdown.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    struct CaptureDrop(Arc<AtomicBool>);

    impl Drop for CaptureDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct LedgerCaptureDrop(Arc<StdMutex<Vec<&'static str>>>);

    impl Drop for LedgerCaptureDrop {
        fn drop(&mut self) {
            self.0.lock().unwrap().push("capture_join_vpm_end");
        }
    }

    fn capturing_machine() -> SessionMachine {
        let mut machine = SessionMachine::new();
        machine.transition(SessionState::Connecting).unwrap();
        machine.transition(SessionState::Ready).unwrap();
        machine.transition(SessionState::Capturing).unwrap();
        machine
    }

    fn pending_capture(dropped: Arc<AtomicBool>) -> AbortOnDropTask<anyhow::Result<()>> {
        let drop_signal = CaptureDrop(dropped);
        AbortOnDropTask::new(tokio::spawn(async move {
            let _drop = drop_signal;
            std::future::pending::<()>().await;
            Ok(())
        }))
    }

    fn audio_delta() -> Message {
        Message::Text(
            r#"{"type":"response.audio.delta","response_id":"r","item_id":"i","output_index":0,"content_index":0,"delta":"AAE="}"#
                .into(),
        )
    }

    fn audio_delta_with(delta: &str) -> Message {
        Message::Text(
            format!(
                r#"{{"type":"response.audio.delta","response_id":"r","item_id":"i","output_index":0,"content_index":0,"delta":"{delta}"}}"#
            )
            .into(),
        )
    }

    fn session_handle(completed: bool) -> (SessionHandle, watch::Receiver<bool>) {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let terminal = completed.then_some(SessionTerminalOutcome::Clean);
        let (_terminal_tx, terminal_rx) = watch::channel(terminal);
        (
            SessionHandle {
                cancel_tx,
                terminal_rx,
                setup_phase: Arc::new(StdMutex::new(SetupPhase::Connecting)),
            },
            cancel_rx,
        )
    }

    async fn run_stream_case(
        item: Result<Message, MockWsError>,
        playback: MockPlayback,
    ) -> (
        anyhow::Result<()>,
        Arc<StdMutex<MockSinkState>>,
        Arc<MockPlayerState>,
        Arc<AtomicBool>,
    ) {
        let sink_state = Arc::new(StdMutex::new(MockSinkState::default()));
        let mut sink = MockSink {
            state: sink_state.clone(),
        };
        let mut stream = futures::stream::iter([item]);
        let player_state = Arc::new(MockPlayerState::default());
        let player = MockPlayer {
            state: player_state.clone(),
            playback,
            shutdown_gate: None,
        };
        let (_audio_tx, mut audio_rx) = mpsc::channel(1);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let mut capture = pending_capture(capture_dropped.clone());
        let mut machine = capturing_machine();
        let result = run_connected_resources(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
        )
        .await;
        (result, sink_state, player_state, capture_dropped)
    }

    fn assert_full_teardown(
        sink: &Arc<StdMutex<MockSinkState>>,
        player: &Arc<MockPlayerState>,
        capture_dropped: &Arc<AtomicBool>,
    ) {
        assert!(sink.lock().unwrap().closed, "websocket sink was not closed");
        assert!(player.shutdown.load(Ordering::SeqCst));
        assert!(capture_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn receive_decode_and_server_errors_run_full_teardown() {
        let cases = [
            Err(MockWsError("receive failed")),
            Ok(Message::Text("{".into())),
            Ok(Message::Text(
                r#"{"type":"error","error":{"message":"server failed"}}"#.into(),
            )),
        ];
        for item in cases {
            let (result, sink, player, capture) = run_stream_case(item, MockPlayback::Accept).await;
            assert!(result.is_err());
            assert_full_teardown(&sink, &player, &capture);
        }
    }

    #[tokio::test]
    async fn response_done_closes_the_real_resource_path_cleanly() {
        let sink_state = Arc::new(StdMutex::new(MockSinkState::default()));
        let mut sink = MockSink {
            state: sink_state.clone(),
        };
        let done =
            Message::Text(include_str!("../tests/fixtures/qwen_server_response_done.json").into());
        let mut stream = futures::stream::iter([Ok::<Message, MockWsError>(done)]);
        let player_state = Arc::new(MockPlayerState::default());
        let player = MockPlayer {
            state: player_state.clone(),
            playback: MockPlayback::Accept,
            shutdown_gate: None,
        };
        let (audio_tx, mut audio_rx) = mpsc::channel(1);
        audio_tx.send(UploadCommand::Commit).await.unwrap();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let mut capture = pending_capture(capture_dropped.clone());
        let mut machine = capturing_machine();

        run_connected_resources(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
        )
        .await
        .unwrap();

        assert_full_teardown(&sink_state, &player_state, &capture_dropped);
        assert_eq!(sink_state.lock().unwrap().sent.len(), 2);
    }

    #[tokio::test]
    async fn websocket_close_invalid_base64_and_odd_playback_run_full_teardown() {
        let cases = [
            Ok(Message::Close(None)),
            Ok(audio_delta_with("%%%")),
            Ok(audio_delta_with("AA==")),
        ];
        for item in cases {
            let (result, sink, player, capture) = run_stream_case(item, MockPlayback::Accept).await;
            assert!(result.is_err());
            assert_full_teardown(&sink, &player, &capture);
        }
    }

    #[tokio::test]
    async fn websocket_eof_runs_full_teardown() {
        let sink_state = Arc::new(StdMutex::new(MockSinkState::default()));
        let mut sink = MockSink {
            state: sink_state.clone(),
        };
        let mut stream = futures::stream::empty::<Result<Message, MockWsError>>();
        let player_state = Arc::new(MockPlayerState::default());
        let player = MockPlayer {
            state: player_state.clone(),
            playback: MockPlayback::Accept,
            shutdown_gate: None,
        };
        let (_audio_tx, mut audio_rx) = mpsc::channel(1);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let mut capture = pending_capture(capture_dropped.clone());
        let mut machine = capturing_machine();

        let result = run_connected_resources(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
        )
        .await;

        assert!(result.unwrap_err().to_string().contains("websocket closed"));
        assert_full_teardown(&sink_state, &player_state, &capture_dropped);
    }

    #[tokio::test]
    async fn upload_send_failure_runs_full_teardown() {
        let sink_state = Arc::new(StdMutex::new(MockSinkState {
            fail_send: true,
            ..MockSinkState::default()
        }));
        let mut sink = MockSink {
            state: sink_state.clone(),
        };
        let mut stream = futures::stream::pending::<Result<Message, MockWsError>>();
        let player_state = Arc::new(MockPlayerState::default());
        let player = MockPlayer {
            state: player_state.clone(),
            playback: MockPlayback::Accept,
            shutdown_gate: None,
        };
        let (audio_tx, mut audio_rx) = mpsc::channel(1);
        audio_tx
            .send(UploadCommand::Audio(vec![0, 0]))
            .await
            .unwrap();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let mut capture = pending_capture(capture_dropped.clone());
        let mut machine = capturing_machine();

        let result = run_connected_resources(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
        )
        .await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("append Qwen capture PCM"));
        assert_full_teardown(&sink_state, &player_state, &capture_dropped);
    }

    #[tokio::test]
    async fn clear_commit_and_response_create_failures_run_full_teardown() {
        for (upload, fail_after, expected) in [
            (UploadCommand::Clear, None, "clear Qwen input audio buffer"),
            (
                UploadCommand::Commit,
                None,
                "commit Qwen input audio buffer",
            ),
            (UploadCommand::Commit, Some(1), "create Qwen response"),
        ] {
            let sink_state = Arc::new(StdMutex::new(MockSinkState {
                fail_send: fail_after.is_none(),
                fail_after,
                ..MockSinkState::default()
            }));
            let mut sink = MockSink {
                state: sink_state.clone(),
            };
            let mut stream = futures::stream::pending::<Result<Message, MockWsError>>();
            let player_state = Arc::new(MockPlayerState::default());
            let player = MockPlayer {
                state: player_state.clone(),
                playback: MockPlayback::Accept,
                shutdown_gate: None,
            };
            let (audio_tx, mut audio_rx) = mpsc::channel(1);
            audio_tx.send(upload).await.unwrap();
            let (_cancel_tx, mut cancel_rx) = watch::channel(false);
            let capture_dropped = Arc::new(AtomicBool::new(false));
            let mut capture = pending_capture(capture_dropped.clone());
            let mut machine = capturing_machine();

            let result = run_connected_resources(
                &mut sink,
                &mut stream,
                player,
                &mut audio_rx,
                &mut capture,
                &mut cancel_rx,
                &mut machine,
            )
            .await;

            assert!(result.unwrap_err().to_string().contains(expected));
            assert_full_teardown(&sink_state, &player_state, &capture_dropped);
        }
    }

    #[tokio::test]
    async fn cancellation_preempts_a_blocked_append_send() {
        let send_polled = Arc::new(tokio::sync::Notify::new());
        let sink_state = Arc::new(StdMutex::new(MockSinkState {
            block_next_send: true,
            blocked_send_polled: Some(send_polled.clone()),
            ..MockSinkState::default()
        }));
        let mut sink = MockSink {
            state: sink_state.clone(),
        };
        let mut stream = futures::stream::pending::<Result<Message, MockWsError>>();
        let player_state = Arc::new(MockPlayerState::default());
        let player = MockPlayer {
            state: player_state.clone(),
            playback: MockPlayback::Accept,
            shutdown_gate: None,
        };
        let (audio_tx, mut audio_rx) = mpsc::channel(1);
        audio_tx
            .send(UploadCommand::Audio(vec![0, 0]))
            .await
            .unwrap();
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let cancel = tokio::spawn(async move {
            send_polled.notified().await;
            cancel_tx.send(true).unwrap();
        });
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let mut capture = pending_capture(capture_dropped.clone());
        let mut machine = capturing_machine();

        run_connected_resources(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
        )
        .await
        .unwrap();
        cancel.await.unwrap();

        assert_full_teardown(&sink_state, &player_state, &capture_dropped);
        assert!(player_state.cleared.load(Ordering::SeqCst));
        assert_eq!(sink_state.lock().unwrap().sent.len(), 1);
    }

    #[tokio::test]
    async fn connect_and_session_update_operations_are_cancel_preemptible() {
        let (connect_started_tx, connect_started_rx) = oneshot::channel();
        let (connect_cancel_tx, mut connect_cancel_rx) = watch::channel(false);
        let cancel_connect = tokio::spawn(async move {
            connect_started_rx.await.unwrap();
            connect_cancel_tx.send(true).unwrap();
        });
        let connect_result = cancellation_aware(
            &mut connect_cancel_rx,
            Duration::from_secs(30),
            "controlled connect",
            async move {
                connect_started_tx.send(()).unwrap();
                std::future::pending::<()>().await;
                Ok::<(), anyhow::Error>(())
            },
        )
        .await
        .unwrap();
        cancel_connect.await.unwrap();
        assert!(matches!(connect_result, Cancellable::Cancelled));

        let send_polled = Arc::new(tokio::sync::Notify::new());
        let mut sink = MockSink {
            state: Arc::new(StdMutex::new(MockSinkState {
                block_next_send: true,
                blocked_send_polled: Some(send_polled.clone()),
                ..MockSinkState::default()
            })),
        };
        let (update_cancel_tx, mut update_cancel_rx) = watch::channel(false);
        let cancel_update = tokio::spawn(async move {
            send_polled.notified().await;
            update_cancel_tx.send(true).unwrap();
        });
        let update_result = send_event_cancellable(
            &mut sink,
            &ClientEvent::InputAudioBufferClear { event_id: None },
            Duration::from_secs(30),
            &mut update_cancel_rx,
        )
        .await
        .unwrap();
        cancel_update.await.unwrap();
        assert!(matches!(update_result, Cancellable::Cancelled));
    }

    #[test]
    fn cancellation_gate_prevents_player_or_capture_creation_after_request() {
        let (handle, cancel_rx) = session_handle(false);
        handle.request_cancel();
        let constructions = AtomicU64::new(0);
        let resources = setup_if_running(&handle.setup_phase, &cancel_rx, || {
            constructions.fetch_add(2, Ordering::SeqCst);
        })
        .unwrap();
        assert!(resources.is_none());
        assert_eq!(constructions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn playback_backpressure_and_closed_run_full_teardown() {
        for playback in [MockPlayback::Full, MockPlayback::Closed] {
            let (result, sink, player, capture) =
                run_stream_case(Ok(audio_delta()), playback).await;
            assert!(result.is_err(), "playback mode {playback:?} must fail");
            assert_full_teardown(&sink, &player, &capture);
        }
    }

    #[tokio::test]
    async fn connected_path_preserves_exact_uploaded_and_played_pcm_bytes() {
        let uploaded = vec![0x34, 0x12, 0x78, 0x56];
        let sink_state = Arc::new(StdMutex::new(MockSinkState {
            fail_after: Some(1),
            ..MockSinkState::default()
        }));
        let mut sink = MockSink {
            state: sink_state.clone(),
        };
        let mut stream = futures::stream::pending::<Result<Message, MockWsError>>();
        let player = MockPlayer {
            state: Arc::new(MockPlayerState::default()),
            playback: MockPlayback::Accept,
            shutdown_gate: None,
        };
        let (audio_tx, mut audio_rx) = mpsc::channel(2);
        audio_tx
            .send(UploadCommand::Audio(uploaded.clone()))
            .await
            .unwrap();
        audio_tx.send(UploadCommand::Commit).await.unwrap();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let mut capture = pending_capture(Arc::new(AtomicBool::new(false)));
        let mut machine = capturing_machine();

        assert!(run_connected_resources(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
        )
        .await
        .is_err());
        let encoded = {
            let state = sink_state.lock().unwrap();
            let Message::Text(text) = &state.sent[0] else {
                panic!("expected text upload event");
            };
            let value: serde_json::Value = serde_json::from_str(text).unwrap();
            value["audio"].as_str().unwrap().to_string()
        };
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(&encoded)
                .unwrap(),
            uploaded
        );

        let (result, _, player, _) = run_stream_case(Ok(audio_delta()), MockPlayback::Accept).await;
        assert!(result.is_err());
        assert_eq!(*player.played.lock().unwrap(), [vec![0, 1]]);
    }

    #[tokio::test]
    async fn cancellation_joins_capture_and_releases_its_upload_sender() {
        let mut sink = MockSink {
            state: Arc::new(StdMutex::new(MockSinkState::default())),
        };
        let mut stream = futures::stream::pending::<Result<Message, MockWsError>>();
        let player = MockPlayer {
            state: Arc::new(MockPlayerState::default()),
            playback: MockPlayback::Accept,
            shutdown_gate: None,
        };
        let (audio_tx, mut audio_rx) = mpsc::channel(1);
        let weak = audio_tx.downgrade();
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = capture_dropped.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let mut capture = AbortOnDropTask::new(tokio::spawn(async move {
            let _sender = audio_tx;
            let _drop = CaptureDrop(task_dropped);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
            Ok::<(), anyhow::Error>(())
        }));
        started_rx.await.unwrap();
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        cancel_tx.send(true).unwrap();
        let mut machine = capturing_machine();

        run_connected_resources(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
        )
        .await
        .unwrap();
        assert!(capture_dropped.load(Ordering::SeqCst));
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn cancellation_follows_ordered_teardown_ledger() {
        let ledger = Arc::new(StdMutex::new(Vec::new()));
        let mut sink = MockSink {
            state: Arc::new(StdMutex::new(MockSinkState {
                ledger: Some(ledger.clone()),
                ..MockSinkState::default()
            })),
        };
        let mut stream = futures::stream::pending::<Result<Message, MockWsError>>();
        let player = MockPlayer {
            state: Arc::new(MockPlayerState {
                ledger: Some(ledger.clone()),
                ..MockPlayerState::default()
            }),
            playback: MockPlayback::Accept,
            shutdown_gate: None,
        };
        let (_audio_tx, mut audio_rx) = mpsc::channel(1);
        let (started_tx, started_rx) = oneshot::channel();
        let task_ledger = ledger.clone();
        let mut capture = AbortOnDropTask::new(tokio::spawn(async move {
            let _end = LedgerCaptureDrop(task_ledger);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
            Ok::<(), anyhow::Error>(())
        }));
        started_rx.await.unwrap();
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        cancel_tx.send(true).unwrap();
        let mut machine = capturing_machine();

        run_connected_resources(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
        )
        .await
        .unwrap();
        assert_eq!(
            *ledger.lock().unwrap(),
            [
                "player_stop",
                "capture_join_vpm_end",
                "websocket_close",
                "player_shutdown"
            ]
        );
    }

    #[tokio::test]
    async fn capture_join_failure_runs_full_teardown() {
        let sink_state = Arc::new(StdMutex::new(MockSinkState::default()));
        let mut sink = MockSink {
            state: sink_state.clone(),
        };
        let mut stream = futures::stream::pending::<Result<Message, MockWsError>>();
        let player_state = Arc::new(MockPlayerState::default());
        let player = MockPlayer {
            state: player_state.clone(),
            playback: MockPlayback::Accept,
            shutdown_gate: None,
        };
        let (_audio_tx, mut audio_rx) = mpsc::channel(1);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let mut capture = AbortOnDropTask::new(tokio::spawn(async move {
            panic!("controlled capture panic");
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        }));
        let mut machine = capturing_machine();

        let result = run_connected_resources(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
        )
        .await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("capture task panicked"));
        assert!(sink_state.lock().unwrap().closed);
        assert!(player_state.shutdown.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancel_send_failure_completes_all_waiters_only_after_teardown() {
        let sink_state = Arc::new(StdMutex::new(MockSinkState {
            fail_send: true,
            ..MockSinkState::default()
        }));
        let player_state = Arc::new(MockPlayerState::default());
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let handle = SessionHandle {
            cancel_tx,
            terminal_rx,
            setup_phase: Arc::new(StdMutex::new(SetupPhase::Connecting)),
        };

        let first = tokio::spawn({
            let handle = handle.clone();
            async move { handle.cancel().await }
        });
        let second = tokio::spawn({
            let handle = handle.clone();
            async move { handle.cancel().await }
        });

        let task_sink_state = sink_state.clone();
        let task_player_state = player_state.clone();
        let task_capture_dropped = capture_dropped.clone();
        let task = tokio::spawn(async move {
            let mut sink = MockSink {
                state: task_sink_state,
            };
            let mut stream = futures::stream::pending::<Result<Message, MockWsError>>();
            let player = MockPlayer {
                state: task_player_state,
                playback: MockPlayback::Accept,
                shutdown_gate: Some(shutdown_rx),
            };
            let (_audio_tx, mut audio_rx) = mpsc::channel(1);
            let mut capture = pending_capture(task_capture_dropped);
            let mut machine = capturing_machine();
            let result = run_connected_resources(
                &mut sink,
                &mut stream,
                player,
                &mut audio_rx,
                &mut capture,
                &mut cancel_rx,
                &mut machine,
            )
            .await;
            let outcome = SessionTerminalOutcome::from_worker_result(result);
            let _ = terminal_tx.send(Some(outcome.clone()));
            outcome
        });

        player_state.stop_notified.notified().await;
        player_state.shutdown_started.notified().await;
        assert!(!handle.is_completed());
        assert!(!first.is_finished());
        assert!(!second.is_finished());
        assert!(sink_state.lock().unwrap().closed);
        assert!(capture_dropped.load(Ordering::SeqCst));

        shutdown_tx.send(()).unwrap();
        let task_outcome = task.await.unwrap();
        assert!(matches!(task_outcome, SessionTerminalOutcome::Failed(_)));
        assert_eq!(first.await.unwrap(), task_outcome);
        assert_eq!(second.await.unwrap(), task_outcome);
        assert!(player_state.shutdown.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn supervisor_terminal_ledger_orders_recorder_outcome_and_active_clear() {
        let service = Arc::new(QwenVoiceService::new(
            QwenRealtimeConfig::default(),
            CaptureConfig::default(),
        ));
        let ledger = Arc::new(StdMutex::new(Vec::new()));
        let started = Arc::new(tokio::sync::Notify::new());
        let owner = tokio::spawn({
            let service = service.clone();
            let ledger = ledger.clone();
            let started = started.clone();
            async move {
                service
                    .run_supervised_session(move |mut cancel_rx, _setup_phase| async move {
                        started.notify_one();
                        cancel_rx.wait_for(|cancelled| *cancelled).await.unwrap();
                        let mut entries = ledger.lock().unwrap();
                        entries.push("capture_join_vpm_end");
                        entries.push("recorder_stop");
                        entries.push("websocket_close");
                        entries.push("player_shutdown");
                        Ok(())
                    })
                    .await
            }
        });

        started.notified().await;
        assert_eq!(
            service.cancel_active().await,
            Some(SessionTerminalOutcome::Clean)
        );
        assert!(service.active.lock().await.is_none());
        ledger.lock().unwrap().push("terminal_outcome_active_clear");
        owner.await.unwrap().unwrap();
        assert_eq!(
            *ledger.lock().unwrap(),
            [
                "capture_join_vpm_end",
                "recorder_stop",
                "websocket_close",
                "player_shutdown",
                "terminal_outcome_active_clear"
            ]
        );
    }

    #[tokio::test]
    async fn prepared_turn_closes_cancel_snapshot_registration_race() {
        let service = Arc::new(QwenVoiceService::new(
            QwenRealtimeConfig::default(),
            CaptureConfig::default(),
        ));
        let control = service.prepare_session_control();
        let turn_handle = control.handle.clone();
        let live_players = Arc::new(AtomicU64::new(0));
        let max_live_players = Arc::new(AtomicU64::new(0));
        let replacement_starts = Arc::new(AtomicU64::new(0));
        let registered = Arc::new(tokio::sync::Notify::new());
        let cleanup_started = Arc::new(tokio::sync::Notify::new());
        let (registration_tx, registration_rx) = oneshot::channel();
        let (cleanup_tx, cleanup_rx) = oneshot::channel();

        let owner = tokio::spawn({
            let service = service.clone();
            let live_players = live_players.clone();
            let max_live_players = max_live_players.clone();
            let registered = registered.clone();
            let cleanup_started = cleanup_started.clone();
            async move {
                registration_rx.await.unwrap();
                service
                    .run_prepared_supervised_session(
                        control,
                        move |mut cancel_rx, _setup_phase| async move {
                            let live = live_players.fetch_add(1, Ordering::SeqCst) + 1;
                            max_live_players.fetch_max(live, Ordering::SeqCst);
                            registered.notify_one();
                            cancel_rx.wait_for(|cancelled| *cancelled).await.unwrap();
                            cleanup_started.notify_one();
                            cleanup_rx.await.unwrap();
                            live_players.fetch_sub(1, Ordering::SeqCst);
                            Ok(())
                        },
                        SESSION_CANCEL_TIMEOUT,
                    )
                    .await
            }
        });

        assert_eq!(service.cancel_active().await, None);
        registration_tx.send(()).unwrap();
        registered.notified().await;
        assert_eq!(live_players.load(Ordering::SeqCst), 1);

        owner.abort();
        cleanup_started.notified().await;
        let replacement = tokio::spawn({
            let service = service.clone();
            let live_players = live_players.clone();
            let max_live_players = max_live_players.clone();
            let replacement_starts = replacement_starts.clone();
            async move {
                assert_eq!(turn_handle.cancel().await, SessionTerminalOutcome::Clean);
                service
                    .run_supervised_session(move |_cancel_rx, _setup_phase| async move {
                        replacement_starts.fetch_add(1, Ordering::SeqCst);
                        let live = live_players.fetch_add(1, Ordering::SeqCst) + 1;
                        max_live_players.fetch_max(live, Ordering::SeqCst);
                        live_players.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            }
        });

        assert!(!replacement.is_finished());
        assert_eq!(replacement_starts.load(Ordering::SeqCst), 0);
        assert_eq!(live_players.load(Ordering::SeqCst), 1);
        cleanup_tx.send(()).unwrap();

        assert!(owner.await.unwrap_err().is_cancelled());
        replacement.await.unwrap().unwrap();
        assert_eq!(replacement_starts.load(Ordering::SeqCst), 1);
        assert_eq!(live_players.load(Ordering::SeqCst), 0);
        assert_eq!(max_live_players.load(Ordering::SeqCst), 1);
        assert!(service.active.lock().await.is_none());
    }

    #[tokio::test]
    async fn actual_service_owner_abort_retains_teardown_and_blocks_replacement() {
        let service = Arc::new(QwenVoiceService::new(
            QwenRealtimeConfig::default(),
            CaptureConfig::default(),
        ));
        let live_players = Arc::new(AtomicU64::new(0));
        let max_live_players = Arc::new(AtomicU64::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let cleanup_started = Arc::new(tokio::sync::Notify::new());
        let (cleanup_tx, cleanup_rx) = oneshot::channel();

        let owner = tokio::spawn({
            let service = service.clone();
            let live_players = live_players.clone();
            let max_live_players = max_live_players.clone();
            let started = started.clone();
            let cleanup_started = cleanup_started.clone();
            async move {
                service
                    .run_supervised_session(move |mut cancel_rx, _setup_phase| async move {
                        let live = live_players.fetch_add(1, Ordering::SeqCst) + 1;
                        max_live_players.fetch_max(live, Ordering::SeqCst);
                        started.notify_one();
                        cancel_rx.wait_for(|cancelled| *cancelled).await.unwrap();
                        cleanup_started.notify_one();
                        cleanup_rx.await.unwrap();
                        live_players.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            }
        });

        started.notified().await;
        owner.abort();
        cleanup_started.notified().await;
        let replacement = service
            .run_supervised_session(|_cancel_rx, _setup_phase| async { Ok(()) })
            .await
            .unwrap_err();
        assert!(replacement.to_string().contains("already active"));
        assert_eq!(live_players.load(Ordering::SeqCst), 1);

        cleanup_tx.send(()).unwrap();
        assert_eq!(
            service.cancel_active().await,
            Some(SessionTerminalOutcome::Clean)
        );
        assert!(owner.await.unwrap_err().is_cancelled());
        assert_eq!(live_players.load(Ordering::SeqCst), 0);

        let replacement_live = live_players.clone();
        let replacement_max = max_live_players.clone();
        service
            .run_supervised_session(move |_cancel_rx, _setup_phase| async move {
                let live = replacement_live.fetch_add(1, Ordering::SeqCst) + 1;
                replacement_max.fetch_max(live, Ordering::SeqCst);
                replacement_live.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(max_live_players.load(Ordering::SeqCst), 1);
        assert!(service.active.lock().await.is_none());
    }

    #[tokio::test]
    async fn forced_timeout_waits_for_real_resource_teardown_before_replacement() {
        let service = Arc::new(QwenVoiceService::new(
            QwenRealtimeConfig::default(),
            CaptureConfig::default(),
        ));
        let sink_state = Arc::new(StdMutex::new(MockSinkState {
            block_all_sends: true,
            ..MockSinkState::default()
        }));
        let player_state = Arc::new(MockPlayerState::default());
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let live_players = Arc::new(AtomicU64::new(0));
        let max_live_players = Arc::new(AtomicU64::new(0));
        let resources_started = Arc::new(tokio::sync::Notify::new());
        let (player_shutdown_tx, player_shutdown_rx) = oneshot::channel();

        let owner = tokio::spawn({
            let service = service.clone();
            let sink_state = sink_state.clone();
            let player_state = player_state.clone();
            let capture_dropped = capture_dropped.clone();
            let live_players = live_players.clone();
            let max_live_players = max_live_players.clone();
            let resources_started = resources_started.clone();
            async move {
                service
                    .run_supervised_session_with_cancel_timeout(
                        move |mut cancel_rx, _setup_phase| async move {
                            let live = live_players.fetch_add(1, Ordering::SeqCst) + 1;
                            max_live_players.fetch_max(live, Ordering::SeqCst);
                            let mut sink = MockSink { state: sink_state };
                            let mut stream =
                                futures::stream::pending::<Result<Message, MockWsError>>();
                            let player = MockPlayer {
                                state: player_state,
                                playback: MockPlayback::Accept,
                                shutdown_gate: Some(player_shutdown_rx),
                            };
                            let (audio_tx, mut audio_rx) = mpsc::channel(1);
                            let weak_upload = audio_tx.downgrade();
                            let task_dropped = capture_dropped;
                            let mut capture = AbortOnDropTask::new(tokio::spawn(async move {
                                let _sender = audio_tx;
                                let _drop = CaptureDrop(task_dropped);
                                std::future::pending::<()>().await;
                                Ok::<(), anyhow::Error>(())
                            }));
                            let mut machine = capturing_machine();
                            resources_started.notify_one();
                            let result = run_connected_resources_with_timeout(
                                &mut sink,
                                &mut stream,
                                player,
                                &mut audio_rx,
                                &mut capture,
                                (&mut cancel_rx, Duration::from_secs(30), &mut machine),
                            )
                            .await;
                            assert!(weak_upload.upgrade().is_none());
                            live_players.fetch_sub(1, Ordering::SeqCst);
                            result
                        },
                        Duration::ZERO,
                    )
                    .await
            }
        });

        resources_started.notified().await;
        let cancel = tokio::spawn({
            let service = service.clone();
            async move { service.cancel_active().await }
        });
        player_state.stop_notified.notified().await;
        assert!(!cancel.is_finished());
        assert!(!owner.is_finished());
        assert!(service.active.lock().await.is_some());
        assert!(!capture_dropped.load(Ordering::SeqCst));

        let replacement = service
            .run_supervised_session(|_cancel_rx, _setup_phase| async { Ok(()) })
            .await
            .unwrap_err();
        assert!(replacement.to_string().contains("already active"));
        assert_eq!(live_players.load(Ordering::SeqCst), 1);

        player_shutdown_tx.send(()).unwrap();
        assert_eq!(
            cancel.await.unwrap(),
            Some(SessionTerminalOutcome::ForcedTimeout)
        );
        assert!(owner.await.unwrap().is_err());
        assert!(sink_state.lock().unwrap().closed);
        assert!(player_state.shutdown.load(Ordering::SeqCst));
        assert!(capture_dropped.load(Ordering::SeqCst));
        assert_eq!(live_players.load(Ordering::SeqCst), 0);
        assert!(service.active.lock().await.is_none());

        let replacement_live = live_players.clone();
        let replacement_max = max_live_players.clone();
        service
            .run_supervised_session(move |_cancel_rx, _setup_phase| async move {
                let live = replacement_live.fetch_add(1, Ordering::SeqCst) + 1;
                replacement_max.fetch_max(live, Ordering::SeqCst);
                replacement_live.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(max_live_players.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropping_outer_waiter_requests_cancel_without_aborting_supervisor() {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let handle = SessionHandle {
            cancel_tx,
            terminal_rx,
            setup_phase: Arc::new(StdMutex::new(SetupPhase::Connecting)),
        };
        let (cleanup_started_tx, cleanup_started_rx) = oneshot::channel();
        let (cleanup_gate_tx, cleanup_gate_rx) = oneshot::channel();
        let (waiter_started_tx, waiter_started_rx) = oneshot::channel();

        let supervisor = tokio::spawn(async move {
            cancel_rx.wait_for(|cancelled| *cancelled).await.unwrap();
            cleanup_started_tx.send(()).unwrap();
            cleanup_gate_rx.await.unwrap();
            let _ = terminal_tx.send(Some(SessionTerminalOutcome::Clean));
        });

        let outer = tokio::spawn({
            let handle = handle.clone();
            async move {
                let _guard = SessionWaiterGuard {
                    handle,
                    armed: true,
                };
                waiter_started_tx.send(()).unwrap();
                std::future::pending::<()>().await;
            }
        });
        waiter_started_rx.await.unwrap();
        outer.abort();
        assert!(outer.await.unwrap_err().is_cancelled());
        cleanup_started_rx.await.unwrap();

        let duplicate = tokio::spawn({
            let handle = handle.clone();
            async move { handle.cancel().await }
        });
        assert!(!duplicate.is_finished());
        cleanup_gate_tx.send(()).unwrap();
        supervisor.await.unwrap();
        assert_eq!(duplicate.await.unwrap(), SessionTerminalOutcome::Clean);
        assert!(handle.is_completed());
    }

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
        let player = PcmPlayer::spawn_with(
            "/definitely/missing/aplay".to_string(),
            "default".to_string(),
            24_000,
            spawn_aplay,
        );
        tokio::task::yield_now().await;
        assert!(player.shutdown().await.is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stopping_player_before_first_poll_prevents_process_construction() {
        let construction_attempts = Arc::new(AtomicU64::new(0));
        let observed_attempts = construction_attempts.clone();
        let player = PcmPlayer::spawn_with(
            "/unused/aplay".to_string(),
            "default".to_string(),
            24_000,
            move |_path, _device, _sample_rate| {
                observed_attempts.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("process construction must not run after stop")
            },
        );

        player.handle().stop();
        player.shutdown().await.unwrap();

        assert_eq!(construction_attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn accepted_session_cancellation_prevents_deferred_process_construction() {
        let (handle, cancel_rx) = session_handle(false);
        let construction_attempts = Arc::new(AtomicU64::new(0));
        let observed_attempts = construction_attempts.clone();
        let player = setup_if_running(&handle.setup_phase, &cancel_rx, || {
            PcmPlayer::spawn_with_setup_gate(
                "/unused/aplay".to_string(),
                "default".to_string(),
                24_000,
                Some(handle.setup_phase.clone()),
                move |_path, _device, _sample_rate| {
                    observed_attempts.fetch_add(1, Ordering::SeqCst);
                    anyhow::bail!("process construction must not run after session cancellation")
                },
            )
        })
        .unwrap()
        .expect("session resources should be created before cancellation");

        assert_eq!(
            *handle.setup_phase.lock().unwrap(),
            SetupPhase::ResourcesOwned
        );
        handle.request_cancel();
        assert!(*cancel_rx.borrow());
        player.shutdown().await.unwrap();

        assert_eq!(construction_attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn production_player_spawn_gate_keeps_max_live_processes_at_one() {
        let live_processes = Arc::new(AtomicU64::new(0));
        let max_live_processes = Arc::new(AtomicU64::new(0));
        let (spawned_tx, spawned_rx) = oneshot::channel();
        let first_live = live_processes.clone();
        let first_max = max_live_processes.clone();
        let first = PcmPlayer::spawn_with(
            "/unused/aplay".to_string(),
            "default".to_string(),
            24_000,
            move |_path, _device, _sample_rate| {
                let mut child = Command::new("/bin/cat")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn()?;
                let stdin = child.stdin.take().context("test player stdin missing")?;
                let live = first_live.fetch_add(1, Ordering::SeqCst) + 1;
                first_max.fetch_max(live, Ordering::SeqCst);
                let _ = spawned_tx.send(());
                Ok((child, stdin))
            },
        );
        spawned_rx.await.unwrap();

        let second_live = live_processes.clone();
        let second_max = max_live_processes.clone();
        let second = PcmPlayer::spawn_with(
            "/unused/aplay".to_string(),
            "default".to_string(),
            24_000,
            move |_path, _device, _sample_rate| {
                let live = second_live.fetch_add(1, Ordering::SeqCst) + 1;
                second_max.fetch_max(live, Ordering::SeqCst);
                anyhow::bail!("cancelled replacement must not construct a process")
            },
        );
        second.handle().stop();
        second.shutdown().await.unwrap();

        assert_eq!(live_processes.load(Ordering::SeqCst), 1);
        assert_eq!(max_live_processes.load(Ordering::SeqCst), 1);
        first.shutdown().await.unwrap();
        live_processes.fetch_sub(1, Ordering::SeqCst);
        assert_eq!(live_processes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dropping_player_aborts_worker() {
        let player = PcmPlayer::spawn_with(
            "/definitely/missing/aplay".to_string(),
            "default".to_string(),
            24_000,
            spawn_aplay,
        );
        let handle = player.handle();
        drop(player);
        tokio::task::yield_now().await;
        assert_eq!(handle.try_audio(vec![1]), Err(QueueError::Closed));
    }

    #[tokio::test]
    async fn cancellation_handle_waits_for_shared_completion() {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let handle = SessionHandle {
            cancel_tx,
            terminal_rx,
            setup_phase: Arc::new(StdMutex::new(SetupPhase::Connecting)),
        };
        let waiter = tokio::spawn({
            let handle = handle.clone();
            async move { handle.cancel().await }
        });
        cancel_rx.wait_for(|cancelled| *cancelled).await.unwrap();
        assert!(!waiter.is_finished());
        terminal_tx
            .send(Some(SessionTerminalOutcome::Clean))
            .unwrap();
        assert_eq!(waiter.await.unwrap(), SessionTerminalOutcome::Clean);
    }

    #[test]
    fn native_qwen_rejects_non_pcm16_capture_contract() {
        let config = QwenRealtimeConfig::default();
        let capture = CaptureConfig {
            channels: 2,
            ..CaptureConfig::default()
        };
        let err = validate_pcm_contract(&config, &capture).unwrap_err();
        assert!(err.to_string().contains("16 kHz mono S16_LE"));
    }

    #[test]
    fn pcm_frames_require_complete_s16le_samples() {
        validate_s16le_frame(&[0, 1], "test PCM").unwrap();
        let err = validate_s16le_frame(&[0], "test PCM").unwrap_err();
        assert!(err.to_string().contains("not aligned"));
    }

    #[test]
    fn aplay_uses_exact_native_qwen_pcm_contract() {
        assert_eq!(
            aplay_args("default", 24_000),
            ["-q", "-D", "default", "-t", "raw", "-f", "S16_LE", "-r", "24000", "-c", "1"]
        );
    }

    #[tokio::test]
    async fn cancellation_after_completion_coalesces_each_typed_outcome() {
        for outcome in [
            SessionTerminalOutcome::Clean,
            SessionTerminalOutcome::Failed("controlled teardown failure".into()),
            SessionTerminalOutcome::ForcedTimeout,
        ] {
            let (cancel_tx, _cancel_rx) = watch::channel(false);
            let (_terminal_tx, terminal_rx) = watch::channel(Some(outcome.clone()));
            let handle = SessionHandle {
                cancel_tx,
                terminal_rx,
                setup_phase: Arc::new(StdMutex::new(SetupPhase::Connecting)),
            };
            assert_eq!(handle.cancel().await, outcome);
            assert_eq!(handle.cancel().await, outcome);
        }
    }

    #[tokio::test]
    async fn stale_generation_cannot_clear_new_active_session() {
        let (old_handle, _old_cancel) = session_handle(true);
        let (new_handle, _new_cancel) = session_handle(false);
        let active = Mutex::new(Some(ActiveSession {
            id: 2,
            handle: new_handle,
        }));

        clear_active_if_owned(&active, 1).await;
        assert_eq!(
            active.lock().await.as_ref().map(|session| session.id),
            Some(2)
        );
        clear_active_if_owned(&active, 2).await;
        assert!(active.lock().await.is_none());
        assert!(old_handle.is_completed());
    }
}
