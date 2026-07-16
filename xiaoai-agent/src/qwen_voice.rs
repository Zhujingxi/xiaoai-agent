use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::Context;
use base64::Engine as _;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};
use rig_core::tool::server::ToolServerHandle;
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, timeout, Instant};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};
use url::Url;

use crate::agent::SPEAKER_AGENT_INSTRUCTIONS;
use crate::audio::record::AudioRecorder;
use crate::capture::record_utterance_streaming;
use crate::config::{timeout_duration, CaptureConfig, QwenRealtimeConfig};
use crate::mcp::{NativeMcpCallError, NativeMcpClient};
use crate::qwen_realtime::{
    AudioFormat, Base64Pcm, CallId, ClientEvent, ConversationItem, FunctionCallOutput,
    FunctionDefinition, Modality, ResponseId, ResponseOutputItem, ResponseStatus, ServerEvent,
    SessionUpdate, ToolDefinition,
};

const UPLOAD_QUEUE_CAPACITY: usize = 32;
const PLAYBACK_QUEUE_CAPACITY: usize = 64;
const PLAYER_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const PLAYER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const CAPTURE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const WEBSOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const CANCEL_EVENT_TIMEOUT: Duration = Duration::from_millis(250);
const SESSION_CANCEL_TIMEOUT: Duration = Duration::from_secs(5);
const NATIVE_MCP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const GENERIC_TOOL_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const GENERIC_TOOL_COOPERATIVE_CANCEL_GRACE: Duration = Duration::from_millis(250);
const RECONNECT_ATTEMPTS: usize = 2;
const MAX_CALL_ID_BYTES: usize = 256;
const MAX_TOOL_NAME_BYTES: usize = 256;
const MAX_TOOL_ARGUMENT_BYTES: usize = 64 * 1024;
const MAX_TOOL_OUTPUT_BYTES: usize = 64 * 1024;

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
        let tools = NativeToolRuntime::load_with_native_mcp(
            &config,
            self.service.tool_server.clone(),
            self.service.native_mcp.clone(),
        )
        .await?;
        self.service
            .run_prepared_supervised_session(
                self.control,
                move |cancel_rx, setup_phase| {
                    run_realtime_session(
                        config,
                        capture,
                        tools,
                        self.idle_timeout,
                        cancel_rx,
                        setup_phase,
                    )
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
    tool_server: ToolServerHandle,
    native_mcp: Option<NativeMcpClient>,
    active: Arc<Mutex<Option<ActiveSession>>>,
    next_session_id: Arc<AtomicU64>,
}

impl QwenVoiceService {
    pub fn new(
        config: QwenRealtimeConfig,
        capture: CaptureConfig,
        tool_server: ToolServerHandle,
    ) -> Self {
        Self {
            config,
            capture,
            tool_server,
            native_mcp: None,
            active: Arc::new(Mutex::new(None)),
            next_session_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn with_native_mcp(mut self, native_mcp: Option<NativeMcpClient>) -> Self {
        self.native_mcp = native_mcp;
        self
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

#[derive(Clone)]
struct NativeToolRuntime {
    server: ToolServerHandle,
    native_mcp: Option<NativeMcpClient>,
    definitions: Vec<ToolDefinition>,
    call_timeout: Duration,
    max_calls: usize,
    max_iterations: usize,
    effects_observed: Arc<AtomicBool>,
    generic_calls: Arc<GenericToolState>,
}

#[derive(Default)]
struct GenericToolState {
    active_calls: AtomicU64,
    idle: tokio::sync::Notify,
    fail_closed: AtomicBool,
}

struct ActiveGenericToolCall(Arc<GenericToolState>);

impl Drop for ActiveGenericToolCall {
    fn drop(&mut self) {
        self.0.active_calls.fetch_sub(1, Ordering::SeqCst);
        self.0.idle.notify_waiters();
    }
}

enum GenericToolResult {
    Completed(Result<String, String>),
    Cancelled,
}

struct GenericToolCall {
    cancel_tx: Option<watch::Sender<bool>>,
    result_rx: oneshot::Receiver<GenericToolResult>,
    state: Arc<GenericToolState>,
}

impl GenericToolCall {
    async fn wait(mut self, deadline: Duration) -> anyhow::Result<String> {
        match timeout(deadline, &mut self.result_rx).await {
            Ok(Ok(GenericToolResult::Completed(result))) => {
                self.cancel_tx.take();
                result.map_err(anyhow::Error::msg)
            }
            Ok(Ok(GenericToolResult::Cancelled)) => {
                self.cancel_tx.take();
                anyhow::bail!("generic tool call was cancelled")
            }
            Ok(Err(_)) => anyhow::bail!("generic tool supervisor stopped unexpectedly"),
            Err(_) => {
                self.state.fail_closed.store(true, Ordering::SeqCst);
                if let Some(cancel_tx) = self.cancel_tx.take() {
                    let _ = cancel_tx.send(true);
                }
                anyhow::ensure!(
                    timeout(GENERIC_TOOL_CLEANUP_TIMEOUT, &mut self.result_rx)
                        .await
                        .is_ok(),
                    "timed out cleaning up generic tool call"
                );
                anyhow::bail!(
                    "generic tool deadline exceeded; turn failed closed after cancellation"
                )
            }
        }
    }
}

impl Drop for GenericToolCall {
    fn drop(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            let _ = cancel_tx.send(true);
        }
    }
}

fn normalize_qwen_tool_schema(schema: &mut Value) -> anyhow::Result<()> {
    match schema {
        Value::Object(object) => {
            let normalized_type = match object.get("type") {
                Some(Value::Array(types)) => {
                    anyhow::ensure!(
                        types.iter().all(Value::is_string),
                        "tool schema type union contains a non-string value"
                    );
                    let concrete_types = types
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|value| *value != "null")
                        .collect::<HashSet<_>>();
                    anyhow::ensure!(
                        concrete_types.len() == 1,
                        "tool schema type union must contain exactly one non-null type"
                    );
                    concrete_types
                        .into_iter()
                        .next()
                        .map(|value| Value::String(value.to_string()))
                }
                _ => None,
            };
            if let Some(normalized_type) = normalized_type {
                object.insert("type".to_string(), normalized_type);
            }
            for value in object.values_mut() {
                normalize_qwen_tool_schema(value)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_qwen_tool_schema(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

impl NativeToolRuntime {
    #[cfg(test)]
    async fn load(config: &QwenRealtimeConfig, server: ToolServerHandle) -> anyhow::Result<Self> {
        Self::load_with_native_mcp(config, server, None).await
    }

    async fn load_with_native_mcp(
        config: &QwenRealtimeConfig,
        server: ToolServerHandle,
        native_mcp: Option<NativeMcpClient>,
    ) -> anyhow::Result<Self> {
        let definitions = server
            .get_tool_defs(None)
            .await
            .context("load native Qwen tool definitions")?
            .into_iter()
            .map(|definition| {
                anyhow::ensure!(
                    definition.parameters.is_object(),
                    "tool {} parameters must be a JSON Schema object",
                    definition.name
                );
                let mut parameters = definition.parameters;
                normalize_qwen_tool_schema(&mut parameters).with_context(|| {
                    format!(
                        "normalize tool {} schema for Qwen realtime",
                        definition.name
                    )
                })?;
                Ok(ToolDefinition::Function {
                    function: FunctionDefinition {
                        name: definition.name,
                        description: definition.description,
                        parameters,
                    },
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            server,
            native_mcp,
            definitions,
            call_timeout: timeout_duration(config.tool_timeout_s),
            max_calls: config.max_tool_calls,
            max_iterations: config.max_tool_iterations,
            effects_observed: Arc::new(AtomicBool::new(false)),
            generic_calls: Arc::new(GenericToolState::default()),
        })
    }

    fn start_generic_call(
        server: ToolServerHandle,
        state: Arc<GenericToolState>,
        name: String,
        arguments: String,
    ) -> GenericToolCall {
        state.active_calls.fetch_add(1, Ordering::SeqCst);
        let call_state = state.clone();
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let worker_cancel_rx = cancel_rx.clone();
        let (result_tx, result_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _active = ActiveGenericToolCall(state);
            let mut worker = tokio::spawn(crate::shell::with_tool_cancellation(
                worker_cancel_rx,
                async move {
                    server
                        .call_tool(&name, &arguments)
                        .await
                        .map_err(|err| err.to_string())
                },
            ));
            let result = tokio::select! {
                biased;
                changed = cancel_rx.changed() => {
                    if changed.is_err() || *cancel_rx.borrow() {
                        match timeout(GENERIC_TOOL_COOPERATIVE_CANCEL_GRACE, &mut worker).await {
                            Ok(_) => GenericToolResult::Cancelled,
                            Err(_) => {
                                worker.abort();
                                let _ = timeout(GENERIC_TOOL_CLEANUP_TIMEOUT, &mut worker).await;
                                GenericToolResult::Cancelled
                            }
                        }
                    } else {
                        match worker.await {
                            Ok(result) => GenericToolResult::Completed(result),
                            Err(err) => GenericToolResult::Completed(Err(format!(
                                "generic tool task failed: {err}"
                            ))),
                        }
                    }
                }
                result = &mut worker => match result {
                    Ok(result) => GenericToolResult::Completed(result),
                    Err(err) => GenericToolResult::Completed(Err(format!(
                        "generic tool task failed: {err}"
                    ))),
                },
            };
            let _ = result_tx.send(result);
        });
        GenericToolCall {
            cancel_tx: Some(cancel_tx),
            result_rx,
            state: call_state,
        }
    }

    fn execute(&self, call_id: CallId, name: String, arguments: String) -> ToolFuture {
        let server = self.server.clone();
        let native_mcp = self.native_mcp.clone();
        let call_timeout = self.call_timeout;
        let effects_observed = self.effects_observed.clone();
        let generic_calls = self.generic_calls.clone();
        // Start generic work eagerly when the protocol has already supplied a valid object.
        // Otherwise a fully buffered websocket can repeatedly win the session select before
        // the boxed tool future gets its first poll, starving both execution and cancellation.
        let generic_call = if matches!(
            serde_json::from_str::<Value>(&arguments),
            Ok(Value::Object(_))
        ) && native_mcp
            .as_ref()
            .is_none_or(|native_mcp| !native_mcp.has_tool(&name))
        {
            effects_observed.store(true, Ordering::SeqCst);
            Some(Self::start_generic_call(
                server,
                generic_calls,
                name.clone(),
                arguments.clone(),
            ))
        } else {
            None
        };
        async move {
            let output = match serde_json::from_str::<Value>(&arguments) {
                Ok(Value::Object(_)) => {
                    // A started MCP request may have applied side effects even when its reply is
                    // lost or times out, so transparent websocket reconnect is unsafe afterward.
                    effects_observed.store(true, Ordering::SeqCst);
                    let result = if let Some(native_mcp) =
                        native_mcp.filter(|native_mcp| native_mcp.has_tool(&name))
                    {
                        match native_mcp.call(&name, &arguments, call_timeout).await {
                            Ok(result) => Ok(result),
                            Err(NativeMcpCallError::Timeout) => {
                                return ToolExecution {
                                    call_id,
                                    output: structured_tool_error(
                                        "timeout",
                                        "tool call deadline exceeded and MCP request was cancelled",
                                    ),
                                };
                            }
                            Err(NativeMcpCallError::FailClosed) => {
                                return ToolExecution {
                                    call_id,
                                    output: structured_tool_error(
                                        "mcp_fail_closed",
                                        "MCP execution disabled after an ambiguous in-flight call",
                                    ),
                                };
                            }
                            Err(err) => Err(anyhow::anyhow!(err)),
                        }
                    } else {
                        match generic_call {
                            Some(call) => call.wait(call_timeout).await,
                            None => Err(anyhow::anyhow!("generic tool call was not initialized")),
                        }
                    };
                    match result {
                        Ok(result) if result.len() <= MAX_TOOL_OUTPUT_BYTES => {
                            structured_tool_success(&result)
                        }
                        Ok(_) => structured_tool_error(
                            "result_too_large",
                            "tool result exceeded the safety limit",
                        ),
                        Err(err) => structured_tool_error("tool_error", &err.to_string()),
                    }
                }
                Ok(_) => {
                    structured_tool_error("invalid_arguments", "arguments must be a JSON object")
                }
                Err(_) => {
                    structured_tool_error("invalid_arguments", "arguments are not valid JSON")
                }
            };
            ToolExecution { call_id, output }
        }
        .boxed()
    }

    fn effects_observed(&self) -> bool {
        self.effects_observed.load(Ordering::SeqCst)
    }

    fn generic_fail_closed(&self) -> bool {
        self.generic_calls.fail_closed.load(Ordering::SeqCst)
    }

    async fn wait_for_tool_idle(&self) -> anyhow::Result<()> {
        let generic_idle = async {
            loop {
                let notified = self.generic_calls.idle.notified();
                if self.generic_calls.active_calls.load(Ordering::SeqCst) == 0 {
                    break;
                }
                notified.await;
            }
        };
        anyhow::ensure!(
            timeout(GENERIC_TOOL_CLEANUP_TIMEOUT, generic_idle)
                .await
                .is_ok(),
            "timed out cancelling an in-flight generic tool"
        );
        if let Some(native_mcp) = &self.native_mcp {
            anyhow::ensure!(
                native_mcp.wait_for_idle(NATIVE_MCP_CLEANUP_TIMEOUT).await,
                "timed out cancelling the in-flight native MCP request"
            );
        }
        Ok(())
    }
}

fn transparent_reconnect_allowed(tools: &NativeToolRuntime) -> bool {
    !tools.effects_observed()
}

#[derive(Debug)]
struct PendingFunctionCall {
    item_id: String,
    output_index: u32,
    name: String,
    arguments: String,
}

struct ToolExecution {
    call_id: CallId,
    output: String,
}

type ToolFuture = BoxFuture<'static, ToolExecution>;

fn structured_tool_success(raw: &str) -> String {
    let result = serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()));
    json!({"ok": true, "result": result}).to_string()
}

fn structured_tool_error(kind: &str, message: &str) -> String {
    json!({
        "ok": false,
        "error": {
            "kind": kind,
            "message": message.chars().take(256).collect::<String>(),
        }
    })
    .to_string()
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
    tools: NativeToolRuntime,
    idle_timeout: Duration,
    mut cancel_rx: watch::Receiver<bool>,
    setup_phase: Arc<StdMutex<SetupPhase>>,
) -> anyhow::Result<()> {
    let mut connector = ProductionSessionConnector {
        config,
        capture,
        tools: tools.clone(),
        idle_timeout,
        setup_phase,
    };
    run_realtime_session_loop(&tools, &mut cancel_rx, &mut connector).await
}

trait RealtimeSessionConnector {
    fn run_connected<'a>(
        &'a mut self,
        cancel_rx: &'a mut watch::Receiver<bool>,
        machine: &'a mut SessionMachine,
    ) -> BoxFuture<'a, anyhow::Result<()>>;
}

struct ProductionSessionConnector {
    config: QwenRealtimeConfig,
    capture: CaptureConfig,
    tools: NativeToolRuntime,
    idle_timeout: Duration,
    setup_phase: Arc<StdMutex<SetupPhase>>,
}

impl RealtimeSessionConnector for ProductionSessionConnector {
    fn run_connected<'a>(
        &'a mut self,
        cancel_rx: &'a mut watch::Receiver<bool>,
        machine: &'a mut SessionMachine,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        run_connected_session(
            &self.config,
            &self.capture,
            &self.tools,
            self.idle_timeout,
            cancel_rx,
            &self.setup_phase,
            machine,
        )
        .boxed()
    }
}

async fn run_realtime_session_loop<C: RealtimeSessionConnector>(
    tools: &NativeToolRuntime,
    cancel_rx: &mut watch::Receiver<bool>,
    connector: &mut C,
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
        match connector.run_connected(cancel_rx, &mut machine).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                warn!(attempt, "Qwen realtime session failed: {err:#}");
                if !transparent_reconnect_allowed(tools) {
                    machine.transition(SessionState::Failed).ok();
                    return Err(err.context(
                        "transparent reconnect disabled after a native tool request started",
                    ));
                }
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
    tools: &NativeToolRuntime,
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
                turn_detection: None,
                instructions: Some(SPEAKER_AGENT_INSTRUCTIONS.to_string()),
                tools: tools.definitions.clone(),
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
        Some(tools.clone()),
    )
    .await
}

async fn send_tool_execution<S, E>(
    sink: &mut S,
    execution: ToolExecution,
    event_timeout: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<Cancellable<()>>
where
    S: futures::Sink<Message, Error = E> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    send_event_cancellable(
        sink,
        &ClientEvent::ConversationItemCreate {
            event_id: None,
            item: ConversationItem::FunctionCallOutput {
                call_id: execution.call_id,
                output: FunctionCallOutput(execution.output),
            },
        },
        event_timeout,
        cancel_rx,
    )
    .await
    .context("send Qwen function_call_output")
}

async fn continue_after_tools<S, E>(
    sink: &mut S,
    event_timeout: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<Cancellable<()>>
where
    S: futures::Sink<Message, Error = E> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    send_event_cancellable(
        sink,
        &ClientEvent::ResponseCreate {
            event_id: None,
            response: None,
        },
        event_timeout,
        cancel_rx,
    )
    .await
    .context("continue Qwen response after tools")
}

async fn run_connected_resources_with_timeout<S, St, SinkError, StreamError, P, CaptureOutput>(
    sink: &mut S,
    stream: &mut St,
    player: P,
    audio_rx: &mut mpsc::Receiver<UploadCommand>,
    capture_task: &mut AbortOnDropTask<anyhow::Result<CaptureOutput>>,
    control: (&mut watch::Receiver<bool>, Duration, &mut SessionMachine),
    tools: Option<NativeToolRuntime>,
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
    let mut response_deadline: Option<Instant> = None;
    let mut capture_done = false;
    let mut cancel_observed = *cancel_rx.borrow();
    let mut response_calls = HashMap::<CallId, PendingFunctionCall>::new();
    // A call ID is single-use for the whole connected turn, not merely within one
    // response iteration. The call-count limit also bounds this ledger.
    let mut seen_call_ids = HashSet::<CallId>::new();
    let mut pending_tools = FuturesUnordered::<ToolFuture>::new();
    let mut deferred_message: Option<Message> = None;
    let mut waiting_for_tools = false;
    let mut tool_iterations = 0usize;
    let mut tool_calls = 0usize;

    let session_result: anyhow::Result<()> = async {
        loop {
            if cancel_observed || *cancel_rx.borrow() {
                cancel_observed = true;
                break;
            }
            let tools_pending = !pending_tools.is_empty();
            tokio::select! {
                biased;
                changed = cancel_rx.changed() => {
                    if changed.is_err() || *cancel_rx.borrow() {
                        cancel_observed = true;
                        break;
                    }
                }
                _ = async {
                    match response_deadline {
                        Some(deadline) => sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    anyhow::bail!("timed out waiting for the next Qwen response event");
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
                                    response_deadline = Some(Instant::now() + event_timeout);
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
                Some(execution) = pending_tools.next(), if tools_pending => {
                    if tools
                        .as_ref()
                        .is_some_and(NativeToolRuntime::generic_fail_closed)
                    {
                        anyhow::bail!(
                            "generic tool outcome was ambiguous; turn failed closed after cleanup"
                        );
                    }
                    if matches!(
                        send_tool_execution(sink, execution, event_timeout, cancel_rx).await?,
                        Cancellable::Cancelled
                    ) {
                        cancel_observed = true;
                        break;
                    }
                    if waiting_for_tools && pending_tools.is_empty() {
                        let tool_runtime = tools
                            .as_ref()
                            .context("Qwen requested tools without a tool runtime")?;
                        anyhow::ensure!(
                            tool_iterations <= tool_runtime.max_iterations,
                            "native Qwen tool iteration limit reached"
                        );
                        if matches!(
                            continue_after_tools(sink, event_timeout, cancel_rx).await?,
                            Cancellable::Cancelled
                        ) {
                            cancel_observed = true;
                            break;
                        }
                        waiting_for_tools = false;
                        active_response = None;
                        response_deadline = Some(Instant::now() + event_timeout);
                    }
                }
                message = async {
                    if deferred_message.is_some() {
                        if !tools_pending {
                            deferred_message
                                .take()
                                .ok_or_else(|| anyhow::anyhow!("deferred message disappeared"))
                        } else {
                            std::future::pending::<anyhow::Result<Message>>().await
                        }
                    } else {
                        stream
                            .next()
                            .await
                            .context("Qwen websocket closed")?
                            .map_err(anyhow::Error::new)
                    }
                } => {
                    let message = message?;
                    if message.is_close() {
                        anyhow::bail!("Qwen websocket closed");
                    }
                    let Message::Text(text) = &message else { continue; };
                    let event = serde_json::from_str::<ServerEvent>(text)
                        .context("decode Qwen server event")?;
                    let defer_until_tools_finish = !pending_tools.is_empty()
                        && match &event {
                            ServerEvent::ResponseAudioDelta(delta) => active_response
                                .as_ref()
                                .is_some_and(|response| response != &delta.response_id),
                            ServerEvent::ResponseAudioTranscriptDone(done) => active_response
                                .as_ref()
                                .is_some_and(|response| response != &done.response_id),
                            ServerEvent::Error(_) => false,
                            _ => true,
                        };
                    if defer_until_tools_finish {
                        deferred_message = Some(message);
                        continue;
                    }
                    match event {
                        ServerEvent::ResponseAudioDelta(delta) => {
                            if active_response
                                .as_ref()
                                .is_some_and(|response| response != &delta.response_id)
                            {
                                warn!("ignoring stale Qwen audio delta");
                                continue;
                            }
                            active_response = Some(delta.response_id);
                            response_deadline = Some(Instant::now() + event_timeout);
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
                        ServerEvent::ResponseAudioTranscriptDone(done) => {
                            anyhow::ensure!(
                                active_response
                                    .as_ref()
                                    .is_none_or(|response| response == &done.response_id),
                                "audio transcript belongs to a conflicting response"
                            );
                            active_response = Some(done.response_id);
                            response_deadline = Some(Instant::now() + event_timeout);
                            debug!(characters = done.transcript.chars().count(), "Qwen transcript completed");
                        }
                        ServerEvent::ResponseFunctionCallArgumentsDone(call) => {
                            let tool_runtime = tools
                                .as_ref()
                                .context("Qwen requested tools without a tool runtime")?;
                            anyhow::ensure!(
                                !waiting_for_tools && pending_tools.is_empty(),
                                "received a function call while prior tools were still running"
                            );
                            anyhow::ensure!(
                                active_response
                                    .as_ref()
                                    .is_none_or(|response| response == &call.response_id),
                                "function call belongs to a conflicting response"
                            );
                            anyhow::ensure!(
                                tool_iterations < tool_runtime.max_iterations,
                                "native Qwen tool iteration limit reached"
                            );
                            anyhow::ensure!(
                                !seen_call_ids.contains(&call.call_id),
                                "replayed Qwen function call ID"
                            );
                            anyhow::ensure!(
                                tool_calls + response_calls.len() < tool_runtime.max_calls,
                                "native Qwen tool call limit reached"
                            );
                            anyhow::ensure!(
                                !call.call_id.0.is_empty()
                                    && call.call_id.0.len() <= MAX_CALL_ID_BYTES,
                                "invalid Qwen function call ID"
                            );
                            anyhow::ensure!(
                                !call.name.is_empty() && call.name.len() <= MAX_TOOL_NAME_BYTES,
                                "invalid Qwen function name"
                            );
                            anyhow::ensure!(
                                call.arguments.0.len() <= MAX_TOOL_ARGUMENT_BYTES,
                                "Qwen function arguments exceed the safety limit"
                            );
                            anyhow::ensure!(
                                !response_calls.contains_key(&call.call_id),
                                "duplicate Qwen function call ID"
                            );
                            anyhow::ensure!(
                                !response_calls.values().any(|prior| {
                                    prior.item_id == call.item_id.0
                                        || prior.output_index == call.output_index
                                }),
                                "duplicate Qwen function call item or output index"
                            );
                            active_response = Some(call.response_id);
                            response_deadline = Some(Instant::now() + event_timeout);
                            seen_call_ids.insert(call.call_id.clone());
                            response_calls.insert(
                                call.call_id,
                                PendingFunctionCall {
                                    item_id: call.item_id.0,
                                    output_index: call.output_index,
                                    name: call.name,
                                    arguments: call.arguments.0,
                                },
                            );
                        }
                        ServerEvent::ResponseDone(done) => {
                            debug!(status = ?done.response.status, "Qwen response completed");
                            anyhow::ensure!(
                                done.response.status == ResponseStatus::Completed,
                                "Qwen response ended with status {:?}",
                                done.response.status
                            );
                            anyhow::ensure!(
                                active_response
                                    .as_ref()
                                    .is_none_or(|response| response == &done.response.id),
                                "response.done belongs to a conflicting response"
                            );
                            let done_calls = done
                                .response
                                .output
                                .iter()
                                .filter_map(|item| match item {
                                    ResponseOutputItem::FunctionCall {
                                        id,
                                        status,
                                        call_id,
                                        name,
                                        arguments,
                                    } => Some((id, status, call_id, name, arguments)),
                                    _ => None,
                                })
                                .collect::<Vec<_>>();
                            if !done_calls.is_empty() {
                                let tool_runtime = tools
                                    .as_ref()
                                    .context("Qwen requested tools without a tool runtime")?;
                                anyhow::ensure!(
                                    done_calls.len() <= tool_runtime.max_calls
                                        && done_calls.len() == response_calls.len(),
                                    "response.done function calls do not match arguments events"
                                );
                                let mut done_ids = HashSet::with_capacity(done_calls.len());
                                for (id, status, call_id, name, arguments) in done_calls {
                                    anyhow::ensure!(
                                        *status == ResponseStatus::Completed
                                            && done_ids.insert(call_id.clone()),
                                        "duplicate or incomplete response.done function call"
                                    );
                                    let received = response_calls.get(call_id).context(
                                        "response.done function call is missing its arguments event",
                                    )?;
                                    anyhow::ensure!(
                                        received.item_id == id.0
                                            && received.name == *name
                                            && received.arguments == arguments.0,
                                        "conflicting function call data across Qwen response events"
                                    );
                                }
                                tool_iterations += 1;
                                tool_calls += response_calls.len();
                                waiting_for_tools = true;
                                response_deadline = None;
                                for (call_id, call) in response_calls.drain() {
                                    pending_tools.push(tool_runtime.execute(
                                        call_id,
                                        call.name,
                                        call.arguments,
                                    ));
                                }

                            } else {
                                anyhow::ensure!(
                                    response_calls.is_empty(),
                                    "response.done omitted a received function call"
                                );
                                response_deadline = None;
                                machine.transition(SessionState::Ready)?;
                                machine.transition(SessionState::ShuttingDown)?;
                                break;
                            }
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
    }
    .await;

    // Dropping the tool futures signals cancellation to the managed RMCP workers.
    // Wait for those workers to send notifications/cull correlation state before
    // websocket teardown can finish or a replacement turn can start.
    drop(pending_tools);
    let mcp_cleanup_result = match &tools {
        Some(tools) => tools.wait_for_tool_idle().await,
        None => Ok(()),
    };

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
        .and(player_result)
        .and(mcp_cleanup_result);
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
        None,
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn run_connected_resources_with_tools<S, St, SinkError, StreamError, P, CaptureOutput>(
    sink: &mut S,
    stream: &mut St,
    player: P,
    audio_rx: &mut mpsc::Receiver<UploadCommand>,
    capture_task: &mut AbortOnDropTask<anyhow::Result<CaptureOutput>>,
    cancel_rx: &mut watch::Receiver<bool>,
    machine: &mut SessionMachine,
    tools: NativeToolRuntime,
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
        Some(tools),
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
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::task::{Context as TaskContext, Poll};
    use rig_core::completion::ToolDefinition as RigToolDefinition;
    use rig_core::tool::Tool;
    use rmcp::model::{
        CallToolRequestParams, CallToolResult, ClientInfo, ErrorData, Implementation,
        ListToolsResult, PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo,
        Tool as RmcpTool,
    };
    use rmcp::service::RequestContext;
    use rmcp::{RoleServer, ServerHandler, ServiceExt};
    use serde::Deserialize;
    use tokio::io::{AsyncRead, ReadBuf};

    #[test]
    fn qwen_tool_schema_normalizes_nullable_type_unions() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "location": {"type": ["string", "null"]},
                "limit": {"type": ["null", "integer"], "minimum": 1},
                "topic": {"type": "string", "enum": ["general", "news"]}
            }
        });

        normalize_qwen_tool_schema(&mut schema).unwrap();

        assert_eq!(schema["properties"]["location"]["type"], "string");
        assert_eq!(schema["properties"]["limit"]["type"], "integer");
        assert_eq!(schema["properties"]["limit"]["minimum"], 1);
        assert_eq!(
            schema["properties"]["topic"]["enum"],
            json!(["general", "news"])
        );
    }

    #[test]
    fn qwen_tool_schema_rejects_ambiguous_type_unions() {
        let mut schema = json!({"type": ["string", "integer"]});

        let error = normalize_qwen_tool_schema(&mut schema).unwrap_err();

        assert!(error.to_string().contains("exactly one non-null type"));
    }

    #[test]
    fn native_qwen_session_uses_manual_turn_detection() {
        let event = ClientEvent::SessionUpdate {
            event_id: None,
            session: SessionUpdate {
                modalities: vec![Modality::Text, Modality::Audio],
                voice: "Cherry".to_string(),
                input_audio_format: AudioFormat::Pcm,
                output_audio_format: AudioFormat::Pcm,
                turn_detection: None,
                instructions: None,
                tools: Vec::new(),
            },
        };

        let value = serde_json::to_value(event).unwrap();

        assert!(value["session"]["turn_detection"].is_null());
    }

    #[derive(Clone)]
    struct MockMcpTool {
        fail: bool,
        pending: Option<Arc<tokio::sync::Notify>>,
        calls: Option<Arc<AtomicU64>>,
    }

    #[derive(Deserialize)]
    struct MockMcpArgs {
        entity_id: String,
    }

    impl Tool for MockMcpTool {
        const NAME: &'static str = "ha_turn_on";
        type Error = std::io::Error;
        type Args = MockMcpArgs;
        type Output = String;

        async fn definition(&self, _: String) -> RigToolDefinition {
            RigToolDefinition {
                name: Self::NAME.to_string(),
                description: "Turn on one Home Assistant entity".to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "entity_id": {"type": "string", "pattern": "^[a-z_]+\\.[a-z0-9_]+$"}
                    },
                    "required": ["entity_id"],
                    "additionalProperties": false
                }),
            }
        }

        async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
            if let Some(calls) = &self.calls {
                calls.fetch_add(1, Ordering::SeqCst);
            }
            if let Some(started) = &self.pending {
                started.notify_one();
                std::future::pending::<()>().await;
            }
            if self.fail {
                return Err(std::io::Error::other("mock MCP failure"));
            }
            Ok(json!({"entity_id": args.entity_id, "changed": true}).to_string())
        }
    }

    #[derive(Clone)]
    struct SideEffectShellTool {
        script: String,
    }

    impl Tool for SideEffectShellTool {
        const NAME: &'static str = "side_effect_shell";
        type Error = std::io::Error;
        type Args = serde_json::Map<String, Value>;
        type Output = String;

        async fn definition(&self, _: String) -> RigToolDefinition {
            RigToolDefinition {
                name: Self::NAME.to_string(),
                description: "Run a deterministic side-effecting shell fixture".to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
            }
        }

        async fn call(&self, _: Self::Args) -> Result<Self::Output, Self::Error> {
            crate::shell::run_shell(&self.script)
                .await
                .map(|result| json!({"exit_code": result.exit_code}).to_string())
                .map_err(std::io::Error::other)
        }
    }

    fn side_effect_path(label: &str) -> std::path::PathBuf {
        static NEXT_PATH: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "xiaoai-qwen-{label}-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::SeqCst)
        ))
    }

    fn side_effect_script(path: &std::path::Path) -> String {
        let path_text = path.to_string_lossy();
        let path = shell_words::quote(&path_text);
        format!("printf 'run\\n' >> {path}; sleep 1; printf 'late\\n' >> {path}")
    }

    #[derive(Clone)]
    struct CancellableRmcpServer {
        started: Arc<tokio::sync::Notify>,
        stopped: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicU64>,
        in_flight: Arc<AtomicU64>,
    }

    struct InFlightRmcpCall {
        in_flight: Arc<AtomicU64>,
        stopped: Arc<tokio::sync::Notify>,
    }

    impl Drop for InFlightRmcpCall {
        fn drop(&mut self) {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.stopped.notify_waiters();
        }
    }

    impl ServerHandler for CancellableRmcpServer {
        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
                .with_protocol_version(ProtocolVersion::LATEST)
                .with_server_info(Implementation::new("cancellable-server", "0.1.0"))
        }

        async fn list_tools(
            &self,
            _request: Option<PaginatedRequestParams>,
            _context: RequestContext<RoleServer>,
        ) -> Result<ListToolsResult, ErrorData> {
            Ok(ListToolsResult::with_all_items(vec![RmcpTool::new(
                "hang_forever".to_string(),
                "Wait for MCP cancellation".to_string(),
                Arc::new(serde_json::Map::new()),
            )]))
        }

        async fn call_tool(
            &self,
            _request: CallToolRequestParams,
            context: RequestContext<RoleServer>,
        ) -> Result<CallToolResult, ErrorData> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            let _in_flight = InFlightRmcpCall {
                in_flight: self.in_flight.clone(),
                stopped: self.stopped.clone(),
            };
            self.started.notify_waiters();
            context.ct.cancelled().await;
            Err(ErrorData::internal_error("cancelled", None))
        }
    }

    #[derive(Clone)]
    struct DynamicRoutingRmcpServer {
        tools: Arc<tokio::sync::RwLock<Vec<RmcpTool>>>,
        list_calls: Arc<AtomicU64>,
        fail_list_from: Option<u64>,
        hang_list_from: Option<u64>,
        started: Arc<tokio::sync::Notify>,
        stopped: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicU64>,
        in_flight: Arc<AtomicU64>,
    }

    impl DynamicRoutingRmcpServer {
        fn new(tools: Vec<RmcpTool>) -> Self {
            Self {
                tools: Arc::new(tokio::sync::RwLock::new(tools)),
                list_calls: Arc::new(AtomicU64::new(0)),
                fail_list_from: None,
                hang_list_from: None,
                started: Arc::new(tokio::sync::Notify::new()),
                stopped: Arc::new(tokio::sync::Notify::new()),
                calls: Arc::new(AtomicU64::new(0)),
                in_flight: Arc::new(AtomicU64::new(0)),
            }
        }

        async fn set_tools(&self, tools: Vec<RmcpTool>) {
            *self.tools.write().await = tools;
        }

        fn failing_after_initial_discovery(mut self) -> Self {
            self.fail_list_from = Some(1);
            self
        }

        fn failing_initial_discovery(mut self) -> Self {
            self.fail_list_from = Some(0);
            self
        }

        fn hanging_initial_discovery(mut self) -> Self {
            self.hang_list_from = Some(0);
            self
        }

        fn hanging_after_initial_discovery(mut self) -> Self {
            self.hang_list_from = Some(1);
            self
        }
    }

    impl ServerHandler for DynamicRoutingRmcpServer {
        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
                .with_protocol_version(ProtocolVersion::LATEST)
                .with_server_info(Implementation::new("dynamic-routing-server", "0.1.0"))
        }

        async fn list_tools(
            &self,
            _request: Option<PaginatedRequestParams>,
            _context: RequestContext<RoleServer>,
        ) -> Result<ListToolsResult, ErrorData> {
            let prior_calls = self.list_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .hang_list_from
                .is_some_and(|hang_from| prior_calls >= hang_from)
            {
                std::future::pending().await
            }
            if self
                .fail_list_from
                .is_some_and(|fail_from| prior_calls >= fail_from)
            {
                return Err(ErrorData::internal_error(
                    "configured routing discovery failure",
                    None,
                ));
            }
            Ok(ListToolsResult::with_all_items(
                self.tools.read().await.clone(),
            ))
        }

        async fn call_tool(
            &self,
            _request: CallToolRequestParams,
            context: RequestContext<RoleServer>,
        ) -> Result<CallToolResult, ErrorData> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            let _in_flight = InFlightRmcpCall {
                in_flight: self.in_flight.clone(),
                stopped: self.stopped.clone(),
            };
            self.started.notify_waiters();
            context.ct.cancelled().await;
            Err(ErrorData::internal_error("cancelled", None))
        }
    }

    fn routing_test_tool(name: &str) -> RmcpTool {
        RmcpTool::new(
            name.to_string(),
            "Wait for managed native cancellation".to_string(),
            Arc::new(serde_json::Map::new()),
        )
    }

    #[derive(Clone)]
    struct NonCooperativeRmcpServer {
        started: Arc<tokio::sync::Notify>,
        stopped: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicU64>,
        in_flight: Arc<AtomicU64>,
        transport_closed: Arc<AtomicBool>,
    }

    struct EofObservedReader<R> {
        inner: R,
        transport_closed: Arc<AtomicBool>,
    }

    impl<R: AsyncRead + Unpin> AsyncRead for EofObservedReader<R> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut TaskContext<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let filled_before = buffer.filled().len();
            let result = Pin::new(&mut self.inner).poll_read(context, buffer);
            if matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() == filled_before {
                self.transport_closed.store(true, Ordering::SeqCst);
            }
            result
        }
    }

    impl ServerHandler for NonCooperativeRmcpServer {
        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
                .with_protocol_version(ProtocolVersion::LATEST)
                .with_server_info(Implementation::new("non-cooperative-server", "0.1.0"))
        }

        async fn list_tools(
            &self,
            _request: Option<PaginatedRequestParams>,
            _context: RequestContext<RoleServer>,
        ) -> Result<ListToolsResult, ErrorData> {
            Ok(ListToolsResult::with_all_items(vec![RmcpTool::new(
                "hang_forever".to_string(),
                "Ignore protocol cancellation and never reply".to_string(),
                Arc::new(serde_json::Map::new()),
            )]))
        }

        async fn call_tool(
            &self,
            _request: CallToolRequestParams,
            context: RequestContext<RoleServer>,
        ) -> Result<CallToolResult, ErrorData> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            let _in_flight = InFlightRmcpCall {
                in_flight: self.in_flight.clone(),
                stopped: self.stopped.clone(),
            };
            self.started.notify_waiters();
            let _ = &context;
            while !self.transport_closed.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(ErrorData::internal_error("transport closed", None))
        }
    }

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
        audio_delta_for("r")
    }

    fn audio_delta_for(response_id: &str) -> Message {
        Message::Text(
            json!({
                "type": "response.audio.delta",
                "response_id": response_id,
                "item_id": "i",
                "output_index": 0,
                "content_index": 0,
                "delta": "AAE="
            })
            .to_string()
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

    fn function_call(call_id: &str) -> Message {
        function_call_with(
            call_id,
            "tool-response",
            "tool-item",
            0,
            "ha_turn_on",
            "{\"entity_id\":\"light.kitchen\"}",
        )
    }

    fn function_call_with(
        call_id: &str,
        response_id: &str,
        item_id: &str,
        output_index: u32,
        name: &str,
        arguments: &str,
    ) -> Message {
        Message::Text(
            json!({
                "type": "response.function_call_arguments.done",
                "response_id": response_id,
                "item_id": item_id,
                "output_index": output_index,
                "call_id": call_id,
                "name": name,
                "arguments": arguments
            })
            .to_string()
            .into(),
        )
    }

    fn tool_response_done() -> Message {
        tool_response_done_with(
            "call-1",
            "tool-response",
            "tool-item",
            "ha_turn_on",
            "{\"entity_id\":\"light.kitchen\"}",
        )
    }

    fn tool_response_done_with(
        call_id: &str,
        response_id: &str,
        item_id: &str,
        name: &str,
        arguments: &str,
    ) -> Message {
        Message::Text(
            json!({
                "type": "response.done",
                "response": {
                    "id": response_id,
                    "object": "realtime.response",
                    "conversation_id": "conversation",
                    "status": "completed",
                    "modalities": ["text", "audio"],
                    "voice": "Cherry",
                    "output_audio_format": "pcm",
                    "output": [{
                        "type": "function_call",
                        "id": item_id,
                        "status": "completed",
                        "call_id": call_id,
                        "name": name,
                        "arguments": arguments
                    }]
                }
            })
            .to_string()
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

    async fn run_tool_messages(
        messages: Vec<Message>,
        tools: NativeToolRuntime,
        event_timeout: Duration,
    ) -> (
        anyhow::Result<()>,
        Arc<StdMutex<MockSinkState>>,
        Arc<MockPlayerState>,
        Arc<AtomicBool>,
    ) {
        let stream = futures::stream::iter(messages.into_iter().map(Ok::<_, MockWsError>))
            .chain(futures::stream::pending());
        run_tool_stream(stream, tools, event_timeout).await
    }

    async fn run_tool_stream<St>(
        stream: St,
        tools: NativeToolRuntime,
        event_timeout: Duration,
    ) -> (
        anyhow::Result<()>,
        Arc<StdMutex<MockSinkState>>,
        Arc<MockPlayerState>,
        Arc<AtomicBool>,
    )
    where
        St: futures::Stream<Item = Result<Message, MockWsError>> + Unpin,
    {
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        run_tool_stream_with_cancel(stream, tools, event_timeout, cancel_rx).await
    }

    async fn run_tool_stream_with_cancel<St>(
        mut stream: St,
        tools: NativeToolRuntime,
        event_timeout: Duration,
        mut cancel_rx: watch::Receiver<bool>,
    ) -> (
        anyhow::Result<()>,
        Arc<StdMutex<MockSinkState>>,
        Arc<MockPlayerState>,
        Arc<AtomicBool>,
    )
    where
        St: futures::Stream<Item = Result<Message, MockWsError>> + Unpin,
    {
        let sink_state = Arc::new(StdMutex::new(MockSinkState::default()));
        let mut sink = MockSink {
            state: sink_state.clone(),
        };
        let player_state = Arc::new(MockPlayerState::default());
        let player = MockPlayer {
            state: player_state.clone(),
            playback: MockPlayback::Accept,
            shutdown_gate: None,
        };
        let (audio_tx, mut audio_rx) = mpsc::channel(1);
        let _keep_audio_open = audio_tx;
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let mut capture = pending_capture(capture_dropped.clone());
        let mut machine = capturing_machine();
        machine.transition(SessionState::Responding).unwrap();
        let result = run_connected_resources_with_timeout(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            (&mut cancel_rx, event_timeout, &mut machine),
            Some(tools),
        )
        .await;
        (result, sink_state, player_state, capture_dropped)
    }

    struct MockRealtimeConnector {
        plans: VecDeque<Vec<Message>>,
        tools: NativeToolRuntime,
        attempts: usize,
        sinks: Vec<Arc<StdMutex<MockSinkState>>>,
    }

    impl RealtimeSessionConnector for MockRealtimeConnector {
        fn run_connected<'a>(
            &'a mut self,
            cancel_rx: &'a mut watch::Receiver<bool>,
            machine: &'a mut SessionMachine,
        ) -> BoxFuture<'a, anyhow::Result<()>> {
            self.attempts += 1;
            let messages = self
                .plans
                .pop_front()
                .expect("unexpected reconnect attempt");
            let tools = self.tools.clone();
            let sink_state = Arc::new(StdMutex::new(MockSinkState::default()));
            self.sinks.push(sink_state.clone());
            async move {
                machine.transition(SessionState::Ready)?;
                machine.transition(SessionState::Capturing)?;
                machine.transition(SessionState::Responding)?;
                let mut sink = MockSink { state: sink_state };
                let mut stream =
                    futures::stream::iter(messages.into_iter().map(Ok::<_, MockWsError>))
                        .chain(futures::stream::pending());
                let player = MockPlayer {
                    state: Arc::new(MockPlayerState::default()),
                    playback: MockPlayback::Accept,
                    shutdown_gate: None,
                };
                let (audio_tx, mut audio_rx) = mpsc::channel(1);
                let _keep_audio_open = audio_tx;
                let mut capture = pending_capture(Arc::new(AtomicBool::new(false)));
                run_connected_resources_with_timeout(
                    &mut sink,
                    &mut stream,
                    player,
                    &mut audio_rx,
                    &mut capture,
                    (cancel_rx, Duration::from_millis(100), machine),
                    Some(tools),
                )
                .await
            }
            .boxed()
        }
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
    async fn native_tool_loop_correlates_call_and_continues_to_final_audio() {
        let tool_server = rig_core::tool::server::ToolServer::new()
            .tool(MockMcpTool {
                fail: false,
                pending: None,
                calls: None,
            })
            .run();
        let config = QwenRealtimeConfig {
            max_tool_iterations: 2,
            max_tool_calls: 1,
            ..QwenRealtimeConfig::default()
        };
        let tools = NativeToolRuntime::load(&config, tool_server).await.unwrap();
        assert_eq!(tools.definitions.len(), 1);
        let ToolDefinition::Function { function } = &tools.definitions[0];
        assert_eq!(function.name, "ha_turn_on");
        assert_eq!(function.parameters["additionalProperties"], false);

        let sink_state = Arc::new(StdMutex::new(MockSinkState::default()));
        let mut sink = MockSink {
            state: sink_state.clone(),
        };
        let final_done =
            Message::Text(include_str!("../tests/fixtures/qwen_server_response_done.json").into());
        let mut stream = futures::stream::iter([
            Ok::<Message, MockWsError>(function_call("call-1")),
            Ok(tool_response_done()),
            Ok(audio_delta_for("resp_HaVOPdbmX6vifiV5pAfJY")),
            Ok(final_done),
        ]);
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

        run_connected_resources_with_tools(
            &mut sink,
            &mut stream,
            player,
            &mut audio_rx,
            &mut capture,
            &mut cancel_rx,
            &mut machine,
            tools,
        )
        .await
        .unwrap();

        let sent = sink_state.lock().unwrap().sent.clone();
        let output_events = sent
            .iter()
            .filter_map(|message| match message {
                Message::Text(text) if text.contains("function_call_output") => Some(text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(output_events.len(), 1);
        assert!(output_events
            .iter()
            .any(|event| event.contains("light.kitchen") && event.contains("changed")));
        assert_eq!(
            player_state.played.lock().unwrap().as_slice(),
            &[vec![0, 1]]
        );
        assert_full_teardown(&sink_state, &player_state, &capture_dropped);
    }

    #[tokio::test]
    async fn native_tool_loop_fails_closed_on_unmatched_and_duplicate_calls() {
        let cases = vec![
            (
                vec![function_call("call-1"), function_call("call-1")],
                "replayed Qwen function call ID",
            ),
            (
                vec![
                    function_call("call-1"),
                    function_call_with(
                        "call-2",
                        "other-response",
                        "tool-item-2",
                        1,
                        "ha_turn_on",
                        "{\"entity_id\":\"light.kitchen\"}",
                    ),
                ],
                "conflicting response",
            ),
            (
                vec![
                    function_call("call-1"),
                    tool_response_done_with(
                        "call-1",
                        "tool-response",
                        "tool-item",
                        "different_name",
                        "{\"entity_id\":\"light.kitchen\"}",
                    ),
                ],
                "conflicting function call data",
            ),
            (vec![tool_response_done()], "do not match arguments events"),
            (
                vec![function_call_with(
                    "",
                    "tool-response",
                    "tool-item",
                    0,
                    "ha_turn_on",
                    "{}",
                )],
                "invalid Qwen function call ID",
            ),
        ];
        for (messages, expected) in cases {
            let calls = Arc::new(AtomicU64::new(0));
            let server = rig_core::tool::server::ToolServer::new()
                .tool(MockMcpTool {
                    fail: false,
                    pending: None,
                    calls: Some(calls.clone()),
                })
                .run();
            let tools = NativeToolRuntime::load(&QwenRealtimeConfig::default(), server)
                .await
                .unwrap();
            let (result, sink, player, capture) =
                run_tool_messages(messages, tools, Duration::from_millis(50)).await;
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains(expected),
                "expected {expected:?}, got {error:?}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_full_teardown(&sink, &player, &capture);
        }
    }

    #[tokio::test]
    async fn later_iteration_call_id_replay_fails_closed_without_second_mcp_call() {
        let calls = Arc::new(AtomicU64::new(0));
        let server = rig_core::tool::server::ToolServer::new()
            .tool(MockMcpTool {
                fail: false,
                pending: None,
                calls: Some(calls.clone()),
            })
            .run();
        let config = QwenRealtimeConfig {
            max_tool_calls: 2,
            max_tool_iterations: 2,
            ..QwenRealtimeConfig::default()
        };
        let tools = NativeToolRuntime::load(&config, server).await.unwrap();
        let messages = vec![
            function_call("call-replay"),
            tool_response_done_with(
                "call-replay",
                "tool-response",
                "tool-item",
                "ha_turn_on",
                "{\"entity_id\":\"light.kitchen\"}",
            ),
            function_call_with(
                "call-replay",
                "later-response",
                "later-item",
                0,
                "ha_turn_on",
                "{\"entity_id\":\"light.kitchen\"}",
            ),
        ];

        let (result, sink, player, capture) =
            run_tool_messages(messages, tools, Duration::from_millis(100)).await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("replayed Qwen function call ID"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_full_teardown(&sink, &player, &capture);
    }

    #[tokio::test]
    async fn malformed_arguments_return_structured_ws_output_without_mcp_execution() {
        let calls = Arc::new(AtomicU64::new(0));
        let server = rig_core::tool::server::ToolServer::new()
            .tool(MockMcpTool {
                fail: false,
                pending: None,
                calls: Some(calls.clone()),
            })
            .run();
        let tools = NativeToolRuntime::load(&QwenRealtimeConfig::default(), server)
            .await
            .unwrap();
        let messages = vec![
            function_call_with(
                "call-bad",
                "tool-response",
                "tool-item",
                0,
                "ha_turn_on",
                "{",
            ),
            tool_response_done_with("call-bad", "tool-response", "tool-item", "ha_turn_on", "{"),
        ];

        let (result, sink, player, capture) =
            run_tool_messages(messages, tools, Duration::from_millis(50)).await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("timed out waiting"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(sink.lock().unwrap().sent.iter().any(|message| matches!(
            message,
            Message::Text(text)
                if text.contains("function_call_output") && text.contains("invalid_arguments")
        )));
        assert_full_teardown(&sink, &player, &capture);
    }

    #[tokio::test]
    async fn native_tool_loop_deadline_and_call_limit_stop_without_execution() {
        let cases = [
            (
                QwenRealtimeConfig::default(),
                function_call("call-timeout"),
                "timed out waiting",
            ),
            (
                QwenRealtimeConfig {
                    max_tool_calls: 0,
                    ..QwenRealtimeConfig::default()
                },
                function_call("call-over-limit"),
                "tool call limit reached",
            ),
        ];
        for (config, message, expected) in cases {
            let calls = Arc::new(AtomicU64::new(0));
            let server = rig_core::tool::server::ToolServer::new()
                .tool(MockMcpTool {
                    fail: false,
                    pending: None,
                    calls: Some(calls.clone()),
                })
                .run();
            let tools = NativeToolRuntime::load(&config, server).await.unwrap();
            let (result, sink, player, capture) =
                run_tool_messages(vec![message], tools, Duration::from_millis(20)).await;
            assert!(result.unwrap_err().to_string().contains(expected));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_full_teardown(&sink, &player, &capture);
        }
    }

    #[tokio::test]
    async fn production_reconnect_loop_retries_before_tool_effect_and_runs_mcp_once() {
        let calls = Arc::new(AtomicU64::new(0));
        let server = rig_core::tool::server::ToolServer::new()
            .tool(MockMcpTool {
                fail: false,
                pending: None,
                calls: Some(calls.clone()),
            })
            .run();
        let tools = NativeToolRuntime::load(&QwenRealtimeConfig::default(), server)
            .await
            .unwrap();
        let final_done =
            Message::Text(include_str!("../tests/fixtures/qwen_server_response_done.json").into());
        let mut connector = MockRealtimeConnector {
            plans: VecDeque::from([
                vec![Message::Close(None)],
                vec![function_call("call-1"), tool_response_done(), final_done],
            ]),
            tools: tools.clone(),
            attempts: 0,
            sinks: Vec::new(),
        };
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        run_realtime_session_loop(&tools, &mut cancel_rx, &mut connector)
            .await
            .unwrap();

        assert_eq!(connector.attempts, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!transparent_reconnect_allowed(&tools));
        assert!(connector
            .sinks
            .iter()
            .all(|sink| sink.lock().unwrap().closed));
    }

    #[tokio::test]
    async fn production_reconnect_loop_fails_closed_after_mcp_without_replay() {
        let calls = Arc::new(AtomicU64::new(0));
        let server = rig_core::tool::server::ToolServer::new()
            .tool(MockMcpTool {
                fail: false,
                pending: None,
                calls: Some(calls.clone()),
            })
            .run();
        let tools = NativeToolRuntime::load(&QwenRealtimeConfig::default(), server)
            .await
            .unwrap();
        let tool_done = tool_response_done_with(
            "call-replay",
            "tool-response",
            "tool-item",
            "ha_turn_on",
            "{\"entity_id\":\"light.kitchen\"}",
        );
        let mut connector = MockRealtimeConnector {
            plans: VecDeque::from([
                vec![
                    function_call("call-replay"),
                    tool_done.clone(),
                    Message::Close(None),
                ],
                vec![function_call("call-replay"), tool_done],
            ]),
            tools: tools.clone(),
            attempts: 0,
            sinks: Vec::new(),
        };
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        let error = run_realtime_session_loop(&tools, &mut cancel_rx, &mut connector)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("transparent reconnect disabled"));
        assert_eq!(connector.attempts, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(connector.plans.len(), 1, "replay websocket was not opened");
        assert!(connector.sinks[0].lock().unwrap().closed);
    }

    #[tokio::test]
    async fn native_tool_failures_are_structured_outputs() {
        let tool_server = rig_core::tool::server::ToolServer::new()
            .tool(MockMcpTool {
                fail: true,
                pending: None,
                calls: None,
            })
            .run();
        let config = QwenRealtimeConfig {
            max_tool_iterations: 2,
            max_tool_calls: 4,
            ..QwenRealtimeConfig::default()
        };
        let tools = NativeToolRuntime::load(&config, tool_server).await.unwrap();

        let failure = tools
            .execute(
                CallId::new("call-fail"),
                "ha_turn_on".to_string(),
                "{\"entity_id\":\"light.kitchen\"}".to_string(),
            )
            .await;
        let parsed: serde_json::Value = serde_json::from_str(&failure.output).unwrap();
        assert_eq!(parsed["ok"], false);
        assert_eq!(parsed["error"]["kind"], "tool_error");
        let malformed = tools
            .execute(
                CallId::new("call-bad-json"),
                "ha_turn_on".to_string(),
                "{".to_string(),
            )
            .await;
        let parsed: serde_json::Value = serde_json::from_str(&malformed.output).unwrap();
        assert_eq!(parsed["error"]["kind"], "invalid_arguments");
    }

    #[tokio::test]
    async fn mock_websocket_surfaces_mcp_error_but_fails_closed_on_generic_timeout() {
        for (fail, pending, expected_output, expected_error) in [
            (true, None, Some("tool_error"), "timed out waiting"),
            (
                false,
                Some(Arc::new(tokio::sync::Notify::new())),
                None,
                "generic tool outcome was ambiguous",
            ),
        ] {
            let server = rig_core::tool::server::ToolServer::new()
                .tool(MockMcpTool {
                    fail,
                    pending,
                    calls: None,
                })
                .run();
            let mut tools = NativeToolRuntime::load(&QwenRealtimeConfig::default(), server)
                .await
                .unwrap();
            tools.call_timeout = Duration::from_millis(20);
            let (result, sink, player, capture) = run_tool_messages(
                vec![function_call("call-1"), tool_response_done()],
                tools,
                Duration::from_millis(100),
            )
            .await;
            assert!(result.unwrap_err().to_string().contains(expected_error));
            let sent = sink.lock().unwrap();
            match expected_output {
                Some(expected) => assert!(sent.sent.iter().any(|message| matches!(
                    message,
                    Message::Text(text)
                        if text.contains("function_call_output") && text.contains(expected)
                ))),
                None => assert!(!sent.sent.iter().any(|message| matches!(
                    message,
                    Message::Text(text) if text.contains("function_call_output")
                ))),
            }
            drop(sent);
            assert_full_teardown(&sink, &player, &capture);
        }
    }

    #[tokio::test]
    async fn generic_shell_timeout_kills_descendants_and_blocks_retry_side_effects() {
        let path = side_effect_path("timeout");
        let server = rig_core::tool::server::ToolServer::new()
            .tool(SideEffectShellTool {
                script: side_effect_script(&path),
            })
            .run();
        let mut tools = NativeToolRuntime::load(&QwenRealtimeConfig::default(), server)
            .await
            .unwrap();
        tools.call_timeout = Duration::from_millis(30);
        let stream = futures::stream::iter([
            Ok::<_, MockWsError>(function_call_with(
                "call-timeout",
                "tool-response",
                "item-timeout",
                0,
                SideEffectShellTool::NAME,
                "{}",
            )),
            Ok(tool_response_done_with(
                "call-timeout",
                "tool-response",
                "item-timeout",
                SideEffectShellTool::NAME,
                "{}",
            )),
        ])
        .chain(futures::stream::once(async {
            tokio::time::sleep(Duration::from_millis(120)).await;
            Ok(function_call_with(
                "call-retry",
                "tool-response",
                "item-retry",
                0,
                SideEffectShellTool::NAME,
                "{}",
            ))
        }))
        .chain(futures::stream::iter([Ok(tool_response_done_with(
            "call-retry",
            "tool-response",
            "item-retry",
            SideEffectShellTool::NAME,
            "{}",
        ))]))
        .chain(futures::stream::pending())
        .boxed();

        let (result, sink, player, capture) =
            run_tool_stream(stream, tools, Duration::from_millis(100)).await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("generic tool outcome was ambiguous"));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "run\n");
        assert!(!sink.lock().unwrap().sent.iter().any(|message| matches!(
            message,
            Message::Text(text) if text.contains("function_call_output")
        )));
        assert_full_teardown(&sink, &player, &capture);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn session_cancellation_kills_generic_shell_descendants_before_returning() {
        let path = side_effect_path("cancel");
        let server = rig_core::tool::server::ToolServer::new()
            .tool(SideEffectShellTool {
                script: side_effect_script(&path),
            })
            .run();
        let tools = NativeToolRuntime::load(&QwenRealtimeConfig::default(), server)
            .await
            .unwrap();
        let stream = futures::stream::iter([
            Ok::<_, MockWsError>(function_call_with(
                "call-cancel",
                "tool-response",
                "item-cancel",
                0,
                SideEffectShellTool::NAME,
                "{}",
            )),
            Ok(tool_response_done_with(
                "call-cancel",
                "tool-response",
                "item-cancel",
                SideEffectShellTool::NAME,
                "{}",
            )),
        ])
        .chain(futures::stream::pending())
        .boxed();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let task = tokio::spawn(run_tool_stream_with_cancel(
            stream,
            tools,
            Duration::from_millis(100),
            cancel_rx,
        ));
        timeout(Duration::from_secs(1), async {
            while !path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shell side effect never started");
        cancel_tx.send(true).unwrap();
        let (result, sink, player, capture) = timeout(Duration::from_secs(1), task)
            .await
            .expect("cancelled session cleanup was not bounded")
            .unwrap();
        assert!(result.is_ok());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "run\n");
        assert_full_teardown(&sink, &player, &capture);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn native_routing_uses_initial_discovery_without_a_second_list_or_generic_fallback() {
        let server = DynamicRoutingRmcpServer::new(vec![routing_test_tool("hang_initial")])
            .failing_after_initial_discovery();
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_service = tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .serve((server_from_client, server_to_client))
                    .await
                    .expect("server failed to start")
            }
        });
        let tool_server = rig_core::tool::server::ToolServer::new().run();
        let (native_mcp, peer) = crate::mcp::connect_native_test_client(
            (client_from_server, client_to_server),
            tool_server.clone(),
        )
        .await
        .expect("native client failed to connect");
        let server_service = server_service.await.unwrap();
        assert_eq!(server.list_calls.load(Ordering::SeqCst), 1);

        let config = QwenRealtimeConfig {
            tool_timeout_s: 0.02,
            ..QwenRealtimeConfig::default()
        };
        let tools =
            NativeToolRuntime::load_with_native_mcp(&config, tool_server, Some(native_mcp.clone()))
                .await
                .unwrap();
        let first = tools
            .execute(
                CallId::new("initial-timeout"),
                "hang_initial".to_string(),
                "{}".to_string(),
            )
            .await;
        let retry = tools
            .execute(
                CallId::new("initial-retry"),
                "hang_initial".to_string(),
                "{}".to_string(),
            )
            .await;
        assert!(first.output.contains("timeout"));
        assert!(retry.output.contains("mcp_fail_closed"));
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
        assert_eq!(server.in_flight.load(Ordering::SeqCst), 0);
        assert!(native_mcp.wait_for_idle(Duration::from_secs(1)).await);
        assert!(peer.is_transport_closed());
        timeout(Duration::from_secs(1), server_service.waiting())
            .await
            .expect("initial-discovery RMCP service leaked")
            .expect("initial-discovery RMCP server failed");
    }

    #[tokio::test]
    async fn poisoned_native_routing_fails_closed_without_fallback_or_service_leak() {
        let server = DynamicRoutingRmcpServer::new(vec![routing_test_tool("must_not_run")]);
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_service = tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .serve((server_from_client, server_to_client))
                    .await
                    .expect("server failed to start")
            }
        });
        let tool_server = rig_core::tool::server::ToolServer::new().run();
        let (native_mcp, peer) = crate::mcp::connect_native_test_client(
            (client_from_server, client_to_server),
            tool_server.clone(),
        )
        .await
        .expect("native client failed to connect");
        let server_service = server_service.await.unwrap();
        let tools = NativeToolRuntime::load_with_native_mcp(
            &QwenRealtimeConfig::default(),
            tool_server,
            Some(native_mcp.clone()),
        )
        .await
        .unwrap();
        native_mcp.poison_routing_lock();

        for call_id in ["poisoned-first", "poisoned-second"] {
            let execution = tools
                .execute(
                    CallId::new(call_id),
                    "must_not_run".to_string(),
                    "{}".to_string(),
                )
                .await;
            assert!(execution.output.contains("mcp_fail_closed"));
        }

        assert_eq!(server.calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.in_flight.load(Ordering::SeqCst), 0);
        assert!(native_mcp.wait_for_idle(Duration::from_secs(1)).await);
        assert!(peer.is_transport_closed());
        timeout(Duration::from_secs(1), server_service.waiting())
            .await
            .expect("poisoned-routing RMCP service leaked")
            .expect("poisoned-routing RMCP server failed");
    }

    #[tokio::test]
    async fn failed_initial_native_discovery_closes_service_before_exposing_tools() {
        let server = DynamicRoutingRmcpServer::new(vec![routing_test_tool("must_not_run")])
            .failing_initial_discovery();
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_service = tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .serve((server_from_client, server_to_client))
                    .await
                    .expect("server failed to start")
            }
        });
        let tool_server = rig_core::tool::server::ToolServer::new().run();
        let error = match crate::mcp::connect_native_test_client(
            (client_from_server, client_to_server),
            tool_server.clone(),
        )
        .await
        {
            Ok(_) => panic!("native discovery failure must fail closed"),
            Err(error) => error,
        };
        assert!(error.contains("configured routing discovery failure"));
        assert_eq!(server.list_calls.load(Ordering::SeqCst), 1);
        assert_eq!(server.calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.in_flight.load(Ordering::SeqCst), 0);
        assert!(tool_server.get_tool_defs(None).await.unwrap().is_empty());

        let server_service = server_service.await.unwrap();
        timeout(Duration::from_secs(1), server_service.waiting())
            .await
            .expect("failed-discovery RMCP service leaked")
            .expect("failed-discovery RMCP server failed");
    }

    #[tokio::test]
    async fn hanging_initial_native_discovery_is_bounded_and_closes_service() {
        let server = DynamicRoutingRmcpServer::new(vec![routing_test_tool("must_not_run")])
            .hanging_initial_discovery();
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_service = tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .serve((server_from_client, server_to_client))
                    .await
                    .expect("server failed to start")
            }
        });
        let tool_server = rig_core::tool::server::ToolServer::new().run();
        let result = timeout(
            Duration::from_secs(1),
            crate::mcp::connect_native_test_client(
                (client_from_server, client_to_server),
                tool_server.clone(),
            ),
        )
        .await
        .expect("native initial discovery was unbounded");
        assert!(result.is_err());
        assert_eq!(server.list_calls.load(Ordering::SeqCst), 1);
        assert_eq!(server.calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.in_flight.load(Ordering::SeqCst), 0);
        assert!(tool_server.get_tool_defs(None).await.unwrap().is_empty());

        let server_service = server_service.await.unwrap();
        timeout(Duration::from_secs(6), server_service.waiting())
            .await
            .expect("hanging-discovery RMCP service leaked")
            .expect("hanging-discovery RMCP server failed");
    }

    async fn assert_hanging_refresh_fails_closed(notification_count: usize) {
        let server = DynamicRoutingRmcpServer::new(vec![routing_test_tool("initial_tool")])
            .hanging_after_initial_discovery();
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_service = tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .serve((server_from_client, server_to_client))
                    .await
                    .expect("server failed to start")
            }
        });
        let tool_server = rig_core::tool::server::ToolServer::new().run();
        let (native_mcp, peer) = crate::mcp::connect_native_test_client(
            (client_from_server, client_to_server),
            tool_server.clone(),
        )
        .await
        .expect("native client failed to connect");
        let server_service = server_service.await.unwrap();
        server_service
            .peer()
            .notify_tool_list_changed()
            .await
            .expect("failed to notify tool list change");
        timeout(Duration::from_secs(1), async {
            while server.list_calls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refresh list request never started");
        for _ in 1..notification_count {
            server_service
                .peer()
                .notify_tool_list_changed()
                .await
                .expect("failed to flood tool list change");
        }
        timeout(Duration::from_secs(1), async {
            while !peer.is_transport_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("hanging refresh did not close the transport");

        let tools = NativeToolRuntime::load_with_native_mcp(
            &QwenRealtimeConfig::default(),
            tool_server,
            Some(native_mcp.clone()),
        )
        .await
        .unwrap();
        for call_id in ["refresh-first", "refresh-retry"] {
            let result = tools
                .execute(
                    CallId::new(call_id),
                    "initial_tool".to_string(),
                    "{}".to_string(),
                )
                .await;
            assert!(result.output.contains("mcp_fail_closed"));
        }
        assert_eq!(server.list_calls.load(Ordering::SeqCst), 2);
        assert_eq!(server.calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.in_flight.load(Ordering::SeqCst), 0);
        assert!(native_mcp.wait_for_idle(Duration::from_secs(1)).await);
        timeout(Duration::from_secs(6), server_service.waiting())
            .await
            .expect("hanging-refresh RMCP service leaked")
            .expect("hanging-refresh RMCP server failed");
    }

    #[tokio::test]
    async fn hanging_native_list_refresh_is_bounded_and_fails_closed_without_fallback() {
        assert_hanging_refresh_fails_closed(1).await;
    }

    #[tokio::test]
    async fn flooded_native_list_refresh_is_single_flight_and_fails_closed() {
        assert_hanging_refresh_fails_closed(128).await;
    }

    #[tokio::test]
    async fn list_changed_addition_uses_managed_cancellation_and_leaves_no_worker() {
        let server = DynamicRoutingRmcpServer::new(vec![routing_test_tool("initial_tool")]);
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_service = tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .serve((server_from_client, server_to_client))
                    .await
                    .expect("server failed to start")
            }
        });
        let tool_server = rig_core::tool::server::ToolServer::new().run();
        let (native_mcp, peer) = crate::mcp::connect_native_test_client(
            (client_from_server, client_to_server),
            tool_server.clone(),
        )
        .await
        .expect("native client failed to connect");
        let server_service = server_service.await.unwrap();
        server
            .set_tools(vec![routing_test_tool("hang_added")])
            .await;
        server_service
            .peer()
            .notify_tool_list_changed()
            .await
            .expect("failed to notify tool list change");
        timeout(Duration::from_secs(1), async {
            while !native_mcp.has_tool("hang_added") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("native routing did not observe list_changed addition");
        assert_eq!(server.list_calls.load(Ordering::SeqCst), 2);

        let config = QwenRealtimeConfig {
            tool_timeout_s: 0.02,
            ..QwenRealtimeConfig::default()
        };
        let tools =
            NativeToolRuntime::load_with_native_mcp(&config, tool_server, Some(native_mcp.clone()))
                .await
                .unwrap();
        assert!(tools.definitions.iter().any(|definition| matches!(
            definition,
            ToolDefinition::Function { function } if function.name == "hang_added"
        )));
        let first = tools
            .execute(
                CallId::new("added-timeout"),
                "hang_added".to_string(),
                "{}".to_string(),
            )
            .await;
        let retry = tools
            .execute(
                CallId::new("added-retry"),
                "hang_added".to_string(),
                "{}".to_string(),
            )
            .await;
        assert!(first.output.contains("timeout"));
        assert!(retry.output.contains("mcp_fail_closed"));
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
        assert_eq!(server.in_flight.load(Ordering::SeqCst), 0);
        assert!(native_mcp.wait_for_idle(Duration::from_secs(1)).await);
        assert!(peer.is_transport_closed());
        timeout(Duration::from_secs(1), server_service.waiting())
            .await
            .expect("list_changed RMCP service leaked")
            .expect("list_changed RMCP server failed");
    }

    #[tokio::test]
    async fn native_rmcp_timeout_cleans_up_and_retry_fails_closed_in_production_loop() {
        let started = Arc::new(tokio::sync::Notify::new());
        let stopped = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicU64::new(0));
        let in_flight = Arc::new(AtomicU64::new(0));
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn({
            let started = started.clone();
            let stopped = stopped.clone();
            let calls = calls.clone();
            let in_flight = in_flight.clone();
            async move {
                let running = CancellableRmcpServer {
                    started,
                    stopped,
                    calls,
                    in_flight,
                }
                .serve((server_from_client, server_to_client))
                .await
                .expect("server failed to start");
                running.waiting().await.expect("server error");
            }
        });
        let tool_server = rig_core::tool::server::ToolServer::new().run();
        let handler =
            crate::mcp::home_assistant_handler(ClientInfo::default(), tool_server.clone());
        let service = handler
            .connect((client_from_server, client_to_server))
            .await
            .expect("client failed to connect");
        let peer = service.peer().clone();
        let native_mcp = NativeMcpClient::new(service, ["hang_forever".to_string()]);
        let config = QwenRealtimeConfig {
            tool_timeout_s: 0.02,
            max_tool_calls: 2,
            max_tool_iterations: 2,
            ..QwenRealtimeConfig::default()
        };
        let tools =
            NativeToolRuntime::load_with_native_mcp(&config, tool_server, Some(native_mcp.clone()))
                .await
                .unwrap();
        let initial_messages = vec![
            function_call_with(
                "call-timeout",
                "tool-response",
                "tool-item",
                0,
                "hang_forever",
                "{}",
            ),
            tool_response_done_with(
                "call-timeout",
                "tool-response",
                "tool-item",
                "hang_forever",
                "{}",
            ),
        ];
        let retry_call = function_call_with(
            "call-retry",
            "retry-response",
            "retry-item",
            0,
            "hang_forever",
            "{}",
        );
        let retry_done = tool_response_done_with(
            "call-retry",
            "retry-response",
            "retry-item",
            "hang_forever",
            "{}",
        );
        let retry_after_cleanup = stopped.clone();
        let stream = futures::stream::iter(initial_messages.into_iter().map(Ok::<_, MockWsError>))
            .chain(futures::stream::once(async move {
                retry_after_cleanup.notified().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(retry_call)
            }))
            .chain(futures::stream::iter([Ok(retry_done)]))
            .chain(futures::stream::pending())
            .boxed();

        let (result, sink, player, capture) =
            run_tool_stream(stream, tools, Duration::from_millis(100)).await;
        let error = result.unwrap_err().to_string();
        assert!(error.contains("timed out waiting"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
        assert!(native_mcp.is_poisoned());
        assert!(native_mcp.wait_for_idle(Duration::from_millis(50)).await);
        let sent = sink.lock().unwrap().sent.clone();
        assert!(sent.iter().any(|message| matches!(
            message,
            Message::Text(text)
                if text.contains("function_call_output") && text.contains("\\\"timeout\\\"")
        )));
        assert!(sent.iter().any(|message| matches!(
            message,
            Message::Text(text)
                if text.contains("function_call_output") && text.contains("mcp_fail_closed")
        )));
        assert_full_teardown(&sink, &player, &capture);

        assert!(peer.is_transport_closed());
        assert!(peer.list_all_tools().await.is_err());
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("RMCP server task leaked")
            .unwrap();
    }

    #[tokio::test]
    async fn non_cooperative_rmcp_timeout_closes_service_before_idle_without_retry() {
        let started = Arc::new(tokio::sync::Notify::new());
        let stopped = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicU64::new(0));
        let in_flight = Arc::new(AtomicU64::new(0));
        let transport_closed = Arc::new(AtomicBool::new(false));
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn({
            let server = NonCooperativeRmcpServer {
                started: started.clone(),
                stopped: stopped.clone(),
                calls: calls.clone(),
                in_flight: in_flight.clone(),
                transport_closed: transport_closed.clone(),
            };
            async move {
                let server_from_client = EofObservedReader {
                    inner: server_from_client,
                    transport_closed,
                };
                server
                    .serve((server_from_client, server_to_client))
                    .await
                    .expect("server failed to start")
                    .waiting()
                    .await
                    .expect("server failed while running");
            }
        });
        let tool_server = rig_core::tool::server::ToolServer::new().run();
        let handler = crate::mcp::home_assistant_handler(ClientInfo::default(), tool_server);
        let service = handler
            .connect((client_from_server, client_to_server))
            .await
            .expect("client failed to connect");
        let peer = service.peer().clone();
        let native_mcp = NativeMcpClient::new(service, ["hang_forever".to_string()]);

        let result = timeout(
            Duration::from_secs(1),
            native_mcp.call("hang_forever", "{}", Duration::from_millis(20)),
        )
        .await
        .expect("non-cooperative RMCP cleanup was unbounded");
        assert!(matches!(result, Err(NativeMcpCallError::Timeout)));
        assert!(native_mcp.wait_for_idle(Duration::from_secs(1)).await);
        assert!(peer.is_transport_closed());
        timeout(Duration::from_secs(1), stopped.notified())
            .await
            .expect("non-cooperative RMCP handler was not dropped");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);

        assert!(matches!(
            native_mcp
                .call("hang_forever", "{}", Duration::from_millis(20))
                .await,
            Err(NativeMcpCallError::FailClosed)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("RMCP server task leaked")
            .unwrap();
    }

    #[tokio::test]
    async fn cancellation_and_deadline_before_rmcp_request_handle_close_service_and_join_worker() {
        for cancel_by_drop in [false, true] {
            let started = Arc::new(tokio::sync::Notify::new());
            let stopped = Arc::new(tokio::sync::Notify::new());
            let calls = Arc::new(AtomicU64::new(0));
            let in_flight = Arc::new(AtomicU64::new(0));
            let (client_to_server, server_from_client) = tokio::io::duplex(8192);
            let (server_to_client, client_from_server) = tokio::io::duplex(8192);
            let server_task = tokio::spawn({
                let server = CancellableRmcpServer {
                    started,
                    stopped,
                    calls: calls.clone(),
                    in_flight: in_flight.clone(),
                };
                async move {
                    server
                        .serve((server_from_client, server_to_client))
                        .await
                        .expect("server failed to start")
                        .waiting()
                        .await
                        .expect("server failed while running");
                }
            });
            let tool_server = rig_core::tool::server::ToolServer::new().run();
            let handler = crate::mcp::home_assistant_handler(ClientInfo::default(), tool_server);
            let service = handler
                .connect((client_from_server, client_to_server))
                .await
                .expect("client failed to connect");
            let peer = service.peer().clone();
            let establishment_arrived = Arc::new(tokio::sync::Notify::new());
            let establishment_release = Arc::new(tokio::sync::Notify::new());
            let native_mcp = NativeMcpClient::new_with_request_establishment_gate(
                service,
                ["hang_forever".to_string()],
                establishment_arrived.clone(),
                establishment_release,
            );
            let worker_client = native_mcp.clone();
            let call_task = tokio::spawn(async move {
                worker_client
                    .call("hang_forever", "{}", Duration::from_millis(20))
                    .await
            });
            timeout(Duration::from_secs(1), establishment_arrived.notified())
                .await
                .expect("request establishment did not reach the deterministic gate");

            if cancel_by_drop {
                call_task.abort();
                assert!(call_task.await.unwrap_err().is_cancelled());
            } else {
                assert!(matches!(
                    timeout(Duration::from_secs(1), call_task)
                        .await
                        .expect("pre-handle deadline cleanup was unbounded")
                        .expect("native MCP caller task panicked"),
                    Err(NativeMcpCallError::Timeout)
                ));
            }

            assert!(native_mcp.wait_for_idle(Duration::from_secs(1)).await);
            assert!(native_mcp.is_poisoned());
            assert!(peer.is_transport_closed());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(in_flight.load(Ordering::SeqCst), 0);
            timeout(Duration::from_secs(1), server_task)
                .await
                .expect("RMCP server task leaked")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn dropping_native_rmcp_call_sends_protocol_cancellation_and_joins_worker() {
        let started = Arc::new(tokio::sync::Notify::new());
        let stopped = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicU64::new(0));
        let in_flight = Arc::new(AtomicU64::new(0));
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn({
            let server = CancellableRmcpServer {
                started: started.clone(),
                stopped: stopped.clone(),
                calls: calls.clone(),
                in_flight: in_flight.clone(),
            };
            async move {
                server
                    .serve((server_from_client, server_to_client))
                    .await
                    .expect("server failed to start")
                    .waiting()
                    .await
                    .expect("server failed while running");
            }
        });
        let tool_server = rig_core::tool::server::ToolServer::new().run();
        let handler = crate::mcp::home_assistant_handler(ClientInfo::default(), tool_server);
        let service = handler
            .connect((client_from_server, client_to_server))
            .await
            .expect("client failed to connect");
        let peer = service.peer().clone();
        let native_mcp = NativeMcpClient::new(service, ["hang_forever".to_string()]);
        let call_client = native_mcp.clone();
        let call = tokio::spawn(async move {
            call_client
                .call("hang_forever", "{}", Duration::from_secs(60))
                .await
        });

        timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("RMCP call never started");
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(native_mcp.wait_for_idle(Duration::from_secs(1)).await);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
        assert!(native_mcp.is_poisoned());
        assert!(peer.is_transport_closed());
        assert!(peer.list_all_tools().await.is_err());

        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("RMCP server task leaked")
            .unwrap();
    }

    #[tokio::test]
    async fn cancellation_preempts_an_in_flight_native_tool_call() {
        let started = Arc::new(tokio::sync::Notify::new());
        let tool_server = rig_core::tool::server::ToolServer::new()
            .tool(MockMcpTool {
                fail: false,
                pending: Some(started.clone()),
                calls: None,
            })
            .run();
        let tools = NativeToolRuntime::load(&QwenRealtimeConfig::default(), tool_server)
            .await
            .unwrap();
        let sink_state = Arc::new(StdMutex::new(MockSinkState::default()));
        let player_state = Arc::new(MockPlayerState::default());
        let capture_dropped = Arc::new(AtomicBool::new(false));
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let task_sink = sink_state.clone();
        let task_player = player_state.clone();
        let task_capture = capture_dropped.clone();

        let task = tokio::spawn(async move {
            let mut sink = MockSink { state: task_sink };
            let mut stream = futures::stream::iter([
                Ok::<Message, MockWsError>(function_call("call-pending")),
                Ok(tool_response_done_with(
                    "call-pending",
                    "tool-response",
                    "tool-item",
                    "ha_turn_on",
                    "{\"entity_id\":\"light.kitchen\"}",
                )),
                Ok(audio_delta_for("tool-response")),
            ])
            .chain(futures::stream::pending());
            let player = MockPlayer {
                state: task_player,
                playback: MockPlayback::Accept,
                shutdown_gate: None,
            };
            let (audio_tx, mut audio_rx) = mpsc::channel(1);
            let _keep_audio_open = audio_tx;
            let mut capture = pending_capture(task_capture);
            let mut machine = capturing_machine();
            run_connected_resources_with_tools(
                &mut sink,
                &mut stream,
                player,
                &mut audio_rx,
                &mut capture,
                &mut cancel_rx,
                &mut machine,
                tools,
            )
            .await
        });

        timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("tool call did not start");
        timeout(Duration::from_secs(1), async {
            loop {
                if !player_state.played.lock().unwrap().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("audio streaming was blocked by the in-flight tool call");
        cancel_tx.send(true).unwrap();
        timeout(Duration::from_secs(2), task)
            .await
            .expect("cancelled tool session did not stop")
            .unwrap()
            .unwrap();

        assert_full_teardown(&sink_state, &player_state, &capture_dropped);
        let sent = sink_state.lock().unwrap().sent.clone();
        assert!(sent.iter().any(|message| matches!(
            message,
            Message::Text(text) if text.contains("response.cancel")
        )));
        assert!(!sent.iter().any(|message| matches!(
            message,
            Message::Text(text) if text.contains("function_call_output")
        )));
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
            rig_core::tool::server::ToolServer::new().run(),
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
            rig_core::tool::server::ToolServer::new().run(),
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
            rig_core::tool::server::ToolServer::new().run(),
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
            rig_core::tool::server::ToolServer::new().run(),
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
                                None,
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
    fn state_machine_accepts_disconnect_reconnect_cycle() {
        let mut machine = SessionMachine::new();
        for state in [
            SessionState::Connecting,
            SessionState::Ready,
            SessionState::Capturing,
            SessionState::Responding,
            SessionState::Reconnecting,
            SessionState::Connecting,
            SessionState::Ready,
            SessionState::Capturing,
            SessionState::Responding,
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
