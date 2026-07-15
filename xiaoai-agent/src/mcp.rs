use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rig_core::tool::rmcp::{McpClientHandler, McpTool, DEFAULT_MCP_TOOL_TIMEOUT};
use rig_core::tool::server::ToolServerHandle;
use rmcp::model::{
    CallToolRequest, CallToolRequestParams, CallToolResult, CancelledNotificationParam,
    ClientCapabilities, ClientInfo, ClientRequest, Implementation, RawContent, ServerResult,
};
use rmcp::service::{
    NotificationContext, PeerRequestOptions, RequestHandle, RunningService,
    RunningServiceCancellationToken, ServerSink, ServiceError,
};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{ClientHandler, ServiceExt};
use tracing::{info, warn};

use crate::config::{AppConfig, HomeAssistantMcpConfig, VoiceRuntime};
use crate::mcp_legacy_sse::LegacySseClientTransport;

pub struct McpConnections {
    _home_assistant: Option<HomeAssistantService>,
    native_client: Option<NativeMcpClient>,
}

const NATIVE_CANCEL_NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(1);
const NATIVE_SERVICE_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
#[cfg(test)]
const NATIVE_TEST_DISCOVERY_TIMEOUT: Duration = Duration::from_millis(50);
const MCP_REFRESH_IDLE: usize = 0;
const MCP_REFRESH_RUNNING: usize = 1;
const MCP_REFRESH_PENDING: usize = 2;
const MCP_SERVICE_OPEN: usize = 0;
const MCP_SERVICE_CLOSING: usize = 1;
const MCP_SERVICE_CLOSED: usize = 2;
const MCP_SERVICE_CLOSE_FAILED: usize = 3;

type HomeAssistantService = RunningService<rmcp::RoleClient, McpClientHandler>;
type NativeHomeAssistantService = RunningService<rmcp::RoleClient, NativeMcpHandler>;

enum NativeOwnedService {
    #[cfg(test)]
    Rig(HomeAssistantService),
    Native(NativeHomeAssistantService),
}

impl NativeOwnedService {
    async fn close_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<rmcp::service::QuitReason>, tokio::task::JoinError> {
        match self {
            #[cfg(test)]
            Self::Rig(service) => service.close_with_timeout(timeout).await,
            Self::Native(service) => service.close_with_timeout(timeout).await,
        }
    }
}

struct NativeMcpRouting {
    tools: std::sync::RwLock<HashSet<String>>,
    failed: AtomicBool,
    service_cancellation: std::sync::Mutex<Option<RunningServiceCancellationToken>>,
}

impl Default for NativeMcpRouting {
    fn default() -> Self {
        Self {
            tools: std::sync::RwLock::new(HashSet::new()),
            failed: AtomicBool::new(false),
            service_cancellation: std::sync::Mutex::new(None),
        }
    }
}

impl NativeMcpRouting {
    fn fail_closed(&self) {
        self.failed.store(true, Ordering::SeqCst);
    }

    fn install_service_cancellation(&self, cancellation: RunningServiceCancellationToken) {
        if self.failed.load(Ordering::SeqCst) {
            cancellation.cancel();
            return;
        }
        let mut slot = match self.service_cancellation.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        if self.failed.load(Ordering::SeqCst) {
            drop(slot);
            cancellation.cancel();
        } else {
            *slot = Some(cancellation);
        }
    }

    fn fail_closed_and_cancel(&self) {
        self.fail_closed();
        let cancellation = match self.service_cancellation.lock() {
            Ok(mut slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
    }

    fn replace_tools(
        &self,
        tools: impl IntoIterator<Item = String>,
    ) -> Result<(), NativeMcpRoutingError> {
        let mut routing_tools = self.tools.write().map_err(|_| {
            self.fail_closed();
            NativeMcpRoutingError::LockPoisoned
        })?;
        routing_tools.clear();
        routing_tools.extend(tools);
        Ok(())
    }

    fn initialize_tools(
        &self,
        tools: impl IntoIterator<Item = String>,
    ) -> Result<(), NativeMcpRoutingError> {
        let mut routing_tools = self.tools.write().map_err(|_| {
            self.fail_closed();
            NativeMcpRoutingError::LockPoisoned
        })?;
        if routing_tools.is_empty() {
            routing_tools.extend(tools);
        }
        Ok(())
    }

    fn routes_tool(&self, name: &str) -> bool {
        if self.failed.load(Ordering::SeqCst) {
            return true;
        }
        match self.tools.read() {
            Ok(tools) => tools.contains(name),
            Err(_) => {
                self.fail_closed();
                warn!("native MCP routing lock poisoned during lookup; failing closed");
                // Stay on the native path so `call` returns the typed fail-closed
                // error instead of falling back to the generic MCP proxy.
                true
            }
        }
    }
}

struct NativeMcpHandler {
    client_info: ClientInfo,
    tool_server: ToolServerHandle,
    managed_tools: tokio::sync::RwLock<Vec<String>>,
    routing: Arc<NativeMcpRouting>,
    discovery_timeout: Duration,
    refresh_state: AtomicUsize,
}

#[derive(Debug, thiserror::Error)]
enum NativeMcpRoutingError {
    #[error("native MCP routing lock poisoned")]
    LockPoisoned,
    #[error(transparent)]
    ToolServer(#[from] rig_core::tool::server::ToolServerError),
}

impl NativeMcpHandler {
    fn new(
        client_info: ClientInfo,
        tool_server: ToolServerHandle,
        routing: Arc<NativeMcpRouting>,
        discovery_timeout: Duration,
    ) -> Self {
        Self {
            client_info,
            tool_server,
            managed_tools: tokio::sync::RwLock::new(Vec::new()),
            routing,
            discovery_timeout,
            refresh_state: AtomicUsize::new(MCP_REFRESH_IDLE),
        }
    }

    fn build_tool(&self, tool: rmcp::model::Tool, peer: ServerSink) -> McpTool {
        McpTool::from_mcp_server(tool, peer).with_timeout(DEFAULT_MCP_TOOL_TIMEOUT)
    }

    async fn replace_tools(
        &self,
        tools: Vec<rmcp::model::Tool>,
        peer: ServerSink,
    ) -> Result<(), NativeMcpRoutingError> {
        let mut managed = self.managed_tools.write().await;
        for name in managed.drain(..) {
            self.tool_server.remove_tool(&name).await?;
        }

        let names = tools
            .iter()
            .map(|tool| tool.name.to_string())
            .collect::<Vec<_>>();
        // Publish routing before any refreshed MCP proxy becomes visible. This
        // deliberately biases the small refresh window toward a safe direct MCP
        // call rather than the generic ToolServer timeout path.
        self.routing.replace_tools(names.iter().cloned())?;
        for tool in tools {
            self.tool_server
                .add_tool(self.build_tool(tool, peer.clone()))
                .await?;
        }
        *managed = names;
        Ok(())
    }
}

struct NativeRefreshGuard<'a>(&'a AtomicUsize);

impl Drop for NativeRefreshGuard<'_> {
    fn drop(&mut self) {
        self.0.store(MCP_REFRESH_IDLE, Ordering::SeqCst);
    }
}

impl ClientHandler for NativeMcpHandler {
    fn get_info(&self) -> ClientInfo {
        self.client_info.clone()
    }

    async fn on_tool_list_changed(&self, context: NotificationContext<rmcp::RoleClient>) {
        if self.routing.failed.load(Ordering::SeqCst) {
            return;
        }
        match self.refresh_state.compare_exchange(
            MCP_REFRESH_IDLE,
            MCP_REFRESH_RUNNING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {}
            Err(MCP_REFRESH_RUNNING) => {
                let _ = self.refresh_state.compare_exchange(
                    MCP_REFRESH_RUNNING,
                    MCP_REFRESH_PENDING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                return;
            }
            Err(_) => return,
        }
        let _refresh_guard = NativeRefreshGuard(&self.refresh_state);
        loop {
            let tools =
                match tokio::time::timeout(self.discovery_timeout, context.peer.list_all_tools())
                    .await
                {
                    Ok(Ok(tools)) => tools,
                    Ok(Err(err)) => {
                        self.routing.fail_closed_and_cancel();
                        warn!("failed to refresh native MCP tool routing; failing closed: {err}");
                        return;
                    }
                    Err(_) => {
                        self.routing.fail_closed_and_cancel();
                        warn!("native MCP tool routing refresh timed out; failing closed");
                        return;
                    }
                };
            if let Err(err) = self.replace_tools(tools, context.peer.clone()).await {
                self.routing.fail_closed_and_cancel();
                warn!("failed to register refreshed native MCP tools; failing closed: {err}");
                return;
            }
            if self
                .refresh_state
                .compare_exchange(
                    MCP_REFRESH_RUNNING,
                    MCP_REFRESH_IDLE,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return;
            }
            // Notifications received during a successful refresh collapse into
            // exactly one follow-up pass. A failing or hanging pass never retries.
            self.refresh_state
                .store(MCP_REFRESH_RUNNING, Ordering::SeqCst);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NativeMcpCallError {
    #[error("native MCP connection is fail-closed after an ambiguous in-flight call")]
    FailClosed,
    #[error("native MCP tool call timed out")]
    Timeout,
    #[error("native MCP tool call was cancelled")]
    Cancelled,
    #[error("native MCP service cleanup timed out")]
    CleanupTimeout,
    #[error("native MCP service cleanup failed: {0}")]
    Cleanup(String),
    #[error("native MCP service error: {0}")]
    Service(#[from] ServiceError),
    #[error("native MCP returned an unexpected response")]
    UnexpectedResponse,
    #[error("native MCP tool returned an error: {0}")]
    Tool(String),
}

struct NativeMcpState {
    peer: ServerSink,
    routing: Arc<NativeMcpRouting>,
    poisoned: AtomicBool,
    active_calls: AtomicUsize,
    idle: tokio::sync::Notify,
    cancel_all: tokio::sync::watch::Sender<bool>,
    service: tokio::sync::Mutex<Option<NativeOwnedService>>,
    service_state: AtomicUsize,
    #[cfg(test)]
    request_establishment_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

#[derive(Clone)]
pub struct NativeMcpClient {
    state: Arc<NativeMcpState>,
}

impl NativeMcpClient {
    #[cfg(test)]
    pub(crate) fn poison_routing_lock(&self) {
        let routing = self.state.routing.clone();
        let _ = std::thread::spawn(move || {
            let _guard = routing
                .tools
                .write()
                .expect("test failed to acquire native MCP routing lock");
            panic!("poison native MCP routing lock for regression coverage");
        })
        .join();
    }

    #[cfg(test)]
    pub(crate) fn new(
        service: HomeAssistantService,
        tools: impl IntoIterator<Item = String>,
    ) -> Self {
        Self::new_inner(NativeOwnedService::Rig(service), tools, None, None)
    }

    #[cfg(test)]
    pub(crate) fn new_with_request_establishment_gate(
        service: HomeAssistantService,
        tools: impl IntoIterator<Item = String>,
        request_establishment_arrived: Arc<tokio::sync::Notify>,
        request_establishment_release: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self::new_inner(
            NativeOwnedService::Rig(service),
            tools,
            Some((request_establishment_arrived, request_establishment_release)),
            None,
        )
    }

    fn from_native(service: NativeHomeAssistantService, routing: Arc<NativeMcpRouting>) -> Self {
        Self::new_inner(
            NativeOwnedService::Native(service),
            std::iter::empty(),
            None,
            Some(routing),
        )
    }

    fn new_inner(
        service: NativeOwnedService,
        tools: impl IntoIterator<Item = String>,
        request_establishment_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        routing: Option<Arc<NativeMcpRouting>>,
    ) -> Self {
        let (cancel_all, _) = tokio::sync::watch::channel(false);
        let peer = match &service {
            #[cfg(test)]
            NativeOwnedService::Rig(service) => service.peer().clone(),
            NativeOwnedService::Native(service) => service.peer().clone(),
        };
        let routing = routing.unwrap_or_else(|| Arc::new(NativeMcpRouting::default()));
        if let Err(err) = routing.initialize_tools(tools) {
            warn!("failed to initialize native MCP routing; failing closed: {err}");
        }
        #[cfg(not(test))]
        let _ = request_establishment_gate;
        Self {
            state: Arc::new(NativeMcpState {
                peer,
                routing,
                poisoned: AtomicBool::new(false),
                active_calls: AtomicUsize::new(0),
                idle: tokio::sync::Notify::new(),
                cancel_all,
                service: tokio::sync::Mutex::new(Some(service)),
                service_state: AtomicUsize::new(MCP_SERVICE_OPEN),
                #[cfg(test)]
                request_establishment_gate,
            }),
        }
    }

    pub fn has_tool(&self, name: &str) -> bool {
        self.state.routing.routes_tool(name)
    }

    pub fn is_poisoned(&self) -> bool {
        self.state.poisoned.load(Ordering::SeqCst)
    }

    pub async fn call(
        &self,
        name: &str,
        arguments: &str,
        deadline: Duration,
    ) -> Result<String, NativeMcpCallError> {
        if self.state.routing.failed.load(Ordering::SeqCst) {
            fail_close_native_mcp(&self.state, None).await?;
            return Err(NativeMcpCallError::FailClosed);
        }
        if self.is_poisoned() {
            return Err(NativeMcpCallError::FailClosed);
        }
        let arguments = serde_json::from_str(arguments)
            .map_err(|err| NativeMcpCallError::Tool(format!("invalid MCP arguments: {err}")))?;
        let request = ClientRequest::CallToolRequest(CallToolRequest::new(
            CallToolRequestParams::new(name.to_string()).with_arguments(arguments),
        ));
        let state = self.state.clone();
        let cancel_all_rx = state.cancel_all.subscribe();
        state.active_calls.fetch_add(1, Ordering::SeqCst);
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run_native_mcp_call(
            state.clone(),
            request,
            deadline,
            cancel_rx,
            cancel_all_rx,
        ));
        NativeMcpCall {
            state,
            cancel_tx: Some(cancel_tx),
            task: Some(task),
        }
        .wait()
        .await
    }

    pub async fn wait_for_idle(&self, deadline: Duration) -> bool {
        let wait = async {
            loop {
                let notified = self.state.idle.notified();
                let active_calls = self.state.active_calls.load(Ordering::SeqCst);
                let service_state = self.state.service_state.load(Ordering::SeqCst);
                if active_calls == 0 && (!self.is_poisoned() || service_state == MCP_SERVICE_CLOSED)
                {
                    break;
                }
                if active_calls == 0 && service_state == MCP_SERVICE_CLOSE_FAILED {
                    return false;
                }
                notified.await;
            }
            true
        };
        tokio::time::timeout(deadline, wait).await == Ok(true)
    }
}

struct ActiveMcpCall(Arc<NativeMcpState>);

impl Drop for ActiveMcpCall {
    fn drop(&mut self) {
        self.0.active_calls.fetch_sub(1, Ordering::SeqCst);
        self.0.idle.notify_waiters();
    }
}

struct NativeMcpCall {
    state: Arc<NativeMcpState>,
    cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<String, NativeMcpCallError>>>,
}

impl NativeMcpCall {
    async fn wait(mut self) -> Result<String, NativeMcpCallError> {
        let task = self.task.take().ok_or_else(|| {
            NativeMcpCallError::Tool("native MCP worker handle was missing".to_string())
        })?;
        let result = task
            .await
            .map_err(|err| NativeMcpCallError::Tool(format!("native MCP task panicked: {err}")))?;
        self.cancel_tx.take();
        result
    }
}

impl Drop for NativeMcpCall {
    fn drop(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            self.state.poisoned.store(true, Ordering::SeqCst);
            self.state.cancel_all.send_replace(true);
            let _ = cancel_tx.send(());
        }
    }
}

async fn run_native_mcp_call(
    state: Arc<NativeMcpState>,
    request: ClientRequest,
    deadline: Duration,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
    mut cancel_all_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<String, NativeMcpCallError> {
    let _active = ActiveMcpCall(state.clone());
    if *cancel_all_rx.borrow() {
        return Err(NativeMcpCallError::Cancelled);
    }
    let deadline = tokio::time::sleep(deadline);
    tokio::pin!(deadline);
    let establish = async {
        #[cfg(test)]
        if let Some((arrived, release)) = &state.request_establishment_gate {
            arrived.notify_one();
            release.notified().await;
        }
        state
            .peer
            .send_cancellable_request(request, PeerRequestOptions::no_options())
            .await
    };
    tokio::pin!(establish);
    let mut request = match tokio::select! {
        biased;
        _ = &mut cancel_rx => Err(NativeMcpCallError::Cancelled),
        _ = cancel_all_rx.changed() => Err(NativeMcpCallError::Cancelled),
        _ = &mut deadline => Err(NativeMcpCallError::Timeout),
        request = &mut establish => request.map_err(NativeMcpCallError::from),
    } {
        Ok(request) => request,
        Err(outcome @ (NativeMcpCallError::Timeout | NativeMcpCallError::Cancelled)) => {
            fail_close_native_mcp(&state, None).await?;
            return Err(outcome);
        }
        Err(err) => return Err(err),
    };
    let outcome = tokio::select! {
        biased;
        _ = &mut cancel_rx => Err(NativeMcpCallError::Cancelled),
        _ = cancel_all_rx.changed() => Err(NativeMcpCallError::Cancelled),
        _ = &mut deadline => Err(NativeMcpCallError::Timeout),
        response = &mut request.rx => {
            let response = response.map_err(|_| ServiceError::TransportClosed)??;
            match response {
                ServerResult::CallToolResult(result) => render_mcp_result(result),
                _ => Err(NativeMcpCallError::UnexpectedResponse),
            }
        }
    };
    if matches!(
        outcome,
        Err(NativeMcpCallError::Timeout | NativeMcpCallError::Cancelled)
    ) {
        fail_close_native_mcp(&state, Some(&request)).await?;
    }
    outcome
}

async fn fail_close_native_mcp(
    state: &NativeMcpState,
    request: Option<&RequestHandle<rmcp::RoleClient>>,
) -> Result<(), NativeMcpCallError> {
    state.poisoned.store(true, Ordering::SeqCst);
    state.cancel_all.send_replace(true);
    if let Some(request) = request {
        let cancellation = request.peer.notify_cancelled(CancelledNotificationParam {
            request_id: request.id.clone(),
            reason: Some("native MCP call ended before a response".to_string()),
        });
        let _ = tokio::time::timeout(NATIVE_CANCEL_NOTIFICATION_TIMEOUT, cancellation).await;
    }

    match state.service_state.compare_exchange(
        MCP_SERVICE_OPEN,
        MCP_SERVICE_CLOSING,
        Ordering::SeqCst,
        Ordering::SeqCst,
    ) {
        Ok(_) => {
            let service = state.service.lock().await.take();
            let result = match service {
                Some(mut service) => match service
                    .close_with_timeout(NATIVE_SERVICE_CLOSE_TIMEOUT)
                    .await
                {
                    Ok(Some(_)) => Ok(()),
                    Ok(None) => Err(NativeMcpCallError::CleanupTimeout),
                    Err(err) => Err(NativeMcpCallError::Cleanup(err.to_string())),
                },
                None => Err(NativeMcpCallError::Cleanup(
                    "native MCP service ownership was missing".to_string(),
                )),
            };
            state.service_state.store(
                if result.is_ok() {
                    MCP_SERVICE_CLOSED
                } else {
                    MCP_SERVICE_CLOSE_FAILED
                },
                Ordering::SeqCst,
            );
            state.idle.notify_waiters();
            result
        }
        Err(MCP_SERVICE_CLOSED) => Ok(()),
        Err(MCP_SERVICE_CLOSE_FAILED) => Err(NativeMcpCallError::Cleanup(
            "a concurrent native MCP cleanup failed".to_string(),
        )),
        Err(_) => {
            let wait = async {
                loop {
                    let notified = state.idle.notified();
                    match state.service_state.load(Ordering::SeqCst) {
                        MCP_SERVICE_CLOSED => return Ok(()),
                        MCP_SERVICE_CLOSE_FAILED => {
                            return Err(NativeMcpCallError::Cleanup(
                                "a concurrent native MCP cleanup failed".to_string(),
                            ));
                        }
                        _ => notified.await,
                    }
                }
            };
            tokio::time::timeout(NATIVE_SERVICE_CLOSE_TIMEOUT, wait)
                .await
                .map_err(|_| NativeMcpCallError::CleanupTimeout)?
        }
    }
}

fn render_mcp_result(result: CallToolResult) -> Result<String, NativeMcpCallError> {
    if result.is_error == Some(true) {
        let message = result
            .content
            .iter()
            .filter_map(|content| content.raw.as_text().map(|text| text.text.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(NativeMcpCallError::Tool(if message.is_empty() {
            "MCP tool reported an error without a message".to_string()
        } else {
            message
        }));
    }
    let mut output = String::new();
    for content in result.content {
        match content.raw {
            RawContent::Text(text) => output.push_str(&text.text),
            other => {
                return Err(NativeMcpCallError::Tool(format!(
                    "unsupported MCP tool content: {other:?}"
                )))
            }
        }
    }
    Ok(output)
}

impl McpConnections {
    pub async fn connect(config: Arc<AppConfig>, tool_server: ToolServerHandle) -> Self {
        let ha = &config.mcp.home_assistant;
        if !ha.enabled {
            return Self {
                _home_assistant: None,
                native_client: None,
            };
        }

        let timeout = match ha.validated_timeout_duration() {
            Ok(timeout) => timeout,
            Err(err) => {
                warn!("failed to connect Home Assistant MCP: {err}");
                return Self {
                    _home_assistant: None,
                    native_client: None,
                };
            }
        };

        let client_info = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("xiaoai-agent", env!("CARGO_PKG_VERSION")),
        );
        if config.voice.runtime == VoiceRuntime::Legacy {
            return Self::connect_legacy(client_info, tool_server, ha, timeout).await;
        }
        Self::connect_native(client_info, tool_server, ha, timeout).await
    }

    async fn connect_legacy(
        client_info: ClientInfo,
        tool_server: ToolServerHandle,
        ha: &HomeAssistantMcpConfig,
        timeout: Duration,
    ) -> Self {
        let handler = home_assistant_handler(client_info, tool_server);
        let service = if is_legacy_sse_url(&ha.url) {
            info!("connecting Home Assistant MCP over legacy SSE");
            let transport =
                LegacySseClientTransport::new(ha.url.clone(), ha.token.clone(), timeout);
            handler.connect(transport).await
        } else {
            info!("connecting Home Assistant MCP over streamable HTTP");
            let mut transport_config =
                StreamableHttpClientTransportConfig::with_uri(ha.url.clone());
            if !ha.token.trim().is_empty() {
                transport_config = transport_config.auth_header(ha.token.clone());
            }
            let transport = StreamableHttpClientTransport::from_config(transport_config);
            handler.connect(transport).await
        };

        match service {
            Ok(service) => {
                info!("connected Home Assistant MCP tools");
                Self {
                    _home_assistant: Some(service),
                    native_client: None,
                }
            }
            Err(err) => {
                warn!("failed to connect Home Assistant MCP: {err}");
                Self {
                    _home_assistant: None,
                    native_client: None,
                }
            }
        }
    }

    async fn connect_native(
        client_info: ClientInfo,
        tool_server: ToolServerHandle,
        ha: &HomeAssistantMcpConfig,
        discovery_timeout: Duration,
    ) -> Self {
        let routing = Arc::new(NativeMcpRouting::default());
        let handler =
            NativeMcpHandler::new(client_info, tool_server, routing.clone(), discovery_timeout);
        let service = if is_legacy_sse_url(&ha.url) {
            info!("connecting Home Assistant MCP over legacy SSE for native Qwen");
            let transport =
                LegacySseClientTransport::new(ha.url.clone(), ha.token.clone(), discovery_timeout);
            connect_native_handler(handler, transport, discovery_timeout).await
        } else {
            info!("connecting Home Assistant MCP over streamable HTTP for native Qwen");
            let mut transport_config =
                StreamableHttpClientTransportConfig::with_uri(ha.url.clone());
            if !ha.token.trim().is_empty() {
                transport_config = transport_config.auth_header(ha.token.clone());
            }
            let transport = StreamableHttpClientTransport::from_config(transport_config);
            connect_native_handler(handler, transport, discovery_timeout).await
        };
        match service {
            Ok(service) => {
                info!("connected native Home Assistant MCP tools");
                Self {
                    _home_assistant: None,
                    native_client: Some(NativeMcpClient::from_native(service, routing)),
                }
            }
            Err(err) => {
                warn!("failed to connect native Home Assistant MCP: {err}");
                Self {
                    _home_assistant: None,
                    native_client: None,
                }
            }
        }
    }

    pub fn native_client(&self) -> Option<NativeMcpClient> {
        self.native_client.clone()
    }
}

async fn connect_native_handler<T, E, A>(
    handler: NativeMcpHandler,
    transport: T,
    discovery_timeout: Duration,
) -> Result<NativeHomeAssistantService, String>
where
    T: rmcp::transport::IntoTransport<rmcp::RoleClient, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let startup_deadline = tokio::time::Instant::now()
        .checked_add(discovery_timeout)
        .ok_or_else(|| "native MCP discovery timeout is too large for a deadline".to_string())?;
    let mut service = match tokio::time::timeout_at(
        startup_deadline,
        ServiceExt::serve(handler, transport),
    )
    .await
    {
        Ok(Ok(service)) => service,
        Ok(Err(err)) => return Err(err.to_string()),
        Err(_) => return Err("native MCP initialization timed out".to_string()),
    };
    service
        .service()
        .routing
        .install_service_cancellation(service.cancellation_token());
    let tools =
        match tokio::time::timeout_at(startup_deadline, service.peer().list_all_tools()).await {
            Ok(Ok(tools)) => tools,
            Ok(Err(err)) => {
                return Err(close_failed_native_service(&mut service, err.to_string()).await)
            }
            Err(_) => {
                return Err(close_failed_native_service(
                    &mut service,
                    "native MCP initial tool discovery timed out".to_string(),
                )
                .await)
            }
        };
    let replace_result = tokio::time::timeout_at(
        startup_deadline,
        service
            .service()
            .replace_tools(tools, service.peer().clone()),
    )
    .await;
    match replace_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            return Err(close_failed_native_service(&mut service, err.to_string()).await)
        }
        Err(_) => {
            return Err(close_failed_native_service(
                &mut service,
                "native MCP initial tool registration timed out".to_string(),
            )
            .await)
        }
    }
    Ok(service)
}

async fn close_failed_native_service(
    service: &mut NativeHomeAssistantService,
    failure: String,
) -> String {
    service.service().routing.fail_closed_and_cancel();
    match service
        .close_with_timeout(NATIVE_SERVICE_CLOSE_TIMEOUT)
        .await
    {
        Ok(Some(_)) => failure,
        Ok(None) => format!("{failure}; native MCP service cleanup timed out"),
        Err(err) => format!("{failure}; native MCP service cleanup failed: {err}"),
    }
}

#[cfg(test)]
pub(crate) async fn connect_native_test_client<T, E, A>(
    transport: T,
    tool_server: ToolServerHandle,
) -> Result<(NativeMcpClient, ServerSink), String>
where
    T: rmcp::transport::IntoTransport<rmcp::RoleClient, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let routing = Arc::new(NativeMcpRouting::default());
    let handler = NativeMcpHandler::new(
        ClientInfo::default(),
        tool_server,
        routing.clone(),
        NATIVE_TEST_DISCOVERY_TIMEOUT,
    );
    let service = connect_native_handler(handler, transport, NATIVE_TEST_DISCOVERY_TIMEOUT).await?;
    let peer = service.peer().clone();
    Ok((NativeMcpClient::from_native(service, routing), peer))
}

pub(crate) fn home_assistant_handler(
    client_info: ClientInfo,
    tool_server: ToolServerHandle,
) -> McpClientHandler {
    // Keep Rig's legacy/default MCP tool-call deadline. `ha.timeout_s` bounds
    // the legacy SSE transport; native Qwen applies its own per-call deadline.
    McpClientHandler::new(client_info, tool_server)
}

fn is_legacy_sse_url(url: &str) -> bool {
    url.trim_end_matches('/').ends_with("/sse")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rig_core::tool::rmcp::DEFAULT_MCP_TOOL_TIMEOUT;
    use rig_core::tool::server::ToolServer;
    use rmcp::model::*;
    use rmcp::service::RequestContext;
    use rmcp::{RoleServer, ServerHandler, ServiceExt};
    use tokio::io::{AsyncBufReadExt, BufReader};

    use super::home_assistant_handler;

    fn poisoned_routing() -> Arc<super::NativeMcpRouting> {
        let routing = Arc::new(super::NativeMcpRouting::default());
        let poison_target = routing.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poison_target
                .tools
                .write()
                .expect("test failed to acquire native MCP routing lock");
            panic!("poison native MCP routing lock for regression coverage");
        })
        .join();
        routing
    }

    #[test]
    fn poisoned_routing_operations_are_non_panicking_and_fail_closed() {
        let replace = poisoned_routing();
        assert!(replace.replace_tools(["replacement".to_string()]).is_err());
        assert!(replace.failed.load(std::sync::atomic::Ordering::SeqCst));

        let initialize = poisoned_routing();
        assert!(initialize
            .initialize_tools(["initial".to_string()])
            .is_err());
        assert!(initialize.failed.load(std::sync::atomic::Ordering::SeqCst));

        let lookup = poisoned_routing();
        assert!(lookup.routes_tool("must_stay_on_native_path"));
        assert!(lookup.failed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn native_initialize_timeout_is_bounded_and_drops_handshake_transport() {
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let initialize_received = Arc::new(tokio::sync::Notify::new());
        let server_task = tokio::spawn({
            let initialize_received = initialize_received.clone();
            async move {
                let mut reader = BufReader::new(server_from_client);
                let mut line = String::new();
                reader
                    .read_line(&mut line)
                    .await
                    .expect("mock server failed to read Initialize");
                let message: serde_json::Value =
                    serde_json::from_str(&line).expect("Initialize was not valid JSON");
                assert_eq!(message["method"], "initialize");
                initialize_received.notify_one();

                // Keep the response side open but deliberately never answer Initialize.
                let _server_to_client = server_to_client;
                line.clear();
                assert_eq!(
                    reader
                        .read_line(&mut line)
                        .await
                        .expect("mock server failed while waiting for client cleanup"),
                    0,
                    "startup timeout did not drop the handshake transport"
                );
            }
        });
        let tool_server = ToolServer::new().run();
        let connect = super::connect_native_test_client(
            (client_from_server, client_to_server),
            tool_server.clone(),
        );
        let (result, ()) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(1), connect),
            async { initialize_received.notified().await }
        );
        let error = match result.expect("native Initialize handshake was unbounded") {
            Ok(_) => panic!("server that never answers Initialize unexpectedly connected"),
            Err(error) => error,
        };
        assert!(error.contains("native MCP initialization timed out"));
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("native Initialize handshake transport leaked")
            .expect("mock Initialize server failed");
        assert!(tool_server.get_tool_defs(None).await.unwrap().is_empty());
    }

    #[derive(Clone)]
    struct HangingToolServer {
        started: Arc<tokio::sync::Notify>,
    }

    impl ServerHandler for HangingToolServer {
        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
                .with_protocol_version(ProtocolVersion::LATEST)
                .with_server_info(Implementation::new("hanging-server", "0.1.0"))
        }

        async fn list_tools(
            &self,
            _request: Option<PaginatedRequestParams>,
            _context: RequestContext<RoleServer>,
        ) -> Result<ListToolsResult, ErrorData> {
            Ok(ListToolsResult::with_all_items(vec![Tool::new(
                "hang_forever".to_string(),
                "Never returns".to_string(),
                Arc::new(serde_json::Map::new()),
            )]))
        }

        async fn call_tool(
            &self,
            _request: CallToolRequestParams,
            _context: RequestContext<RoleServer>,
        ) -> Result<CallToolResult, ErrorData> {
            self.started.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn home_assistant_wrapper_preserves_rig_default_tool_timeout() {
        assert_eq!(DEFAULT_MCP_TOOL_TIMEOUT, Duration::from_secs(300));
        let started = Arc::new(tokio::sync::Notify::new());
        let (client_to_server, server_from_client) = tokio::io::duplex(8192);
        let (server_to_client, client_from_server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn({
            let started = started.clone();
            async move {
                let running = HangingToolServer { started }
                    .serve((server_from_client, server_to_client))
                    .await
                    .expect("server failed to start");
                running.waiting().await.expect("server error");
            }
        });
        let tools = ToolServer::new().run();
        let handler = home_assistant_handler(ClientInfo::default(), tools.clone());
        let _service = handler
            .connect((client_from_server, client_to_server))
            .await
            .expect("client failed to connect");

        let mut call = tokio::spawn({
            let tools = tools.clone();
            async move { tools.call_tool("hang_forever", "{}").await }
        });
        started.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut call)
                .await
                .is_err(),
            "application wrapper replaced Rig's generous legacy timeout"
        );
        call.abort();
        server_task.abort();
    }
}
