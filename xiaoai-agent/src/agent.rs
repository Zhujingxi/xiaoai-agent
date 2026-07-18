use std::sync::Arc;

use anyhow::Context;
use rig_core::client::CompletionClient;
use rig_core::completion::Prompt;
use rig_core::providers::openai;
use rig_core::tool::server::{ToolServer, ToolServerHandle};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::config::{timeout_duration, AppConfig, VoiceRuntime};
use crate::device::Device;
use crate::mcp::{McpConnections, NativeMcpClient};
use crate::music::MusicService;
use crate::tools::*;

pub const SPEAKER_AGENT_INSTRUCTIONS: &str = concat!(
    "你是小爱同学，一个运行在小爱音箱上的中文语音助手。",
    "你运行在小爱音箱端侧；端侧程序负责语音唤醒、云端 ASR、TTS 和播放控制。",
    "你负责根据用户文本请求完成任务并返回适合朗读的文本。",
    "直接给最终答案，不要输出思考过程。",
    "回答必须极简：默认只说一句话，能更短就更短。",
    "不要客套话，不要开场白和结束语，不要复述用户的问题，不要事后总结。",
    "只有用户明确要求详细解释时才展开。",
    "不要使用 Markdown，不要列太长的清单。",
    "当用户询问当前时间、日期、天气或预报时，必须调用工具获得实时信息。",
    "当用户询问新闻、最近发生的事、实时资料，或明确要求上网搜索时，必须调用 web_search 工具。",
    "基于搜索结果回答时，要简短说明信息来源；搜索结果不足或互相矛盾时，要说无法确认。",
    "当用户要求控制智能家居设备时，必须调用 Home Assistant 或 MCP 工具；",
    "控制智能家居前优先调用 GetLiveContext 定位目标名称、区域、domain 和当前 state。",
    "如果目标 state 是 unavailable 或 unknown，要告诉用户设备当前不可用，不要继续假装成功。",
    "只有工具返回明确成功时才可以说操作完成。",
    "如果工具返回错误、没有匹配设备或结果不明确，要说明无法确认，不要假装成功。",
    "当用户要求播放、搜索、停止音乐或询问当前播放内容时，必须调用音乐工具。",
    "用户只说播放某首歌或某位歌手时，优先调用 play_music_query，",
    "这会替换当前队列并开始播放；用户说放点音乐、随便放点歌、随机播放时，调用 play_random_music；",
    "用户说加入、添加到队列、稍后播放时，调用 add_music_to_play_queue；",
    "用户明确说随机加几首到队列时，调用 add_random_music_to_play_queue。",
    "用户说下一首、上一首、暂停、继续播放时，调用对应的音乐播放控制工具。",
    "如果候选不明确或播放失败，再简短追问或说明原因。",
    "当音乐工具提示需要网易云验证码登录，或用户要求登录网易云时，",
    "先调用 request_music_login_code 发送验证码；用户报出短信数字后，",
    "调用 submit_music_login_code 完成登录；登录 cookie 只在本次进程内保存。",
    "当用户表达再见、拜拜、不用了、结束对话、先这样等结束意图时，",
    "必须调用 end_conversation 工具，然后用一句简短自然的告别作为最终回复。",
    "绝不能在用户没有明确表达结束意图时主动调用 end_conversation；完成任务不等于结束对话。",
    "如果工具返回错误或信息不足，要坦率说明无法确认。"
);

/// Built-in instructions plus the site-specific rules from
/// `agent.custom_instructions` (editable via the web UI).
pub fn speaker_instructions(config: &AppConfig) -> String {
    let custom = config.agent.custom_instructions.trim();
    if custom.is_empty() {
        SPEAKER_AGENT_INSTRUCTIONS.to_string()
    } else {
        format!("{SPEAKER_AGENT_INSTRUCTIONS}\n{custom}")
    }
}

#[derive(Debug, Clone)]
pub struct AgentTurnResult {
    pub text: String,
    pub should_end: bool,
    pub end_reason: String,
}

/// Strips content that must never reach TTS: model reasoning blocks
/// (`<think>...</think>`), tool-call/progress indicator lines (emoji-led, as
/// produced by Hermes Agent), and markdown code-fence markers. The remote
/// agent may include any of these in its final message; the speaker should
/// only read the user-facing answer.
pub fn sanitize_speech_text(raw: &str) -> String {
    let without_thinking = strip_think_blocks(raw);
    let mut kept = Vec::new();
    for line in without_thinking.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            continue;
        }
        if trimmed
            .chars()
            .next()
            .is_some_and(is_progress_indicator_char)
        {
            continue;
        }
        kept.push(line);
    }
    let joined = kept.join("\n");
    // Collapse the blank runs left behind by removed lines, then trim.
    let mut out = String::with_capacity(joined.len());
    let mut newlines = 0;
    for ch in joined.chars() {
        if ch == '\n' {
            newlines += 1;
            if newlines > 2 {
                continue;
            }
        } else {
            newlines = 0;
        }
        out.push(ch);
    }
    out.trim().to_string()
}

/// Removes `<think>...</think>` and `<thinking>...</thinking>` blocks. An
/// unclosed opening tag drops the rest of the message: a truncated reasoning
/// block must not leak into TTS either.
fn strip_think_blocks(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    loop {
        let open_at = ["<think>", "<thinking>"]
            .iter()
            .filter_map(|tag| rest.find(tag))
            .min();
        let Some(start) = open_at else {
            out.push_str(rest);
            break;
        };
        out.push_str(&rest[..start]);
        let (open_len, close) = if rest[start..].starts_with("<thinking>") {
            ("<thinking>".len(), "</thinking>")
        } else {
            ("<think>".len(), "</think>")
        };
        let body_start = start + open_len;
        match rest[body_start..].find(close) {
            Some(i) => rest = &rest[body_start + i + close.len()..],
            None => break,
        }
    }
    out
}

/// Emoji-led lines are tool/progress indicators (e.g. "💻 ls", "🔍
/// searching..."), not speakable answer text.
fn is_progress_indicator_char(ch: char) -> bool {
    matches!(
        ch as u32,
        0x2190..=0x21FF // arrows
            | 0x2600..=0x27BF // misc symbols & dingbats
            | 0x2B00..=0x2BFF // misc symbols & arrows (⏳)
            | 0x1F000..=0x1FAFF // pictographs (💻🔍✅)
            | 0xFE00..=0xFE0F // variation selectors
    )
}

#[derive(Default)]
pub struct AgentControl {
    pub should_end: bool,
    pub end_reason: String,
}

pub struct AgentRuntime {
    config: Arc<AppConfig>,
    tool_server: ToolServerHandle,
    // Owned so the legacy Home Assistant MCP connection stays alive.
    _mcp: McpConnections,
    native_mcp: std::sync::RwLock<Option<NativeMcpClient>>,
    native_mcp_generation: std::sync::atomic::AtomicU64,
    native_mcp_reconnecting: std::sync::atomic::AtomicBool,
    history: Mutex<Vec<(String, String)>>,
    control: Arc<Mutex<AgentControl>>,
}

impl AgentRuntime {
    pub async fn new(
        config: Arc<AppConfig>,
        device: Device,
        music: Arc<MusicService>,
    ) -> anyhow::Result<Self> {
        let control = Arc::new(Mutex::new(AgentControl::default()));
        let tool_server = ToolServer::new()
            .tool(GetCurrentTime::new(config.clone()))
            .tool(GetWeather::new(config.clone()))
            .tool(WebSearch::new(config.clone()))
            .tool(EndConversation::new(control.clone()))
            .tool(SearchMusic::new(music.clone()))
            .tool(RequestMusicLoginCode::new(music.clone()))
            .tool(SubmitMusicLoginCode::new(music.clone()))
            .tool(PlayMusicQuery::new(music.clone()))
            .tool(AddMusicToQueue::new(music.clone()))
            .tool(AddRandomMusicToQueue::new(music.clone()))
            .tool(PlayRandomMusic::new(music.clone()))
            .tool(StopMusicPlayback::new(music.clone()))
            .tool(PauseMusicPlayback::new(music.clone()))
            .tool(ResumeMusicPlayback::new(music.clone()))
            .tool(PlayNextMusic::new(music.clone()))
            .tool(PlayPreviousMusic::new(music.clone()))
            .tool(GetMusicStatus::new(music))
            .run();

        let mcp = McpConnections::connect(config.clone(), tool_server.clone()).await;
        if let Err(err) = device.stop_audio().await {
            warn!("initial stop_audio failed, continuing: {err:?}");
        }

        let native_mcp = std::sync::RwLock::new(mcp.native_client());
        Ok(Self {
            config,
            tool_server,
            _mcp: mcp,
            native_mcp,
            native_mcp_generation: std::sync::atomic::AtomicU64::new(0),
            native_mcp_reconnecting: std::sync::atomic::AtomicBool::new(false),
            history: Mutex::new(Vec::new()),
            control,
        })
    }

    pub async fn run_turn(&self, message: &str) -> anyhow::Result<AgentTurnResult> {
        {
            let mut control = self.control.lock().await;
            control.should_end = false;
            control.end_reason.clear();
        }

        let prompt = self.prompt_with_history(message).await;
        let attempts = self.config.llm.retries.saturating_add(1);
        let mut last_error = None;
        let mut text = None;
        for attempt in 1..=attempts {
            match self.prompt_once(prompt.clone()).await {
                Ok(reply) => {
                    text = Some(reply);
                    break;
                }
                Err(err) => last_error = Some(err),
            }

            if attempt < attempts {
                if let Some(err) = &last_error {
                    warn!("LLM attempt {attempt}/{attempts} failed: {err:?}");
                }
            } else if let Some(err) = last_error {
                return Err(err);
            } else {
                return Err(anyhow::anyhow!("LLM request failed without attempts"));
            }
        }
        let Some(mut text) = text else {
            return Err(last_error
                .unwrap_or_else(|| anyhow::anyhow!("LLM request failed without attempts")));
        };
        if self.config.voice.runtime == VoiceRuntime::Hermes {
            let sanitized = sanitize_speech_text(&text);
            if sanitized.len() != text.len() {
                debug!("stripped non-speech content from hermes reply");
            }
            text = sanitized;
        }

        let control = self.control.lock().await;
        self.push_history(message, &text).await;
        Ok(AgentTurnResult {
            text,
            should_end: control.should_end,
            end_reason: control.end_reason.clone(),
        })
    }

    pub fn tool_server(&self) -> ToolServerHandle {
        self.tool_server.clone()
    }

    pub fn native_mcp_client(&self) -> Option<NativeMcpClient> {
        match self.native_mcp.read() {
            Ok(slot) => slot.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    pub fn native_mcp_needs_reconnect(&self) -> bool {
        if !self.config.mcp.home_assistant.enabled
            || self.config.voice.runtime != crate::config::VoiceRuntime::NativeQwen
        {
            return false;
        }
        match self.native_mcp_client() {
            // The boot-time connect failed (e.g. network not up yet); the
            // client never existed and must be established at idle.
            None => true,
            Some(client) => client.needs_reconnect(),
        }
    }

    /// Bumped after every successful native MCP reconnect so callers holding a
    /// prewarmed session can rotate it onto the fresh client.
    pub fn native_mcp_generation(&self) -> u64 {
        self.native_mcp_generation
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Replaces a fail-closed native MCP client with a freshly connected one.
    /// Safe to call while idle; single-flight and a no-op when healthy.
    pub async fn reconnect_native_mcp(&self) -> bool {
        use std::sync::atomic::Ordering;
        if self.native_mcp_reconnecting.swap(true, Ordering::SeqCst) {
            return false;
        }
        let reconnected = self.reconnect_native_mcp_inner().await;
        self.native_mcp_reconnecting.store(false, Ordering::SeqCst);
        reconnected
    }

    async fn reconnect_native_mcp_inner(&self) -> bool {
        if !self.native_mcp_needs_reconnect() {
            return false;
        }
        match self.native_mcp_client() {
            Some(current) => {
                warn!("native Home Assistant MCP client is fail-closed; reconnecting");
                for name in current.tool_names() {
                    if let Err(err) = self.tool_server.remove_tool(&name).await {
                        warn!("failed to remove stale MCP tool {name}: {err}");
                    }
                }
            }
            None => warn!("native Home Assistant MCP was never connected; connecting at idle"),
        }
        let fresh = McpConnections::connect(self.config.clone(), self.tool_server.clone())
            .await
            .native_client();
        match fresh {
            Some(fresh) => {
                match self.native_mcp.write() {
                    Ok(mut slot) => *slot = Some(fresh),
                    Err(poisoned) => *poisoned.into_inner() = Some(fresh),
                }
                self.native_mcp_generation
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tracing::info!("reconnected native Home Assistant MCP tools");
                true
            }
            None => {
                warn!("native MCP reconnect failed; keeping fail-closed client until retry");
                false
            }
        }
    }

    async fn prompt_once(&self, prompt: String) -> anyhow::Result<String> {
        let http_client = reqwest::Client::builder()
            .timeout(timeout_duration(self.config.llm.timeout_s))
            .build()
            .context("failed to create LLM HTTP client")?;
        let client = openai::Client::builder()
            .api_key(self.config.llm.api_key.as_str())
            .base_url(self.config.llm.base_url.as_str())
            .http_client(http_client)
            .build()
            .context("failed to create OpenAI-compatible Rig client")?
            .completions_api();

        let builder = client
            .agent(self.config.llm.model.as_str())
            .preamble(&speaker_instructions(&self.config))
            .temperature(self.config.llm.temperature)
            .max_tokens(self.config.llm.max_tokens)
            .default_max_turns(3);

        let text = if self.config.voice.runtime == VoiceRuntime::Hermes {
            // The remote Hermes agent is the sole brain: do not advertise the
            // local tool server or send provider-specific reasoning params.
            builder.build().prompt(prompt).await?
        } else {
            builder
                .additional_params(json!({
                    "reasoning_effort": "low",
                    "enable_thinking": false,
                    "chat_template_kwargs": {"enable_thinking": false}
                }))
                .tool_server_handle(self.tool_server.clone())
                .build()
                .prompt(prompt)
                .await?
        };
        Ok(text.trim().to_string())
    }

    pub async fn reset_session(&self, reason: &str) {
        self.history.lock().await.clear();
        let mut control = self.control.lock().await;
        control.should_end = false;
        control.end_reason = reason.to_string();
    }

    async fn prompt_with_history(&self, message: &str) -> String {
        let history = self.history.lock().await;
        if history.is_empty() {
            return message.to_string();
        }
        let mut prompt = String::from("最近对话历史：\n");
        for (role, text) in history.iter() {
            prompt.push_str(role);
            prompt.push('：');
            prompt.push_str(text);
            prompt.push('\n');
        }
        prompt.push_str("当前用户：");
        prompt.push_str(message);
        prompt
    }

    async fn push_history(&self, user: &str, assistant: &str) {
        let mut history = self.history.lock().await;
        history.push(("用户".to_string(), user.to_string()));
        history.push(("助手".to_string(), assistant.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::sanitize_speech_text;

    #[test]
    fn sanitize_keeps_plain_answer_text() {
        let raw = "现在下午三点。";
        assert_eq!(sanitize_speech_text(raw), raw);
    }

    #[test]
    fn sanitize_strips_closed_and_unclosed_think_blocks() {
        assert_eq!(
            sanitize_speech_text("<think>先在内部推理一下</think>答案是四十二。"),
            "答案是四十二。"
        );
        assert_eq!(
            sanitize_speech_text("<thinking>更长标签的推理</thinking>答案B"),
            "答案B"
        );
        assert_eq!(sanitize_speech_text("前面保留。<think>没写完的推理"), "前面保留。");
    }

    #[test]
    fn sanitize_strips_tool_progress_lines_but_keeps_answer() {
        let raw = "💻 ls -la\n🔍 searching the web...\n✅ found 3 results\n\n答案是：今天晴天。";
        assert_eq!(sanitize_speech_text(raw), "答案是：今天晴天。");
    }

    #[test]
    fn sanitize_strips_code_fence_markers_but_keeps_content() {
        let raw = "可以这样：\n```sh\nuptime\n```\n就这些。";
        assert_eq!(sanitize_speech_text(raw), "可以这样：\nuptime\n就这些。");
    }

    #[test]
    fn sanitize_returns_empty_when_only_progress_output() {
        assert_eq!(sanitize_speech_text("🔍 searching...\n💻 cargo test"), "");
        assert_eq!(sanitize_speech_text("<think>只有推理</think>"), "");
    }

    #[test]
    fn sanitize_collapses_blank_runs_left_by_stripped_lines() {
        let raw = "第一句。\n🔍 searching...\n💻 working...\n第二句。";
        assert_eq!(sanitize_speech_text(raw), "第一句。\n第二句。");
    }
}
