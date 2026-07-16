# XiaoAI Agent

![](https://forthebadge.com/images/badges/built-with-love.svg)
![](https://forthebadge.com/images/badges/made-with-rust.svg)
![](https://forthebadge.com/images/badges/powered-by-electricity.svg)
![](https://forthebadge.com/images/badges/makes-people-smile.svg)

运行在小爱音箱端侧的独立语音 Agent。仅需配置 ASR 与大模型服务 API，即可在音箱端侧完成唤醒、ASR、LLM 对话、工具调用和 TTS 回复。
与 Open-XiaoAI 和 [MiGPT](https://github.com/idootop/mi-gpt) 项目不同，XiaoAI Agent 无需部署专门的服务端运行 Agent，也不会与原生小爱同学抢麦、抢答或触发小米云端控制。
目前仅在 Xiaomi 智能音箱 Pro（OH2P）固件 `1.62.2` 上测试成功，其他型号和固件版本需要自行适配并承担风险。

https://github.com/user-attachments/assets/b12d71b7-6734-4166-a2fe-959f82273702

## 特性

- 完全接管语音对话流程：为了避免和原生小爱同学抢麦、抢答或触发小米云端控制，本项目会将原生小爱的麦克风输入静音，真实麦克风音频由 `xiaoai-agent` 接管，使用音箱系统 TTS 命令播报回复。
- 无需单独搭建服务器：Agent 直接运行在音箱上，不再依赖独立的 WebSocket 消息桥接层。
- 复用设备原生音频能力：使用固件内置的常驻唤醒和 VPM 音频回调机制，音频体验完美；支持连续对话、VAD、中途打断、回声消除、播放时录音。
- 支持工具和设备控制：使用现代 Agent 框架支撑，内置时间、天气、网络搜索、Navidrome 音乐播放工具，并可通过 Home Assistant MCP 控制智能家居。
- 支持 AirPlay 音频输出：音箱可以作为 AirPlay 音频接收端，播放来自 iPhone、iPad、Mac 等设备的音频流。
- 保留音箱其它系统能力：麦克风输入会被 `xiaoai-agent` 接管，但蓝牙网关等非语音对话服务不受到影响，且 LED 指示灯动态可以自定义控制。

## 代码结构

```text
.
├── xiaoai-agent/              # Rust 编写的音箱端 Agent
├── deploy/client-patch/       # 用于制作带 SSH 和启动钩子的补丁固件
├── deploy/flash-tool/         # macOS 刷机辅助工具
├── deploy/OH2P_1.62.2_BUILD_NOTES.md # OH2P 构建踩坑记录
├── upstream-open-xiaoai/      # 上游 Open-XiaoAI 快照说明和许可证
└── AGENTS.md                  # README 的工程补充说明
```

`deploy/client-patch/`、`deploy/flash-tool/` 和 `upstream-open-xiaoai/` 主要来自其它开源项目。

## 使用流程

### 1. 克隆仓库

```bash
git clone https://github.com/stevenjoezhang/xiaoai-agent.git
cd xiaoai-agent
```

### 2. 重新打包补丁固件

为了在音箱上运行 XiaoAI Agent 程序，需要自行使用本仓库重新打包补丁固件，并刷入带 SSH、启动脚本和音频路径调整的 rootfs。不要直接使用上游 Open-XiaoAI 预构建的 patched 固件；它不包含本项目用于静音原生小爱麦克风输入的补丁。

- 生成补丁固件和刷机：见 [deploy/README.md](deploy/README.md)
- 作者自己 OH2P 1.62.2 构建踩坑记录：见 [deploy/OH2P_1.62.2_BUILD_NOTES.md](deploy/OH2P_1.62.2_BUILD_NOTES.md)

补丁固件会提供 SSH 和 `/data/init.sh` 启动钩子，并让原生小爱的麦克风输入静音，避免与 `xiaoai-agent` 冲突。

### 3. 构建音箱端 Agent

可以直接使用 GitHub Actions 自动构建的 `xiaoai-agent` 程序，从 [Releases](https://github.com/stevenjoezhang/xiaoai-agent/releases) 下载即可。

也可以自行在本地构建。由于音箱端是 ARMv7 Linux，通常需交叉编译。先安装构建工具链：

```bash
rustup toolchain install 1.96.0
rustup target add armv7-unknown-linux-gnueabihf --toolchain 1.96.0
cargo install cargo-zigbuild
```

`cargo-zigbuild` 还需要 Zig。macOS 可以使用 Homebrew 安装：

```bash
brew install zig
```

构建给 OH2P 使用的 ARMv7 Linux 二进制时，使用固定 Rust 版本和 glibc 2.25 目标：

```bash
(cd xiaoai-agent && cargo +1.96.0 zigbuild --release --target armv7-unknown-linux-gnueabihf.2.25)
```

更多交叉编译和 ABI 注意事项见 [AGENTS.md](AGENTS.md)。

### 4. 创建运行配置

为了正常使用，需要准备 ASR 服务和大模型服务 API Key。可选配置包含 Home Assistant Token 等。

```bash
cp xiaoai-agent/agent.example.yaml xiaoai-agent/agent.yaml
```

然后编辑 `xiaoai-agent/agent.yaml`：

- `asr.provider`：ASR 后端，可选 `open_ai`、`openai_realtime` 或
  `xiaomi_aivs`。`open_ai` 使用 OpenAI-compatible HTTP ASR 配置；
  `openai_realtime` 使用 OpenAI Realtime transcription WebSocket 事件协议；
  `xiaomi_aivs` 复用音箱原生 AIVS ASR，并默认发送 ASR-only
  `Execution.RequestControl`，避免云端 NLP/TTS/设备控制副作用。
- `asr.open_ai.base_url`、`asr.open_ai.api_key`、`asr.open_ai.model`：
  OpenAI-compatible ASR 服务配置
- `asr.openai_realtime.base_url`、`asr.openai_realtime.api_key`、
  `asr.openai_realtime.model`：OpenAI Realtime transcription 服务配置；选择
  `openai_realtime` 时，录音链路会在 VAD 采集期间持续发送
  `input_audio_buffer.append`，并在一句话结束后 `commit` 等待最终文本
- `llm.base_url`、`llm.api_key`、`llm.model`：大模型服务配置
- `voice.runtime`：语音运行时。默认 `legacy` 保留现有的 ASR → 文本 Agent →
  设备 TTS 链路；`native_qwen` 改用 Qwen3.5 Omni Realtime 的双向音频链路，
  此时必须配置 `voice.qwen.api_key`，并把 `voice.qwen.url` 中的
  `{WorkspaceId}` 替换为 API Key 所属地域的百炼业务空间 ID
- `voice.qwen`：原生 Qwen Realtime 的 WebRTC 信令地址、模型、音色和音频参数。
  VPM 输入固定为 16 kHz 单声道 S16_LE，传输层编码为 Opus/RTP；模型返回的
  Opus/RTP 音频解码为 48 kHz 单声道 S16_LE 播放。会话使用 `semantic_vad`，
  控制事件和工具调用通过服务端 `txt` DataChannel 传输；
  `tool_timeout_s`、`max_tool_calls` 和 `max_tool_iterations` 为每轮工具执行设置
  超时与上限，防止模型无限递归调用
- `mcp.home_assistant`：Home Assistant MCP 配置；`timeout_s` 除了限制旧版 SSE
  传输，也限制原生 Qwen 启动时的工具发现和后续工具列表刷新。发现或刷新超时后
  连接会关闭并保持 fail-closed，不会回退到可能重复执行的通用 MCP 代理路径
- `music`：音乐服务配置，推荐使用 Navidrome；不需要音乐功能时保持 `music.enabled: false`
- `runtime` / `capture`：唤醒和录音参数，通常先使用示例值
- `airplay`：AirPlay 音频输出配置，默认关闭

选择 `native_qwen` 时，程序会把文本 Agent 已注册的内置工具和 MCP 工具定义
转换为 Qwen Realtime function tools。模型返回函数调用后，程序通过同一个 Rig
`ToolServerHandle` 执行工具，将结构化结果作为 `function_call_output` 写回原会话，
再请求模型继续生成最终语音。`call_id` 在整轮会话中只能使用一次，后续迭代重放会
直接终止该会话且不会再次执行工具；大小限制内的畸形或非对象 JSON 参数不会调用
MCP，而会作为 `invalid_arguments` 结构化错误返回模型。未知工具、MCP 错误和超时也
会作为结构化错误返回模型。工具执行和 WebRTC DataChannel 发送均响应会话取消。
`legacy` 运行时不启用此原生循环，行为保持不变。

#### Web 配置页面

Agent 默认在 `0.0.0.0:8080` 提供 Web 配置页面。在同一局域网内打开音箱的
LAN 地址即可访问，例如 `http://192.168.31.227:8080`；启动参数 `--web-bind`
和 `--web-port` 可以覆盖监听地址和端口。

Web 页面刻意不提供身份认证，只能在可信局域网内使用，切勿把 8080 端口暴露到
互联网。页面会遮蔽所有已有密钥，后端也绝不会把这些密钥返回浏览器。

点击“保存”会先校验配置，再写入 `agent.yaml`，并在同一目录创建备份
`agent.yaml.bak`。保存后不会热加载，新配置需重启 Agent 才会生效。点击“重启服务”
只会替换当前 `xiaoai-agent`（Agent）进程，不会重启音箱，也不会重启或恢复原生
小爱同学。

如果手动编辑 YAML 导致 Agent 无法启动，通过 SSH 执行以下命令恢复备份：

```sh
cp /data/open-xiaoai/agent.yaml.bak /data/open-xiaoai/agent.yaml
```

### 5. 安装到音箱

刷机并确认 SSH 可用后，将 `xiaoai-agent` 二进制程序和配置安装到持久化目录：

```bash
ssh root@<speaker-ip> 'mkdir -p /data/open-xiaoai'

scp -O xiaoai-agent/target/armv7-unknown-linux-gnueabihf/release/xiaoai-agent \
  root@<speaker-ip>:/data/open-xiaoai/xiaoai-agent

scp -O xiaoai-agent/agent.yaml \
  root@<speaker-ip>:/data/open-xiaoai/agent.yaml

ssh root@<speaker-ip> 'chmod +x /data/open-xiaoai/xiaoai-agent'
```

通过 SSH 登录音箱后，先手动运行，确认唤醒、录音、ASR、大模型回复和 TTS 都正常：

```sh
RUST_LOG=debug /data/open-xiaoai/xiaoai-agent -c /data/open-xiaoai/agent.yaml
```

确认后，在音箱上写入 `/data/init.sh` 开机自启：

```sh
cat >/data/init.sh <<'EOF'
#!/bin/sh
RUST_LOG=info /data/open-xiaoai/xiaoai-agent -c /data/open-xiaoai/agent.yaml >>/data/open-xiaoai/xiaoai-agent.log 2>&1 &
EOF
chmod +x /data/init.sh
```

## 运行原理

Agent 启动后会常驻运行：

1. 使用固件原生 VPM/FlexKWS 监听唤醒词。
2. 每次唤醒都会中断当前语音输出或音乐播放，并重置当前对话轮次。
3. 从 VPM ASR 回调流采集一段 16 kHz 单声道音频。
4. 使用配置的 ASR 后端识别文本，可选 OpenAI-compatible HTTP ASR、OpenAI
   Realtime transcription 或原生 Xiaomi AIVS ASR。
5. 把识别文本交给端侧 Rig Agent，并按需调用 MCP、天气、音乐等工具。
6. 使用小爱音箱系统 TTS 命令朗读回复。

## TODO

- [ ] 支持音箱按键控制

## 免责声明

本项目为非官方技术研究项目，与小米及其关联公司不存在任何隶属、合作、授权、认可或背书关系。

使用者应自行确认其使用行为符合适用法律法规、平台规则、设备厂商政策及相关服务协议，并自行承担由下载、安装、配置、修改、传播或使用本项目所产生的全部风险与责任。

详细免责声明请见 [DISCLAIMER.md](./DISCLAIMER.md)。项目授权与分发条件以仓库中的 [LICENSE](./LICENSE) 文件为准。

## 许可证和来源

本仓库包含本项目自研的 `xiaoai-agent/`，也包含来自 Open-XiaoAI 等项目的部署辅助材料。上游材料的来源和许可证见 [upstream-open-xiaoai/](upstream-open-xiaoai/)。
