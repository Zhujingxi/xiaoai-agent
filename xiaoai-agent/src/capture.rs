use std::future::Future;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use tracing::{debug, info, warn};

use crate::audio::config::AudioConfig;
use crate::audio::record::AudioRecorder;
use crate::config::CaptureConfig;
use crate::monitor::kws::{request_vpm_status, subscribe_vpm_asr_audio};
use crate::vad::{SpeechCollector, SpeechEvent};

const VPM_STATUS_ASR_START: i32 = 2;
const VPM_STATUS_ASR_END: i32 = 3;

struct VpmAsrGuard {
    set_status: fn(i32) -> bool,
}

impl VpmAsrGuard {
    fn start() -> Self {
        if !request_vpm_status(VPM_STATUS_ASR_START) {
            warn!("failed to request VPM ASR_START status");
        }
        Self {
            set_status: request_vpm_status,
        }
    }
}

impl Drop for VpmAsrGuard {
    fn drop(&mut self) {
        if !(self.set_status)(VPM_STATUS_ASR_END) {
            warn!("failed to request VPM ASR_END status");
        }
    }
}

pub async fn record_utterance<F, Fut>(
    config: CaptureConfig,
    idle_timeout: Duration,
    on_speech_start: F,
) -> anyhow::Result<Vec<u8>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    if is_vpm_asr_capture(&config.pcm) {
        return record_vpm_asr_utterance(config, idle_timeout, on_speech_start).await;
    }

    let audio_config = AudioConfig {
        pcm: config.pcm.clone(),
        channels: config.channels,
        bits_per_sample: config.bits_per_sample,
        sample_rate: config.sample_rate,
        period_size: config.period_size,
        buffer_size: config.buffer_size,
    };
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(32);
    AudioRecorder::instance()
        .start_recording(
            move |bytes| {
                let tx = tx.clone();
                async move {
                    tx.send(bytes).await.map_err(|err| err.to_string())?;
                    Ok(())
                }
            },
            Some(audio_config),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))?;

    let idle_timeout = idle_timeout.max(Duration::from_secs(1));
    let idle_timer = sleep(idle_timeout);
    tokio::pin!(idle_timer);
    let mut collector = SpeechCollector::new(&config);
    let mut speech_started = false;

    loop {
        tokio::select! {
            _ = &mut idle_timer, if !speech_started => {
                let _ = AudioRecorder::instance().stop_recording().await;
                anyhow::bail!("timed out waiting for user speech");
            }
            bytes = rx.recv() => {
                let Some(bytes) = bytes else {
                    let _ = AudioRecorder::instance().stop_recording().await;
                    anyhow::bail!("audio recorder stopped before utterance was captured");
                };
                for event in collector.push(&bytes) {
                    match event {
                        SpeechEvent::SpeechStart => {
                            speech_started = true;
                            on_speech_start().await;
                        }
                        SpeechEvent::SpeechRejected => {
                            speech_started = false;
                            idle_timer.as_mut().reset(Instant::now() + idle_timeout);
                        }
                        SpeechEvent::Utterance(pcm) => {
                            let _ = AudioRecorder::instance().stop_recording().await;
                            return Ok(pcm);
                        }
                    }
                }
            }
        }
    }
}

pub async fn record_utterance_streaming<F, Fut, C, CFut, R, RFut>(
    config: CaptureConfig,
    idle_timeout: Duration,
    on_speech_start: F,
    on_audio_chunk: C,
    on_speech_rejected: R,
) -> anyhow::Result<Vec<u8>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
    C: Fn(Vec<u8>) -> CFut + Send + Sync + 'static,
    CFut: Future<Output = anyhow::Result<()>> + Send + 'static,
    R: Fn() -> RFut + Send + Sync + 'static,
    RFut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    if is_vpm_asr_capture(&config.pcm) {
        return record_vpm_asr_utterance_streaming(
            config,
            idle_timeout,
            on_speech_start,
            on_audio_chunk,
            on_speech_rejected,
        )
        .await;
    }

    let audio_config = AudioConfig {
        pcm: config.pcm.clone(),
        channels: config.channels,
        bits_per_sample: config.bits_per_sample,
        sample_rate: config.sample_rate,
        period_size: config.period_size,
        buffer_size: config.buffer_size,
    };
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(32);
    AudioRecorder::instance()
        .start_recording(
            move |bytes| {
                let tx = tx.clone();
                async move {
                    tx.send(bytes).await.map_err(|err| err.to_string())?;
                    Ok(())
                }
            },
            Some(audio_config),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))?;

    let result = collect_recorded_streaming_utterance(
        config,
        idle_timeout,
        on_speech_start,
        on_audio_chunk,
        on_speech_rejected,
        &mut rx,
    )
    .await;
    let _ = AudioRecorder::instance().stop_recording().await;
    result
}

/// Keep the configured capture backend open and forward PCM until the task is
/// cancelled or the backend fails. Realtime transports use their server-side
/// VAD to split this continuous stream into conversational turns.
pub async fn stream_audio_continuously<C, Fut>(
    config: CaptureConfig,
    on_audio_chunk: C,
) -> anyhow::Result<()>
where
    C: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    if is_vpm_asr_capture(&config.pcm) {
        let rx = subscribe_vpm_asr_audio()
            .context("VPM ASR audio stream is unavailable; native KWS monitor is not ready")?;
        let _asr_guard = VpmAsrGuard::start();
        info!("CAPTURE_BACKEND backend=vpm_asr continuous=true");
        return forward_vpm_audio(rx, on_audio_chunk).await;
    }

    let audio_config = AudioConfig {
        pcm: config.pcm,
        channels: config.channels,
        bits_per_sample: config.bits_per_sample,
        sample_rate: config.sample_rate,
        period_size: config.period_size,
        buffer_size: config.buffer_size,
    };
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(32);
    AudioRecorder::instance()
        .start_recording(
            move |bytes| {
                let tx = tx.clone();
                async move {
                    tx.send(bytes).await.map_err(|err| err.to_string())?;
                    Ok(())
                }
            },
            Some(audio_config),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))?;

    info!("CAPTURE_BACKEND backend=alsa continuous=true");
    let result = async {
        while let Some(bytes) = rx.recv().await {
            on_audio_chunk(bytes).await?;
        }
        anyhow::bail!("audio recorder stopped during continuous capture")
    }
    .await;
    let _ = AudioRecorder::instance().stop_recording().await;
    result
}

async fn forward_vpm_audio<C, Fut>(
    mut rx: broadcast::Receiver<Vec<u8>>,
    on_audio_chunk: C,
) -> anyhow::Result<()>
where
    C: Fn(Vec<u8>) -> Fut + Send + Sync,
    Fut: Future<Output = anyhow::Result<()>> + Send,
{
    let mut chunks = 0u64;
    loop {
        let bytes = match rx.recv().await {
            Ok(bytes) => bytes,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(
                    skipped,
                    "lagged while continuously reading VPM ASR audio stream"
                );
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                anyhow::bail!("VPM ASR audio stream stopped during continuous capture")
            }
        };
        chunks += 1;
        if chunks == 1 {
            debug!(bytes = bytes.len(), "VPM_ASR_FIRST_CONTINUOUS_CHUNK");
        }
        on_audio_chunk(bytes).await?;
    }
}

async fn record_vpm_asr_utterance<F, Fut>(
    config: CaptureConfig,
    idle_timeout: Duration,
    on_speech_start: F,
) -> anyhow::Result<Vec<u8>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let rx = subscribe_vpm_asr_audio()
        .context("VPM ASR audio stream is unavailable; native KWS monitor is not ready")?;
    let _asr_guard = VpmAsrGuard::start();
    collect_vpm_asr_utterance(config, idle_timeout, on_speech_start, rx).await
}

async fn record_vpm_asr_utterance_streaming<F, Fut, C, CFut, R, RFut>(
    config: CaptureConfig,
    idle_timeout: Duration,
    on_speech_start: F,
    on_audio_chunk: C,
    on_speech_rejected: R,
) -> anyhow::Result<Vec<u8>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
    C: Fn(Vec<u8>) -> CFut + Send + Sync + 'static,
    CFut: Future<Output = anyhow::Result<()>> + Send + 'static,
    R: Fn() -> RFut + Send + Sync + 'static,
    RFut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let rx = subscribe_vpm_asr_audio()
        .context("VPM ASR audio stream is unavailable; native KWS monitor is not ready")?;
    let _asr_guard = VpmAsrGuard::start();
    collect_vpm_asr_utterance_streaming(
        config,
        idle_timeout,
        on_speech_start,
        on_audio_chunk,
        on_speech_rejected,
        rx,
    )
    .await
}

async fn collect_vpm_asr_utterance<F, Fut>(
    config: CaptureConfig,
    idle_timeout: Duration,
    on_speech_start: F,
    mut rx: broadcast::Receiver<Vec<u8>>,
) -> anyhow::Result<Vec<u8>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let idle_timeout = idle_timeout.max(Duration::from_secs(1));
    let idle_timer = sleep(idle_timeout);
    tokio::pin!(idle_timer);
    let mut collector = SpeechCollector::new(&config);
    let mut speech_started = false;
    let mut chunks = 0u64;
    info!("CAPTURE_BACKEND backend=vpm_asr");

    loop {
        tokio::select! {
            _ = &mut idle_timer, if !speech_started => {
                anyhow::bail!("timed out waiting for user speech");
            }
            chunk = rx.recv() => {
                let bytes = match chunk {
                    Ok(bytes) => bytes,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "lagged while reading VPM ASR audio stream");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        anyhow::bail!("VPM ASR audio stream stopped before utterance was captured");
                    }
                };
                chunks += 1;
                if chunks == 1 {
                    debug!(bytes = bytes.len(), "VPM_ASR_FIRST_CHUNK");
                }
                for event in collector.push(&bytes) {
                    match event {
                        SpeechEvent::SpeechStart => {
                            speech_started = true;
                            on_speech_start().await;
                        }
                        SpeechEvent::SpeechRejected => {
                            speech_started = false;
                            idle_timer.as_mut().reset(Instant::now() + idle_timeout);
                        }
                        SpeechEvent::Utterance(pcm) => return Ok(pcm),
                    }
                }
            }
        }
    }
}

async fn collect_recorded_streaming_utterance<F, Fut, C, CFut, R, RFut>(
    config: CaptureConfig,
    idle_timeout: Duration,
    on_speech_start: F,
    on_audio_chunk: C,
    on_speech_rejected: R,
    rx: &mut mpsc::Receiver<Vec<u8>>,
) -> anyhow::Result<Vec<u8>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
    C: Fn(Vec<u8>) -> CFut + Send + Sync + 'static,
    CFut: Future<Output = anyhow::Result<()>> + Send + 'static,
    R: Fn() -> RFut + Send + Sync + 'static,
    RFut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let idle_timeout = idle_timeout.max(Duration::from_secs(1));
    let idle_timer = sleep(idle_timeout);
    tokio::pin!(idle_timer);
    let mut collector = SpeechCollector::new(&config);
    let mut speech_started = false;

    loop {
        tokio::select! {
            _ = &mut idle_timer, if !speech_started => {
                anyhow::bail!("timed out waiting for user speech");
            }
            bytes = rx.recv() => {
                let Some(bytes) = bytes else {
                    anyhow::bail!("audio recorder stopped before utterance was captured");
                };
                on_audio_chunk(bytes.clone()).await?;
                for event in collector.push(&bytes) {
                    match event {
                        SpeechEvent::SpeechStart => {
                            speech_started = true;
                            on_speech_start().await;
                        }
                        SpeechEvent::SpeechRejected => {
                            speech_started = false;
                            on_speech_rejected().await?;
                            idle_timer.as_mut().reset(Instant::now() + idle_timeout);
                        }
                        SpeechEvent::Utterance(pcm) => return Ok(pcm),
                    }
                }
            }
        }
    }
}

async fn collect_vpm_asr_utterance_streaming<F, Fut, C, CFut, R, RFut>(
    config: CaptureConfig,
    idle_timeout: Duration,
    on_speech_start: F,
    on_audio_chunk: C,
    on_speech_rejected: R,
    mut rx: broadcast::Receiver<Vec<u8>>,
) -> anyhow::Result<Vec<u8>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
    C: Fn(Vec<u8>) -> CFut + Send + Sync + 'static,
    CFut: Future<Output = anyhow::Result<()>> + Send + 'static,
    R: Fn() -> RFut + Send + Sync + 'static,
    RFut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let idle_timeout = idle_timeout.max(Duration::from_secs(1));
    let idle_timer = sleep(idle_timeout);
    tokio::pin!(idle_timer);
    let mut collector = SpeechCollector::new(&config);
    let mut speech_started = false;
    let mut chunks = 0u64;
    info!("CAPTURE_BACKEND backend=vpm_asr streaming=true");

    loop {
        tokio::select! {
            _ = &mut idle_timer, if !speech_started => {
                anyhow::bail!("timed out waiting for user speech");
            }
            chunk = rx.recv() => {
                let bytes = match chunk {
                    Ok(bytes) => bytes,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "lagged while reading VPM ASR audio stream");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        anyhow::bail!("VPM ASR audio stream stopped before utterance was captured");
                    }
                };
                chunks += 1;
                if chunks == 1 {
                    debug!(bytes = bytes.len(), "VPM_ASR_FIRST_CHUNK");
                }
                on_audio_chunk(bytes.clone()).await?;
                for event in collector.push(&bytes) {
                    match event {
                        SpeechEvent::SpeechStart => {
                            speech_started = true;
                            on_speech_start().await;
                        }
                        SpeechEvent::SpeechRejected => {
                            speech_started = false;
                            on_speech_rejected().await?;
                            idle_timer.as_mut().reset(Instant::now() + idle_timeout);
                        }
                        SpeechEvent::Utterance(pcm) => return Ok(pcm),
                    }
                }
            }
        }
    }
}

fn is_vpm_asr_capture(pcm: &str) -> bool {
    matches!(pcm.trim(), "vpm_asr" | "vpm")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    static LAST_STATUS: AtomicI32 = AtomicI32::new(0);
    static END_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn record_status(status: i32) -> bool {
        LAST_STATUS.store(status, Ordering::SeqCst);
        if status == VPM_STATUS_ASR_END {
            END_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        true
    }

    #[tokio::test]
    async fn real_vpm_stream_abort_sends_asr_end_once_before_join_completes() {
        LAST_STATUS.store(0, Ordering::SeqCst);
        END_COUNT.store(0, Ordering::SeqCst);
        let (_audio_tx, audio_rx) = broadcast::channel(1);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = VpmAsrGuard {
                set_status: record_status,
            };
            started_tx.send(()).unwrap();
            collect_vpm_asr_utterance_streaming(
                CaptureConfig::default(),
                Duration::from_secs(30),
                || async {},
                |_bytes| async { Ok(()) },
                || async { Ok(()) },
                audio_rx,
            )
            .await
        });

        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(LAST_STATUS.load(Ordering::SeqCst), VPM_STATUS_ASR_END);
        assert_eq!(END_COUNT.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn continuous_vpm_stream_forwards_multiple_chunks() {
        let (audio_tx, audio_rx) = broadcast::channel(4);
        let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let observed = received.clone();
        let task = tokio::spawn(async move {
            forward_vpm_audio(audio_rx, move |bytes| {
                let observed = observed.clone();
                async move {
                    observed.lock().unwrap().push(bytes.clone());
                    if bytes == [2] {
                        anyhow::bail!("test stream complete");
                    }
                    Ok(())
                }
            })
            .await
        });

        audio_tx.send(vec![1]).unwrap();
        audio_tx.send(vec![2]).unwrap();

        assert!(task.await.unwrap().is_err());
        assert_eq!(*received.lock().unwrap(), [vec![1], vec![2]]);
    }
}
