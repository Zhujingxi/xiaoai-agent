use std::future::Future;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::base::AppError;

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

const SHELL_PIPE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);

tokio::task_local! {
    static TOOL_CANCELLATION: watch::Receiver<bool>;
}

pub async fn with_tool_cancellation<F: Future>(
    cancellation: watch::Receiver<bool>,
    operation: F,
) -> F::Output {
    TOOL_CANCELLATION.scope(cancellation, operation).await
}

struct ProcessGroupGuard {
    pgid: nix::unistd::Pid,
    armed: bool,
}

impl ProcessGroupGuard {
    fn terminate(&self) {
        let _ = nix::sys::signal::killpg(self.pgid, nix::sys::signal::Signal::SIGKILL);
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if self.armed {
            self.terminate();
        }
    }
}

async fn join_pipe(task: JoinHandle<std::io::Result<Vec<u8>>>) -> std::io::Result<Vec<u8>> {
    timeout(SHELL_PIPE_CLEANUP_TIMEOUT, task)
        .await
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "shell pipe cleanup timed out")
        })?
        .map_err(|err| std::io::Error::other(format!("shell pipe task failed: {err}")))?
}

pub async fn run_shell(script: &str) -> Result<CommandResult, AppError> {
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg(script)
        .process_group(0)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| std::io::Error::other("spawned shell has no process ID"))?;
    let mut process_group = ProcessGroupGuard {
        pgid: nix::unistd::Pid::from_raw(pid as i32),
        armed: true,
    };
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("shell stdout pipe missing"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| std::io::Error::other("shell stderr pipe missing"))?;
    let stdout_task = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).await?;
        Ok(bytes)
    });
    let stderr_task = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).await?;
        Ok(bytes)
    });

    let mut cancellation = TOOL_CANCELLATION.try_with(Clone::clone).ok();
    let status = if let Some(cancel_rx) = &mut cancellation {
        tokio::select! {
            biased;
            changed = cancel_rx.changed() => {
                if changed.is_err() || *cancel_rx.borrow() {
                    process_group.terminate();
                    let _ = child.wait().await;
                    let _ = join_pipe(stdout_task).await;
                    let _ = join_pipe(stderr_task).await;
                    process_group.disarm();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "shell command cancelled",
                    ).into());
                }
                child.wait().await?
            }
            status = child.wait() => status?,
        }
    } else {
        child.wait().await?
    };

    let stdout = String::from_utf8_lossy(&join_pipe(stdout_task).await?).to_string();
    let stderr = String::from_utf8_lossy(&join_pipe(stderr_task).await?).to_string();
    process_group.disarm();
    let exit_code = status.code().unwrap_or(-1);

    Ok(CommandResult {
        stdout,
        stderr,
        exit_code,
    })
}
