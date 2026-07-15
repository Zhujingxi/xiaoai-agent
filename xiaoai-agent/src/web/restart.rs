use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub trait RestartController: Send + Sync {
    fn schedule_restart(&self) -> Result<(), RestartError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RestartError {
    #[error("restart is already scheduled")]
    AlreadyScheduled,
    #[error("self restart is supported only on Unix targets")]
    Unsupported,
    #[error("failed to locate current executable: {0}")]
    CurrentExe(#[source] std::io::Error),
    #[error("failed to start restart worker: {0}")]
    Spawn(#[source] std::io::Error),
}

pub struct ProcessRestarter {
    executable: PathBuf,
    arguments: Vec<OsString>,
    delay: Duration,
    scheduled: AtomicBool,
}

impl ProcessRestarter {
    pub fn current() -> Result<Self, RestartError> {
        let executable = std::env::current_exe().map_err(RestartError::CurrentExe)?;
        let arguments = std::env::args_os().skip(1).collect();

        Ok(Self {
            executable,
            arguments,
            delay: Duration::from_millis(500),
            scheduled: AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    fn new_for_test() -> Self {
        Self {
            executable: PathBuf::new(),
            arguments: Vec::new(),
            delay: Duration::from_millis(500),
            scheduled: AtomicBool::new(false),
        }
    }

    fn claim_restart(&self) -> Result<(), RestartError> {
        self.scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| RestartError::AlreadyScheduled)
    }
}

impl RestartController for ProcessRestarter {
    fn schedule_restart(&self) -> Result<(), RestartError> {
        #[cfg(not(unix))]
        {
            return Err(RestartError::Unsupported);
        }

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;

            self.claim_restart()?;

            let executable = self.executable.clone();
            let arguments = self.arguments.clone();
            let delay = self.delay;
            let worker = std::thread::Builder::new()
                .name("xiaoai-restart".into())
                .spawn(move || {
                    std::thread::sleep(delay);
                    let error = std::process::Command::new(executable)
                        .args(arguments)
                        .exec();
                    eprintln!("failed to restart agent: {error}");
                    std::process::exit(1);
                });

            if let Err(error) = worker {
                self.scheduled.store(false, Ordering::Release);
                return Err(RestartError::Spawn(error));
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProcessRestarter, RestartError};

    #[test]
    fn only_first_restart_request_is_accepted() {
        let restarter = ProcessRestarter::new_for_test();
        assert!(restarter.claim_restart().is_ok());
        assert!(matches!(
            restarter.claim_restart(),
            Err(RestartError::AlreadyScheduled)
        ));
    }
}
