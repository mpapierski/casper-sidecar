use std::{collections::HashMap, net::TcpListener};

use anyhow::{Context, Error, anyhow, bail};

const LISTEN_PID_ENV_VAR: &str = "LISTEN_PID";
const LISTEN_FDS_ENV_VAR: &str = "LISTEN_FDS";
const LISTEN_FDNAMES_ENV_VAR: &str = "LISTEN_FDNAMES";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ActivationName {
    RpcMain,
    RpcSpeculative,
    SsePublish,
    RestApi,
    AdminApi,
}

impl ActivationName {
    fn from_fd_name(value: &str) -> Option<Self> {
        match value {
            "rpc-main" => Some(Self::RpcMain),
            "rpc-speculative" => Some(Self::RpcSpeculative),
            "sse-publish" => Some(Self::SsePublish),
            "rest-api" => Some(Self::RestApi),
            "admin-api" => Some(Self::AdminApi),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::RpcMain => "rpc-main",
            Self::RpcSpeculative => "rpc-speculative",
            Self::SsePublish => "sse-publish",
            Self::RestApi => "rest-api",
            Self::AdminApi => "admin-api",
        }
    }
}

#[derive(Default)]
pub(crate) struct ActivationSockets {
    sockets: HashMap<ActivationName, TcpListener>,
}

impl ActivationSockets {
    #[cfg(unix)]
    pub(crate) fn parse_from_env() -> Result<Self, Error> {
        let snapshot = ActivationEnv::snapshot_from_process_env()?;
        Self::from_snapshot(snapshot, std::process::id())
    }

    #[cfg(not(unix))]
    pub(crate) fn parse_from_env() -> Result<Self, Error> {
        Ok(Self::default())
    }

    pub(crate) fn take(&mut self, name: ActivationName) -> Option<TcpListener> {
        self.sockets.remove(&name)
    }

    #[cfg(unix)]
    fn from_snapshot(snapshot: ActivationEnv, process_id: u32) -> Result<Self, Error> {
        let Some(fd_names) = parse_activation_env(&snapshot, process_id)? else {
            return Ok(Self::default());
        };
        let owned_fds = owned_fds_from_raw(fd_names.len())?;
        Self::from_fd_names_and_owned_fds(fd_names, owned_fds)
    }

    #[cfg(all(test, unix))]
    fn from_snapshot_and_owned_fds(
        snapshot: ActivationEnv,
        process_id: u32,
        owned_fds: Vec<std::os::fd::OwnedFd>,
    ) -> Result<Self, Error> {
        let Some(fd_names) = parse_activation_env(&snapshot, process_id)? else {
            return Ok(Self::default());
        };
        Self::from_fd_names_and_owned_fds(fd_names, owned_fds)
    }

    #[cfg(unix)]
    fn from_fd_names_and_owned_fds(
        fd_names: Vec<Option<ActivationName>>,
        owned_fds: Vec<std::os::fd::OwnedFd>,
    ) -> Result<Self, Error> {
        if fd_names.len() != owned_fds.len() {
            bail!(
                "socket activation expected {} file descriptors but received {}",
                fd_names.len(),
                owned_fds.len()
            );
        }

        let mut sockets = HashMap::new();
        for (maybe_name, owned_fd) in fd_names.into_iter().zip(owned_fds) {
            let Some(name) = maybe_name else {
                continue;
            };

            validate_listener_socket(&owned_fd, name)?;
            let listener: TcpListener = owned_fd.into();
            listener.set_nonblocking(true).with_context(|| {
                format!("failed to set {} listener to nonblocking", name.as_str())
            })?;
            listener.local_addr().with_context(|| {
                format!(
                    "failed to inspect local address for inherited listener {}",
                    name.as_str()
                )
            })?;
            sockets.insert(name, listener);
        }

        Ok(Self { sockets })
    }
}

#[derive(Debug, Default)]
struct ActivationEnv {
    listen_pid: Option<String>,
    listen_fds: Option<String>,
    listen_fdnames: Option<String>,
}

#[cfg(unix)]
impl ActivationEnv {
    fn snapshot_from_process_env() -> Result<Self, Error> {
        let listen_pid = read_env_var(LISTEN_PID_ENV_VAR)?;
        let listen_fds = read_env_var(LISTEN_FDS_ENV_VAR)?;
        let listen_fdnames = read_env_var(LISTEN_FDNAMES_ENV_VAR)?;

        unsafe {
            std::env::remove_var(LISTEN_PID_ENV_VAR);
            std::env::remove_var(LISTEN_FDS_ENV_VAR);
            std::env::remove_var(LISTEN_FDNAMES_ENV_VAR);
        }

        Ok(Self {
            listen_pid,
            listen_fds,
            listen_fdnames,
        })
    }
}

#[cfg(unix)]
fn read_env_var(key: &str) -> Result<Option<String>, Error> {
    match std::env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(anyhow!(
            "socket activation env var {key} is not valid UTF-8"
        )),
    }
}

#[cfg(unix)]
fn parse_activation_env(
    snapshot: &ActivationEnv,
    process_id: u32,
) -> Result<Option<Vec<Option<ActivationName>>>, Error> {
    if snapshot.listen_pid.is_none()
        && snapshot.listen_fds.is_none()
        && snapshot.listen_fdnames.is_none()
    {
        return Ok(None);
    }

    let listen_pid = parse_required_u32(snapshot.listen_pid.as_deref(), LISTEN_PID_ENV_VAR)?;
    if listen_pid != process_id {
        return Ok(None);
    }

    let listen_fds = parse_required_usize(snapshot.listen_fds.as_deref(), LISTEN_FDS_ENV_VAR)?;
    if listen_fds == 0 {
        return Ok(None);
    }

    let fdnames = snapshot
        .listen_fdnames
        .as_deref()
        .ok_or_else(|| anyhow!("socket activation env var {LISTEN_FDNAMES_ENV_VAR} is required"))?;
    let split_names = fdnames.split(':').collect::<Vec<_>>();
    if split_names.len() != listen_fds {
        bail!(
            "socket activation expected {listen_fds} entries in {LISTEN_FDNAMES_ENV_VAR} but received {}",
            split_names.len()
        );
    }

    let mut seen_supported_names = std::collections::HashSet::new();
    let mut parsed_names = Vec::with_capacity(listen_fds);
    for fd_name in split_names {
        if fd_name.is_empty() {
            bail!("socket activation does not allow empty entries in {LISTEN_FDNAMES_ENV_VAR}");
        }

        let maybe_name = ActivationName::from_fd_name(fd_name);
        if let Some(name) = maybe_name {
            if !seen_supported_names.insert(name) {
                bail!(
                    "socket activation received duplicate listener name {}",
                    name.as_str()
                );
            }
        }
        parsed_names.push(maybe_name);
    }

    Ok(Some(parsed_names))
}

#[cfg(unix)]
fn parse_required_u32(value: Option<&str>, key: &str) -> Result<u32, Error> {
    let value = value.ok_or_else(|| anyhow!("socket activation env var {key} is required"))?;
    value
        .parse()
        .with_context(|| format!("failed to parse socket activation env var {key}"))
}

#[cfg(unix)]
fn parse_required_usize(value: Option<&str>, key: &str) -> Result<usize, Error> {
    let value = value.ok_or_else(|| anyhow!("socket activation env var {key} is required"))?;
    value
        .parse()
        .with_context(|| format!("failed to parse socket activation env var {key}"))
}

#[cfg(unix)]
fn owned_fds_from_raw(count: usize) -> Result<Vec<std::os::fd::OwnedFd>, Error> {
    use std::os::fd::{FromRawFd, OwnedFd, RawFd};

    let mut owned_fds = Vec::with_capacity(count);
    for offset in 0..count {
        let raw_fd = 3 + offset as RawFd;
        ensure_fd_is_open(raw_fd)?;
        let owned_fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        owned_fds.push(owned_fd);
    }
    Ok(owned_fds)
}

#[cfg(unix)]
fn ensure_fd_is_open(raw_fd: std::os::fd::RawFd) -> Result<(), Error> {
    use std::os::fd::BorrowedFd;

    use nix::fcntl::{FcntlArg, fcntl};

    // SAFETY: LISTEN_FDS refers to inherited descriptors that are expected to
    // remain valid for the lifetime of the process.
    let borrowed_fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };
    fcntl(borrowed_fd, FcntlArg::F_GETFD)
        .map(|_| ())
        .with_context(|| format!("socket activation file descriptor {raw_fd} is not open"))
}

#[cfg(unix)]
fn validate_listener_socket(
    owned_fd: &std::os::fd::OwnedFd,
    name: ActivationName,
) -> Result<(), Error> {
    use nix::{
        errno::Errno,
        sys::socket::{SockType, getsockopt, sockopt},
    };

    let socket_type = getsockopt(owned_fd, sockopt::SockType).with_context(|| {
        format!(
            "failed to inspect socket type for inherited listener {}",
            name.as_str()
        )
    })?;
    if socket_type != SockType::Stream {
        bail!("inherited listener {} is not a TCP socket", name.as_str());
    }

    let accept_conn = match getsockopt(owned_fd, sockopt::AcceptConn) {
        Ok(accept_conn) => accept_conn,
        Err(Errno::ENOPROTOOPT) => {
            return Ok(());
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect listen state for inherited listener {}",
                    name.as_str()
                )
            });
        }
    };
    if !accept_conn {
        bail!(
            "inherited listener {} is not in a listening state",
            name.as_str()
        );
    }

    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        net::TcpListener,
        os::fd::OwnedFd,
        sync::{Mutex, OnceLock},
    };

    use super::*;

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn snapshot(
        listen_pid: Option<String>,
        listen_fds: Option<String>,
        listen_fdnames: Option<String>,
    ) -> ActivationEnv {
        ActivationEnv {
            listen_pid,
            listen_fds,
            listen_fdnames,
        }
    }

    fn listener_fd() -> OwnedFd {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.into()
    }

    #[test]
    fn should_parse_multiple_named_fds() {
        let sockets = ActivationSockets::from_snapshot_and_owned_fds(
            snapshot(
                Some(std::process::id().to_string()),
                Some("2".to_string()),
                Some("rpc-main:admin-api".to_string()),
            ),
            std::process::id(),
            vec![listener_fd(), listener_fd()],
        )
        .unwrap();

        assert!(sockets.sockets.contains_key(&ActivationName::RpcMain));
        assert!(sockets.sockets.contains_key(&ActivationName::AdminApi));
    }

    #[test]
    fn should_support_multiple_recognized_named_fds() {
        let sockets = ActivationSockets::from_snapshot_and_owned_fds(
            snapshot(
                Some(std::process::id().to_string()),
                Some("3".to_string()),
                Some("rpc-main:sse-publish:rpc-speculative".to_string()),
            ),
            std::process::id(),
            vec![listener_fd(), listener_fd(), listener_fd()],
        )
        .unwrap();

        assert!(sockets.sockets.contains_key(&ActivationName::RpcMain));
        assert!(sockets.sockets.contains_key(&ActivationName::SsePublish));
        assert!(
            sockets
                .sockets
                .contains_key(&ActivationName::RpcSpeculative)
        );
    }

    #[test]
    fn should_ignore_unknown_fd_names() {
        let sockets = ActivationSockets::from_snapshot_and_owned_fds(
            snapshot(
                Some(std::process::id().to_string()),
                Some("3".to_string()),
                Some("rpc-main:unknown-name:admin-api".to_string()),
            ),
            std::process::id(),
            vec![listener_fd(), listener_fd(), listener_fd()],
        )
        .unwrap();

        assert!(sockets.sockets.contains_key(&ActivationName::RpcMain));
        assert!(sockets.sockets.contains_key(&ActivationName::AdminApi));
        assert_eq!(sockets.sockets.len(), 2);
    }

    #[test]
    fn should_fail_on_bad_listen_pid() {
        let result = ActivationSockets::from_snapshot_and_owned_fds(
            snapshot(
                Some("abc".to_string()),
                Some("1".to_string()),
                Some("rpc-main".to_string()),
            ),
            std::process::id(),
            Vec::new(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn should_fail_on_bad_listen_fds() {
        let result = ActivationSockets::from_snapshot_and_owned_fds(
            snapshot(
                Some(std::process::id().to_string()),
                Some("abc".to_string()),
                Some("rpc-main".to_string()),
            ),
            std::process::id(),
            Vec::new(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn should_fail_when_listen_fdnames_is_missing() {
        let result = ActivationSockets::from_snapshot_and_owned_fds(
            snapshot(
                Some(std::process::id().to_string()),
                Some("1".to_string()),
                None,
            ),
            std::process::id(),
            Vec::new(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn should_fail_when_fdnames_count_mismatches_fds() {
        let result = ActivationSockets::from_snapshot_and_owned_fds(
            snapshot(
                Some(std::process::id().to_string()),
                Some("2".to_string()),
                Some("rpc-main".to_string()),
            ),
            std::process::id(),
            Vec::new(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn should_fail_on_duplicate_supported_fd_names() {
        let result = ActivationSockets::from_snapshot_and_owned_fds(
            snapshot(
                Some(std::process::id().to_string()),
                Some("2".to_string()),
                Some("rpc-main:rpc-main".to_string()),
            ),
            std::process::id(),
            Vec::new(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn should_return_empty_when_pid_does_not_match() {
        let sockets = ActivationSockets::from_snapshot_and_owned_fds(
            snapshot(
                Some((std::process::id() + 1).to_string()),
                Some("1".to_string()),
                Some("rpc-main".to_string()),
            ),
            std::process::id(),
            Vec::new(),
        )
        .unwrap();

        assert!(sockets.sockets.is_empty());
    }

    #[test]
    fn should_unset_activation_env_vars_after_snapshot() {
        let _guard = env_lock().lock().unwrap();

        unsafe {
            std::env::set_var(LISTEN_PID_ENV_VAR, std::process::id().to_string());
            std::env::set_var(LISTEN_FDS_ENV_VAR, "0");
            std::env::remove_var(LISTEN_FDNAMES_ENV_VAR);
        }

        let sockets = ActivationSockets::parse_from_env().unwrap();

        assert!(sockets.sockets.is_empty());
        assert!(std::env::var_os(LISTEN_PID_ENV_VAR).is_none());
        assert!(std::env::var_os(LISTEN_FDS_ENV_VAR).is_none());
        assert!(std::env::var_os(LISTEN_FDNAMES_ENV_VAR).is_none());
    }
}
