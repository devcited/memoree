//! Length-prefixed local transport for the machine protocol.

use std::{
    fs::{self, OpenOptions},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

use crate::{
    error::{MemoryError, Result},
    protocol::{MAX_FRAME_BYTES, Request, Response},
    service::MemoryService,
};

// The service serializes SQLite writes and a single frame may be 24 MiB, so a
// deliberately small local concurrency ceiling gives multi-agent callers room
// without allowing many maximum-sized allocations at once.
pub const MAX_CONCURRENT_CONNECTIONS: usize = 4;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const FRAME_READ_TIMEOUT: Duration = Duration::from_secs(30);
const FRAME_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const REQUEST_IO_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport", content = "address", rename_all = "snake_case")]
pub enum Endpoint {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

/// Server-side network exposure policy. The protocol has no authentication in
/// the local personal release, so non-loopback TCP must be a deliberate opt-in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServePolicy {
    pub dangerously_allow_non_loopback_tcp: bool,
}

impl Endpoint {
    pub fn parse(value: &str) -> Result<Self> {
        if let Some(path) = value.strip_prefix("unix://") {
            if path.is_empty() {
                return Err(MemoryError::Config("Unix endpoint path is empty".into()));
            }
            return Ok(Self::Unix(PathBuf::from(path)));
        }
        if let Some(address) = value.strip_prefix("tcp://") {
            return address
                .parse()
                .map(Self::Tcp)
                .map_err(|error| MemoryError::Config(format!("invalid TCP endpoint: {error}")));
        }
        Err(MemoryError::Config(format!(
            "endpoint must begin with unix:// or tcp://: {value}"
        )))
    }

    pub fn display(&self) -> String {
        match self {
            Self::Unix(path) => format!("unix://{}", path.display()),
            Self::Tcp(address) => format!("tcp://{address}"),
        }
    }
}

pub async fn request(endpoint: &Endpoint, request: &Request) -> Result<Response> {
    match endpoint {
        Endpoint::Unix(path) => {
            let mut stream = timeout(CONNECT_TIMEOUT, UnixStream::connect(path))
                .await
                .map_err(|_| timeout_error("connect", CONNECT_TIMEOUT))?
                .map_err(|error| MemoryError::Transport(error.to_string()))?;
            exchange(&mut stream, request).await
        }
        Endpoint::Tcp(address) => {
            let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(address))
                .await
                .map_err(|_| timeout_error("connect", CONNECT_TIMEOUT))?
                .map_err(|error| MemoryError::Transport(error.to_string()))?;
            exchange(&mut stream, request).await
        }
    }
}

async fn exchange<S>(stream: &mut S, request: &Request) -> Result<Response>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = serde_json::to_vec(request)?;
    let bytes = timeout(REQUEST_IO_TIMEOUT, async {
        write_frame_with_timeout(stream, &request, FRAME_WRITE_TIMEOUT).await?;
        read_frame_with_timeout(stream, FRAME_READ_TIMEOUT).await
    })
    .await
    .map_err(|_| timeout_error("request I/O", REQUEST_IO_TIMEOUT))??;
    serde_json::from_slice(&bytes).map_err(Into::into)
}

pub async fn serve(
    endpoint: Endpoint,
    service: Arc<MemoryService>,
    policy: ServePolicy,
) -> Result<()> {
    validate_serve_endpoint(&endpoint, policy)?;
    let connection_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    let shutdown = wait_for_shutdown_signal();
    tokio::pin!(shutdown);
    match endpoint {
        Endpoint::Unix(path) => {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let endpoint_lock_path = unix_endpoint_lock_path(&path);
            let endpoint_lock = open_private_file(&endpoint_lock_path)?;
            endpoint_lock.try_lock_exclusive().map_err(|error| {
                MemoryError::Transport(format!(
                    "another daemon owns Unix endpoint {}: {error}",
                    path.display()
                ))
            })?;
            if path.exists() {
                let metadata = fs::symlink_metadata(&path)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileTypeExt as _;
                    if !metadata.file_type().is_socket() {
                        return Err(MemoryError::Transport(format!(
                            "refusing to replace non-socket Unix endpoint {}",
                            path.display()
                        )));
                    }
                }
                match timeout(CONNECT_TIMEOUT, UnixStream::connect(&path)).await {
                    Ok(Ok(_)) => {
                        return Err(MemoryError::Transport(format!(
                            "Unix endpoint {} is already accepting connections",
                            path.display()
                        )));
                    }
                    Ok(Err(_)) => {}
                    Err(_) => {
                        return Err(MemoryError::Transport(format!(
                            "probing Unix endpoint {} timed out; refusing to remove it",
                            path.display()
                        )));
                    }
                }
                // The endpoint lock serializes this stale-socket cleanup with
                // all cooperating daemon starts, so a live listener can never
                // be unlinked by a second data directory.
                tokio::fs::remove_file(&path).await?;
            }
            let listener = UnixListener::bind(&path)?;
            set_private_permissions(&path)?;
            tracing::info!(endpoint = %path.display(), "Memoree daemon listening");
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        if let Some(permit) = try_reserve_connection(&connection_permits) {
                            spawn_connection(stream, service.clone(), permit);
                        } else {
                            tracing::warn!(
                                limit = MAX_CONCURRENT_CONNECTIONS,
                                "rejecting connection: concurrent connection limit reached"
                            );
                        }
                    }
                    shutdown_result = &mut shutdown => {
                        tracing::info!("Memoree daemon shutting down");
                        let _ = tokio::fs::remove_file(&path).await;
                        drop(endpoint_lock);
                        shutdown_result?;
                        return Ok(());
                    }
                }
            }
        }
        Endpoint::Tcp(address) => {
            let listener = TcpListener::bind(address).await?;
            tracing::info!(endpoint = %address, "Memoree daemon listening");
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        if let Some(permit) = try_reserve_connection(&connection_permits) {
                            spawn_connection(stream, service.clone(), permit);
                        } else {
                            tracing::warn!(
                                limit = MAX_CONCURRENT_CONNECTIONS,
                                "rejecting connection: concurrent connection limit reached"
                            );
                        }
                    }
                    shutdown_result = &mut shutdown => {
                        shutdown_result?;
                        tracing::info!("Memoree daemon shutting down");
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn validate_serve_endpoint(endpoint: &Endpoint, policy: ServePolicy) -> Result<()> {
    if let Endpoint::Tcp(address) = endpoint
        && !address.ip().is_loopback()
        && !policy.dangerously_allow_non_loopback_tcp
    {
        return Err(MemoryError::Config(format!(
            "refusing unauthenticated non-loopback TCP listener {address}; bind to 127.0.0.1/[::1] or explicitly pass --dangerously-allow-non-loopback-tcp behind a trusted external network boundary"
        )));
    }
    Ok(())
}

async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            received = terminate.recv() => {
                if received.is_none() {
                    return Err(MemoryError::Transport(
                        "SIGTERM signal listener closed unexpectedly".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok(())
    }
}

fn unix_endpoint_lock_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".lock");
    PathBuf::from(value)
}

fn open_private_file(path: &Path) -> Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    set_private_permissions(path)?;
    Ok(file)
}

fn set_private_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn try_reserve_connection(semaphore: &Arc<Semaphore>) -> Option<OwnedSemaphorePermit> {
    semaphore.clone().try_acquire_owned().ok()
}

fn spawn_connection<S>(mut stream: S, service: Arc<MemoryService>, permit: OwnedSemaphorePermit)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        // Keep the permit for the complete request/response lifetime.
        let _permit = permit;
        let result = async {
            let request = match read_frame_with_timeout(&mut stream, FRAME_READ_TIMEOUT).await {
                Ok(bytes) => match serde_json::from_slice::<Request>(&bytes) {
                    Ok(request) => request,
                    Err(error) => {
                        let response = Response::failure(
                            "req_transport",
                            &MemoryError::InvalidRequest(format!("invalid request JSON: {error}")),
                        );
                        return write_frame_with_timeout(
                            &mut stream,
                            &serde_json::to_vec(&response)?,
                            FRAME_WRITE_TIMEOUT,
                        )
                        .await;
                    }
                },
                // Oversized frames are rejected before their body is read.
                // Closing the connection lets a still-writing peer observe
                // the transport failure without a bidirectional deadlock.
                Err(error) => return Err(error),
            };
            let request_id = request.request_id.clone();
            let response = service.handle(request).await;
            let bytes = serde_json::to_vec(&response)?;
            if bytes.len() > MAX_FRAME_BYTES {
                let response = Response::failure(&request_id, &MemoryError::ContentTooLarge);
                return write_frame_with_timeout(
                    &mut stream,
                    &serde_json::to_vec(&response)?,
                    FRAME_WRITE_TIMEOUT,
                )
                .await;
            }
            write_frame_with_timeout(&mut stream, &bytes, FRAME_WRITE_TIMEOUT).await
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "request connection failed");
        }
    });
}

fn timeout_error(operation: &str, duration: Duration) -> MemoryError {
    MemoryError::Transport(format!(
        "{operation} timed out after {} seconds",
        duration.as_secs_f64()
    ))
}

async fn write_frame_with_timeout<S>(stream: &mut S, bytes: &[u8], duration: Duration) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    timeout(duration, write_frame(stream, bytes))
        .await
        .map_err(|_| timeout_error("frame write", duration))?
}

async fn read_frame_with_timeout<S>(stream: &mut S, duration: Duration) -> Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    timeout(duration, read_frame(stream))
        .await
        .map_err(|_| timeout_error("frame read", duration))?
}

async fn write_frame<S>(stream: &mut S, bytes: &[u8]) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(MemoryError::ContentTooLarge);
    }
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(bytes).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_frame<S>(stream: &mut S) -> Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let len = stream.read_u32().await? as usize;
    if len > MAX_FRAME_BYTES {
        return Err(MemoryError::ContentTooLarge);
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{protocol::ErrorCode, store::Store};
    use tokio::io::duplex;

    #[test]
    fn parses_endpoints() {
        assert!(matches!(
            Endpoint::parse("unix:///tmp/memoree.sock").unwrap(),
            Endpoint::Unix(_)
        ));
        assert!(matches!(
            Endpoint::parse("tcp://127.0.0.1:17878").unwrap(),
            Endpoint::Tcp(_)
        ));
        assert!(Endpoint::parse("http://localhost").is_err());
    }

    #[test]
    fn non_loopback_tcp_requires_dangerous_server_opt_in() {
        let loopback = Endpoint::parse("tcp://127.0.0.1:17878").unwrap();
        let wildcard_v4 = Endpoint::parse("tcp://0.0.0.0:17878").unwrap();
        let wildcard_v6 = Endpoint::parse("tcp://[::]:17878").unwrap();

        assert!(validate_serve_endpoint(&loopback, ServePolicy::default()).is_ok());
        for endpoint in [&wildcard_v4, &wildcard_v6] {
            let error = validate_serve_endpoint(endpoint, ServePolicy::default()).unwrap_err();
            assert!(matches!(error, MemoryError::Config(_)));
            assert!(
                error
                    .to_string()
                    .contains("--dangerously-allow-non-loopback-tcp")
            );
            assert!(
                validate_serve_endpoint(
                    endpoint,
                    ServePolicy {
                        dangerously_allow_non_loopback_tcp: true,
                    },
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn unix_endpoint_lock_prevents_split_brain() {
        let temporary = tempfile::tempdir().unwrap();
        let lock_path = unix_endpoint_lock_path(&temporary.path().join("memoree.sock"));
        let first = open_private_file(&lock_path).unwrap();
        first.try_lock_exclusive().unwrap();
        let second = open_private_file(&lock_path).unwrap();
        assert!(second.try_lock_exclusive().is_err());
    }

    #[test]
    fn concurrent_connection_permits_are_bounded() {
        assert_eq!(MAX_CONCURRENT_CONNECTIONS, 4);
        let semaphore = Arc::new(Semaphore::new(2));
        let first = try_reserve_connection(&semaphore).unwrap();
        let second = try_reserve_connection(&semaphore).unwrap();
        assert!(try_reserve_connection(&semaphore).is_none());

        drop(first);
        assert!(try_reserve_connection(&semaphore).is_some());
        drop(second);
    }

    #[tokio::test]
    async fn incomplete_frame_read_times_out() {
        let (mut stream, _peer) = duplex(64);
        let error = read_frame_with_timeout(&mut stream, Duration::from_millis(10))
            .await
            .unwrap_err();
        assert!(matches!(error, MemoryError::Transport(_)));
        assert!(error.to_string().contains("frame read timed out"));
    }

    #[tokio::test]
    async fn blocked_frame_write_times_out() {
        let (mut stream, _peer) = duplex(1);
        let error = write_frame_with_timeout(&mut stream, &[0; 64], Duration::from_millis(10))
            .await
            .unwrap_err();
        assert!(matches!(error, MemoryError::Transport(_)));
        assert!(error.to_string().contains("frame write timed out"));
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected_before_reading_its_body() {
        let (mut stream, mut peer) = duplex(16);
        peer.write_u32((MAX_FRAME_BYTES + 1) as u32).await.unwrap();
        let error = read_frame_with_timeout(&mut stream, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(error, MemoryError::ContentTooLarge));
    }

    #[tokio::test]
    async fn malformed_json_receives_a_structured_error() {
        let temporary = tempfile::tempdir().unwrap();
        let service = Arc::new(MemoryService::new(Store::open(temporary.path()).unwrap()));
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = try_reserve_connection(&semaphore).unwrap();
        let (server, mut client) = duplex(4096);
        spawn_connection(server, service, permit);

        write_frame_with_timeout(&mut client, b"{", Duration::from_secs(1))
            .await
            .unwrap();
        let bytes = read_frame_with_timeout(&mut client, Duration::from_secs(1))
            .await
            .unwrap();
        let response: Response = serde_json::from_slice(&bytes).unwrap();

        assert!(!response.ok);
        assert_eq!(response.request_id, "req_transport");
        assert!(matches!(
            response.error.unwrap().code,
            ErrorCode::InvalidRequest
        ));
    }
}
