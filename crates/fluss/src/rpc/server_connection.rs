// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::cluster::{ServerNode, ServerType};
use crate::error::Error;
use crate::metrics::{
    CLIENT_BYTES_RECEIVED_TOTAL, CLIENT_BYTES_SENT_TOTAL, CLIENT_REQUEST_LATENCY_MS,
    CLIENT_REQUESTS_IN_FLIGHT, CLIENT_REQUESTS_TOTAL, CLIENT_RESPONSES_TOTAL, LABEL_API_KEY,
    api_key_label,
};
use crate::proto::PbApiVersion;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::error::RpcError;
use crate::rpc::error::RpcError::ConnectionError;
use crate::rpc::frame::{AsyncMessageRead, AsyncMessageWrite};
use crate::rpc::message::{
    ApiVersionsRequest, REQUEST_HEADER_LENGTH, ReadType, RequestBody, RequestHeader,
    ResponseHeader, WriteType,
};
use crate::rpc::transport::Transport;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufStream};
use tokio::sync::mpsc;
use tokio::sync::oneshot::{self, Sender};

pub type MessengerTransport = ServerConnectionInner<BufStream<Transport>>;

pub type ServerConnection = Arc<MessengerTransport>;

// Matches Java's ExponentialBackoff(100ms initial, 2x multiplier, 5000ms max, 0.2 jitter).
const AUTH_INITIAL_BACKOFF_MS: f64 = 100.0;
const AUTH_MAX_BACKOFF_MS: f64 = 5000.0;
const AUTH_BACKOFF_MULTIPLIER: f64 = 2.0;
const AUTH_JITTER: f64 = 0.2;

#[derive(Clone)]
pub struct SaslConfig {
    pub username: String,
    pub password: String,
}

impl fmt::Debug for SaslConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslConfig")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// Represents the negotiated API versions between the client and a server node.
/// Built from the server's `ApiVersionsResponse` by intersecting each API's
/// client-supported range with the server-supported range, keeping the highest
/// usable version.
#[derive(Clone, Debug)]
pub struct ServerApiVersions {
    versions: HashMap<ApiKey, Result<ApiVersion, String>>,
}

impl ServerApiVersions {
    /// Build from the server's advertised API version list.
    pub fn new(server_versions: &[PbApiVersion]) -> Self {
        let mut versions = HashMap::new();
        for sv in server_versions {
            let api_key = ApiKey::from(i16::try_from(sv.api_key).unwrap());
            // Skip unknown API keys — the client does not support them.
            let client_range = match api_key.supported_versions() {
                Some(range) => range,
                None => continue,
            };
            let server_min = i16::try_from(sv.min_version).unwrap();
            let server_max = i16::try_from(sv.max_version).unwrap();
            let min_version = client_range.min().0.max(server_min);
            let max_version = client_range.max().0.min(server_max);
            if min_version > max_version {
                versions.insert(
                    api_key,
                    Err(format!(
                        "The server does not support {:?} with version in range [{},{}]. \
                         The supported range is [{},{}].",
                        api_key,
                        client_range.min(),
                        client_range.max(),
                        server_min,
                        server_max,
                    )),
                );
            } else {
                versions.insert(api_key, Ok(ApiVersion(max_version)));
            }
        }
        Self { versions }
    }

    /// Get the negotiated (highest usable) version for a given API key.
    pub fn highest_available_version(&self, api_key: ApiKey) -> Result<ApiVersion, Error> {
        match self.versions.get(&api_key) {
            Some(Ok(version)) => Ok(*version),
            Some(Err(msg)) => Err(Error::UnsupportedVersion {
                message: msg.clone(),
            }),
            None => Err(Error::UnsupportedVersion {
                message: format!("The server does not support {:?}", api_key),
            }),
        }
    }
}

/// Resolve the API version to use for a given API key.
fn resolve_api_version_for(
    api_versions: Option<&ServerApiVersions>,
    api_key: ApiKey,
) -> Result<ApiVersion, Error> {
    // version equals highestSupportedVersion might happen when requesting api version check
    // before serverApiVersions is initialized. We always use the highest version for api
    // version checking.
    let default_version = api_key
        .supported_versions()
        .map(|range| range.max())
        .unwrap();
    match api_versions {
        Some(versions) => versions.highest_available_version(api_key),
        None => Ok(default_version),
    }
}

/// Validate that the server's advertised `server_type` matches the type we expect
/// for the target `ServerNode`.
fn validate_server_type(
    expected: &ServerType,
    response_server_type: Option<i32>,
) -> Result<(), Error> {
    // For forward-compat with servers that do not populate `server_type`, validation is skipped.
    let Some(type_id) = response_server_type else {
        return Ok(());
    };
    let actual = ServerType::from_type_id(type_id);
    if actual.as_ref() == Some(expected) {
        return Ok(());
    }
    let actual_desc = actual
        .map(|t| t.to_string())
        .unwrap_or_else(|| format!("Unknown(type_id={type_id})"));
    Err(Error::InvalidServerType {
        message: format!(
            "Expected server type {expected} but the server advertised {actual_desc}. \
             The client may be talking to the wrong endpoint \
             (e.g. coordinator vs tablet server)."
        ),
    })
}

#[derive(Debug, Default)]
pub struct RpcClient {
    connections: RwLock<HashMap<String, ServerConnection>>,
    client_id: Arc<str>,
    timeout: Option<Duration>,
    max_message_size: usize,
    sasl_config: Option<SaslConfig>,
    /// Optional injected I/O runtime handle. `None` falls back to the
    /// process-global `fluss-io` runtime via [`io_runtime_handle`].
    io_handle: Option<tokio::runtime::Handle>,
}

impl RpcClient {
    pub fn new() -> Self {
        RpcClient {
            connections: Default::default(),
            client_id: Arc::from(""),
            timeout: None,
            max_message_size: usize::MAX,
            sasl_config: None,
            io_handle: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_sasl(mut self, username: String, password: String) -> Self {
        self.sasl_config = Some(SaslConfig { username, password });
        self
    }

    /// Inject the runtime that drives every connection's socket I/O.
    ///
    /// The handle MUST point to a single, long-lived runtime — never a
    /// per-request or otherwise transient runtime — because the socket's
    /// reactor (where its fd is registered) lives on that runtime. If that
    /// runtime is dropped, the connection's I/O stops and any in-flight or
    /// subsequent request fails. When not set, connections fall back to the
    /// process-global `fluss-io` runtime.
    pub fn with_io_handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.io_handle = Some(handle);
        self
    }

    /// Resolve the I/O runtime handle used for both establishing the socket and
    /// running its I/O task: the injected handle if present, otherwise the
    /// process-global `fluss-io` runtime.
    ///
    /// The resolved handle must reference a single, long-lived runtime — the
    /// socket's reactor lives there for the connection's whole lifetime.
    fn io_handle(&self) -> tokio::runtime::Handle {
        self.io_handle.clone().unwrap_or_else(io_runtime_handle)
    }

    pub async fn get_connection(
        &self,
        server_node: &ServerNode,
    ) -> Result<ServerConnection, Error> {
        let server_id = server_node.uid();
        {
            let connections = self.connections.read();
            if let Some(conn) = connections.get(server_id).cloned() {
                // Reuse only a healthy connection. A poisoned OR stopped-I/O-task
                // connection is rebuilt below; the latter (stopped without poison)
                // is what `is_poisoned()` alone missed, leaving a dead connection
                // cached forever after a scan stopped its I/O task.
                if conn.is_healthy() {
                    return Ok(conn);
                }
            }
        }
        let new_server = self.connect(server_node).await?;
        {
            let mut connections = self.connections.write();
            if let Some(race_conn) = connections.get(server_id) {
                if race_conn.is_healthy() {
                    return Ok(race_conn.clone());
                }
            }

            // Replaces a dead/poisoned entry (if any) with the fresh connection.
            connections.insert(server_id.to_owned(), new_server.clone());
        }
        Ok(new_server)
    }

    async fn connect(&self, server_node: &ServerNode) -> Result<ServerConnection, Error> {
        // Resolve a single I/O runtime handle and use it for BOTH establishing
        // the socket and running the connection's I/O task. Creating the
        // `TcpStream` on this handle registers the socket's fd with that
        // runtime's reactor (instead of the caller's), so the connection's
        // liveness no longer depends on whichever runtime first called
        // `connect`.
        let io_handle = self.io_handle();

        let url = server_node.url();
        let timeout = self.timeout;
        let transport = io_handle
            .spawn(async move { Transport::connect(&url, timeout).await })
            .await
            .map_err(|e| ConnectionError(format!("I/O runtime join error: {e}")))?
            .map_err(|error| ConnectionError(error.to_string()))?;

        let messenger = ServerConnectionInner::new_on(
            io_handle,
            BufStream::new(transport),
            self.max_message_size,
            self.client_id.clone(),
        );
        let connection = ServerConnection::new(messenger);

        // Negotiate API versions (must happen before authentication).
        Self::check_api_versions(&connection, server_node.server_type()).await?;

        if let Some(ref sasl) = self.sasl_config {
            Self::authenticate(&connection, &sasl.username, &sasl.password).await?;
        }

        Ok(connection)
    }

    /// Send an `ApiVersionsRequest`, validate the advertised `server_type`, and
    /// store the negotiated versions on the connection.
    async fn check_api_versions(
        connection: &ServerConnection,
        expected_server_type: &ServerType,
    ) -> Result<(), Error> {
        let request = ApiVersionsRequest::new("fluss-rust", env!("CARGO_PKG_VERSION"));
        let response = connection.request(request).await?;
        validate_server_type(expected_server_type, response.server_type)?;
        let api_versions = ServerApiVersions::new(&response.api_versions);
        *connection.api_versions.lock() = Some(api_versions);
        Ok(())
    }

    /// Perform SASL/PLAIN authentication handshake.
    ///
    /// Retries on `RetriableAuthenticateException` with exponential backoff
    /// (matching Java's unbounded retry behaviour). Non-retriable errors
    /// (wrong password, unknown user) propagate immediately as
    /// `Error::FlussAPIError` with the original error code.
    async fn authenticate(
        connection: &ServerConnection,
        username: &str,
        password: &str,
    ) -> Result<(), Error> {
        use crate::rpc::fluss_api_error::FlussError;
        use crate::rpc::message::AuthenticateRequest;
        use rand::Rng;

        let initial_request = AuthenticateRequest::new_plain(username, password);
        let mut retry_count: u32 = 0;

        loop {
            let request = initial_request.clone();
            let result = connection.request(request).await;

            match result {
                Ok(response) => {
                    // Check for server challenge (multi-round auth).
                    // PLAIN mechanism never sends a challenge, but we handle it
                    // for protocol correctness matching Java's handleAuthenticateResponse.
                    if let Some(challenge) = response.challenge {
                        let challenge_req = AuthenticateRequest::from_challenge("PLAIN", challenge);
                        connection.request(challenge_req).await?;
                    }
                    return Ok(());
                }
                Err(Error::FlussAPIError { ref api_error })
                    if FlussError::for_code(api_error.code)
                        == FlussError::RetriableAuthenticateException =>
                {
                    retry_count += 1;
                    // Cap the exponent like Java's ExponentialBackoff.expMax so that
                    // jitter still produces a range at steady state instead of being
                    // clamped to AUTH_MAX_BACKOFF_MS.
                    let exp_max = (AUTH_MAX_BACKOFF_MS / AUTH_INITIAL_BACKOFF_MS).log2();
                    let exp = ((retry_count as f64) - 1.0).min(exp_max);
                    let term = AUTH_INITIAL_BACKOFF_MS * AUTH_BACKOFF_MULTIPLIER.powf(exp);
                    let jitter_factor =
                        1.0 - AUTH_JITTER + rand::rng().random::<f64>() * (2.0 * AUTH_JITTER);
                    let backoff_ms = (term * jitter_factor) as u64;
                    log::warn!(
                        "SASL authentication retriable failure (attempt {retry_count}), \
                         retrying in {backoff_ms}ms: {}",
                        api_error.message
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                // Server-side auth errors (wrong password, unknown user, etc.)
                // propagate with their original error code preserved.
                Err(e) => return Err(e),
            }
        }
    }
}

#[derive(Debug)]
struct Response {
    #[allow(dead_code)]
    header: ResponseHeader,
    data: Cursor<Vec<u8>>,
}

/// An outbound request handed to the dedicated I/O task. The task writes
/// `bytes` to the socket and, on success, parks `resp_tx` keyed by `request_id`
/// until the matching response is read back.
struct Outgoing {
    request_id: i32,
    bytes: Vec<u8>,
    resp_tx: Sender<Result<Response, RpcError>>,
}

/// Process-global dedicated I/O runtime that drives every connection's I/O task.
///
/// The connection actor must run on a stable, long-lived runtime that is
/// independent of whichever runtime constructed the connection or which runtime
/// later calls [`request`](ServerConnectionInner::request). Spawning the I/O
/// task on the caller's runtime (e.g. via bare `tokio::spawn`) splits the
/// socket's read/write halves across runtimes and deadlocks when a connection
/// is reused across runtimes.
fn io_runtime_handle() -> tokio::runtime::Handle {
    static IO_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    IO_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("fluss-io")
                .build()
                .expect("failed to build fluss dedicated I/O runtime")
        })
        .handle()
        .clone()
}

/// Tracks per-request connection metrics and ensures in-flight gauge cleanup on drop.
struct RequestMetricsLifecycle {
    label: Option<&'static str>,
    start: Instant,
    completed: bool,
}

impl RequestMetricsLifecycle {
    fn begin(api_key: crate::rpc::ApiKey, request_bytes: u64) -> Self {
        let label = api_key_label(api_key);
        if let Some(label) = label {
            metrics::counter!(CLIENT_REQUESTS_TOTAL, LABEL_API_KEY => label).increment(1);
            metrics::counter!(CLIENT_BYTES_SENT_TOTAL, LABEL_API_KEY => label)
                .increment(request_bytes);
            metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).increment(1.0);
        }
        Self {
            label,
            start: Instant::now(),
            completed: false,
        }
    }

    fn complete(&mut self, response_bytes: u64) {
        let Some(label) = self.label else {
            return;
        };
        if self.completed {
            return;
        }

        metrics::counter!(CLIENT_RESPONSES_TOTAL, LABEL_API_KEY => label).increment(1);
        metrics::counter!(CLIENT_BYTES_RECEIVED_TOTAL, LABEL_API_KEY => label)
            .increment(response_bytes);
        metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).decrement(1.0);
        metrics::histogram!(CLIENT_REQUEST_LATENCY_MS, LABEL_API_KEY => label)
            .record(self.start.elapsed().as_secs_f64() * 1000.0);
        self.completed = true;
    }
}

impl Drop for RequestMetricsLifecycle {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Some(label) = self.label {
            metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).decrement(1.0);
            self.completed = true;
        }
    }
}

/// Shared poison cell. `None` while the connection is healthy; set to the
/// terminal error once the I/O task observes an unrecoverable read/write error.
///
/// The request map is now owned task-locally by the I/O task, so the only shared
/// state between callers and the I/O task is this poison error.
type PoisonCell = Arc<Mutex<Option<Arc<RpcError>>>>;

/// Record the terminal error into the shared cell (first writer wins) and return
/// the effective poison error.
fn set_poison(cell: &PoisonCell, err: RpcError) -> Arc<RpcError> {
    let mut guard = cell.lock();
    if let Some(existing) = guard.as_ref() {
        return Arc::clone(existing);
    }
    let err = Arc::new(err);
    *guard = Some(Arc::clone(&err));
    err
}

fn read_poison(cell: &PoisonCell) -> Option<Arc<RpcError>> {
    cell.lock().as_ref().map(Arc::clone)
}

#[derive(Debug)]
pub struct ServerConnectionInner<RW> {
    /// Outbound queue feeding the dedicated I/O task. Sending an [`Outgoing`]
    /// here hands the framed request to the task, which owns both halves of the
    /// socket and routes the response back via the embedded oneshot.
    outgoing_tx: mpsc::UnboundedSender<Outgoing>,

    client_id: Arc<str>,

    request_id: AtomicI32,

    /// Shared poison error, set by the I/O task on socket failure.
    poison: PoisonCell,

    /// Negotiated API versions for this connection.
    /// `None` until the ApiVersions handshake completes.
    api_versions: Mutex<Option<ServerApiVersions>>,

    /// Phantom so the type parameter remains meaningful after the socket halves
    /// moved into the I/O task.
    _marker: std::marker::PhantomData<RW>,
}

impl<RW> ServerConnectionInner<RW>
where
    RW: AsyncRead + AsyncWrite + Send + 'static,
{
    /// Construct a connection whose I/O task runs on the process-global
    /// `fluss-io` runtime. Equivalent to [`new_on`](Self::new_on) with
    /// [`io_runtime_handle`].
    pub fn new(stream: RW, max_message_size: usize, client_id: Arc<str>) -> Self {
        Self::new_on(
            io_runtime_handle(),
            stream,
            max_message_size,
            client_id,
        )
    }

    /// Construct a connection whose I/O task is spawned on the given
    /// `io_handle`.
    ///
    /// The handle MUST reference a single, long-lived runtime — never a
    /// per-request or transient runtime — because the socket's reactor lives on
    /// that runtime for the connection's entire lifetime. For the seam to be
    /// fully closed, the socket should also have been *created* on this same
    /// handle (see `RpcClient::connect`), so that both halves of the socket and
    /// its I/O task share one reactor.
    pub fn new_on(
        io_handle: tokio::runtime::Handle,
        stream: RW,
        max_message_size: usize,
        client_id: Arc<str>,
    ) -> Self {
        let (stream_read, stream_write) = tokio::io::split(stream);
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel::<Outgoing>();
        let poison: PoisonCell = Arc::new(Mutex::new(None));
        let poison_captured = Arc::clone(&poison);

        let io_task = async move {
            let mut stream_read = stream_read;
            let mut stream_write = stream_write;
            let mut outgoing_rx = outgoing_rx;
            // In-flight requests awaiting a response, keyed by request id.
            // Task-local: only this task touches it, so no lock is needed.
            let mut in_flight: HashMap<i32, Sender<Result<Response, RpcError>>> = HashMap::new();

            loop {
                tokio::select! {
                    outgoing = outgoing_rx.recv() => {
                        match outgoing {
                            Some(Outgoing { request_id, bytes, resp_tx }) => {
                                let write_result: Result<(), RpcError> = async {
                                    stream_write.write_message(&bytes).await?;
                                    stream_write.flush().await?;
                                    Ok(())
                                }
                                .await;
                                match write_result {
                                    Ok(()) => {
                                        in_flight.insert(request_id, resp_tx);
                                    }
                                    Err(e) => {
                                        // Framing may be out-of-sync; poison and fail everyone.
                                        let err = set_poison(&poison_captured, e);
                                        resp_tx
                                            .send(Err(RpcError::Poisoned(Arc::clone(&err))))
                                            .ok();
                                        for (_id, tx) in in_flight.drain() {
                                            tx.send(Err(RpcError::Poisoned(Arc::clone(&err)))).ok();
                                        }
                                        return;
                                    }
                                }
                            }
                            None => {
                                // All senders dropped: connection is being torn down.
                                return;
                            }
                        }
                    }
                    read = stream_read.read_message(max_message_size) => {
                        match read {
                            Ok(msg) => {
                                // Message was read, so a malformed header should not
                                // poison the whole stream (matches prior behaviour).
                                let mut cursor = Cursor::new(msg);
                                let header = match ResponseHeader::read(&mut cursor) {
                                    Ok(header) => header,
                                    Err(err) => {
                                        log::warn!(
                                            "Cannot read message header, ignoring message: {err:?}"
                                        );
                                        continue;
                                    }
                                };

                                match in_flight.remove(&header.request_id) {
                                    Some(resp_tx) => {
                                        // We don't care if the caller is gone (cancelled).
                                        resp_tx
                                            .send(Ok(Response { header, data: cursor }))
                                            .ok();
                                    }
                                    None => {
                                        log::warn!(
                                            request_id:% = header.request_id;
                                            "Got response for unknown request",
                                        );
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                let err =
                                    set_poison(&poison_captured, RpcError::ReadMessageError(e));
                                for (_id, tx) in in_flight.drain() {
                                    tx.send(Err(RpcError::Poisoned(Arc::clone(&err)))).ok();
                                }
                                return;
                            }
                        }
                    }
                }
            }
        };

        // Spawn on the provided long-lived I/O handle — NOT the caller's
        // runtime. This is the same handle that created the socket, so both
        // socket halves and their I/O task share one reactor. It decouples the
        // connection's socket I/O from whichever runtime constructed it or
        // later calls `request`, and means `new` no longer requires an ambient
        // tokio runtime.
        io_handle.spawn(io_task);

        Self {
            outgoing_tx,
            client_id,
            request_id: AtomicI32::new(0),
            poison,
            api_versions: Mutex::new(None),
            _marker: std::marker::PhantomData,
        }
    }

    fn resolve_api_version(&self, api_key: ApiKey) -> Result<ApiVersion, Error> {
        let guard = self.api_versions.lock();
        resolve_api_version_for(guard.as_ref(), api_key)
    }

    fn is_poisoned(&self) -> bool {
        self.poison.lock().is_some()
    }

    /// Whether this connection is still usable. A connection is healthy only if
    /// it is not poisoned AND its I/O task is still running.
    ///
    /// The I/O task owns the receiving half of `outgoing_tx`; if the task has
    /// stopped for ANY reason — a socket error (which also poisons), a clean
    /// teardown, or its runtime being dropped/cancelled (which cannot run the
    /// poison path) — the channel is closed. Checking `outgoing_tx.is_closed()`
    /// therefore detects a dead I/O task even when poison was never set, which is
    /// the case `is_poisoned()` alone misses and the reason a scan that stops the
    /// task could leave a permanently-unusable cached connection.
    fn is_healthy(&self) -> bool {
        !self.is_poisoned() && !self.outgoing_tx.is_closed()
    }

    pub async fn request<R>(&self, msg: R) -> Result<R::ResponseBody, Error>
    where
        R: RequestBody + Send + WriteType<Vec<u8>>,
        R::ResponseBody: ReadType<Cursor<Vec<u8>>>,
    {
        let api_version = self.resolve_api_version(R::API_KEY)?;
        let request_id = self.request_id.fetch_add(1, Ordering::SeqCst) & 0x7FFFFFFF;
        let header = RequestHeader {
            request_api_key: R::API_KEY,
            request_api_version: api_version,
            request_id,
            client_id: Some(String::from(self.client_id.as_ref())),
        };

        let mut buf = Vec::new();
        // write header
        header
            .write(&mut buf)
            .map_err(RpcError::WriteMessageError)?;
        // write message body
        msg.write(&mut buf).map_err(RpcError::WriteMessageError)?;

        // count only the API message body, excluding the protocol header.
        let request_body_bytes = buf.len().saturating_sub(REQUEST_HEADER_LENGTH) as u64;
        let mut request_metrics = RequestMetricsLifecycle::begin(R::API_KEY, request_body_bytes);

        // Fail fast if the connection is already poisoned. A failed request
        // still counts as a completion with zero received bytes.
        if let Some(err) = read_poison(&self.poison) {
            request_metrics.complete(0);
            return Err(RpcError::Poisoned(err).into());
        }

        let (resp_tx, resp_rx) = oneshot::channel();

        // Hand the framed request to the I/O task. A send error means the task
        // is gone (poisoned or shut down).
        if self
            .outgoing_tx
            .send(Outgoing {
                request_id,
                bytes: buf,
                resp_tx,
            })
            .is_err()
        {
            request_metrics.complete(0);
            let err = read_poison(&self.poison)
                .map(RpcError::Poisoned)
                .unwrap_or_else(|| ConnectionError("connection I/O task has stopped".to_string()));
            return Err(err.into());
        }

        // Await the response. A recv error means the I/O task dropped the
        // sender (poisoned or closed) without replying.
        let mut response = match resp_rx.await {
            Ok(inner) => inner.map_err(Error::from),
            Err(_recv_err) => {
                let err = read_poison(&self.poison)
                    .map(RpcError::Poisoned)
                    .unwrap_or_else(|| {
                        ConnectionError("connection closed before response".to_string())
                    });
                Err(err.into())
            }
        }
        .inspect_err(|_| request_metrics.complete(0))?;

        // count only the API message body, excluding the response header.
        let response_bytes =
            (response.data.get_ref().len() as u64).saturating_sub(response.data.position());
        request_metrics.complete(response_bytes);

        if let Some(error_response) = response.header.error_response {
            return Err(Error::FlussAPIError {
                api_error: crate::rpc::ApiError::from(error_response),
            });
        }

        let body = R::ResponseBody::read(&mut response.data).map_err(RpcError::ReadMessageError)?;

        let read_bytes = response.data.position();
        let message_bytes = response.data.into_inner().len() as u64;
        if read_bytes != message_bytes {
            return Err(RpcError::TooMuchData {
                message_size: message_bytes,
                read: read_bytes,
                api_key: R::API_KEY,
                api_version,
            }
            .into());
        }
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::rpc::ApiKey;
    use crate::rpc::api_version::ApiVersion;
    use crate::rpc::frame::{ReadError, WriteError};
    use crate::rpc::message::{ReadType, RequestBody, WriteType};
    use metrics::{SharedString, Unit};
    use metrics_util::CompositeKey;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::sync::OnceLock;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
    use tokio::sync::Mutex as AsyncMutex;

    // -- Test-only request/response types --------------------------------

    struct TestProduceRequest;
    struct TestProduceResponse;

    impl RequestBody for TestProduceRequest {
        type ResponseBody = TestProduceResponse;
        const API_KEY: ApiKey = ApiKey::ProduceLog;
    }

    impl WriteType<Vec<u8>> for TestProduceRequest {
        fn write(&self, _w: &mut Vec<u8>) -> Result<(), WriteError> {
            Ok(())
        }
    }

    impl ReadType<Cursor<Vec<u8>>> for TestProduceResponse {
        fn read(_r: &mut Cursor<Vec<u8>>) -> Result<Self, ReadError> {
            Ok(TestProduceResponse)
        }
    }

    struct TestMetadataRequest;
    struct TestMetadataResponse;

    impl RequestBody for TestMetadataRequest {
        type ResponseBody = TestMetadataResponse;
        const API_KEY: ApiKey = ApiKey::MetaData;
    }

    impl WriteType<Vec<u8>> for TestMetadataRequest {
        fn write(&self, _w: &mut Vec<u8>) -> Result<(), WriteError> {
            Ok(())
        }
    }

    impl ReadType<Cursor<Vec<u8>>> for TestMetadataResponse {
        fn read(_r: &mut Cursor<Vec<u8>>) -> Result<Self, ReadError> {
            Ok(TestMetadataResponse)
        }
    }

    // -- Mock server -----------------------------------------------------

    /// Reads framed requests and echoes back minimal success responses.
    async fn mock_echo_server(mut stream: tokio::io::DuplexStream) {
        loop {
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).await.is_err() {
                return;
            }
            let len = i32::from_be_bytes(len_buf) as usize;

            let mut payload = vec![0u8; len];
            if stream.read_exact(&mut payload).await.is_err() {
                return;
            }

            // Header layout: api_key(2) + api_version(2) + request_id(4)
            let request_id = i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);

            // Response: resp_type(1, 0=success) + request_id(4)
            let mut resp = Vec::with_capacity(5);
            resp.push(0u8);
            resp.extend_from_slice(&request_id.to_be_bytes());

            let resp_len = (resp.len() as i32).to_be_bytes();
            if stream.write_all(&resp_len).await.is_err()
                || stream.write_all(&resp).await.is_err()
                || stream.flush().await.is_err()
            {
                return;
            }
        }
    }

    /// Reads framed requests and echoes back error responses (resp_type=1).
    async fn mock_error_server(mut stream: tokio::io::DuplexStream) {
        use prost::Message;

        loop {
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).await.is_err() {
                return;
            }
            let len = i32::from_be_bytes(len_buf) as usize;

            let mut payload = vec![0u8; len];
            if stream.read_exact(&mut payload).await.is_err() {
                return;
            }

            let request_id = i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);

            let err = crate::proto::ErrorResponse {
                error_code: 1,
                error_message: Some("test error".to_string()),
            };
            let mut err_buf = Vec::new();
            err.encode(&mut err_buf).expect("ErrorResponse encode");

            let mut resp = Vec::with_capacity(5 + err_buf.len());
            resp.push(1u8); // ERROR_RESPONSE
            resp.extend_from_slice(&request_id.to_be_bytes());
            resp.extend(err_buf);

            let resp_len = (resp.len() as i32).to_be_bytes();
            if stream.write_all(&resp_len).await.is_err()
                || stream.write_all(&resp).await.is_err()
                || stream.flush().await.is_err()
            {
                return;
            }
        }
    }

    // -- Recorder setup --------------------------------------------------

    /// Shared test recorder (installed once per test binary).
    static TEST_SNAPSHOTTER: OnceLock<metrics_util::debugging::Snapshotter> = OnceLock::new();
    static TEST_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

    fn test_snapshotter() -> &'static metrics_util::debugging::Snapshotter {
        TEST_SNAPSHOTTER.get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            recorder
                .install()
                .expect("debugging recorder install should succeed in this test binary");
            snapshotter
        })
    }

    fn test_lock() -> &'static AsyncMutex<()> {
        TEST_LOCK.get_or_init(|| AsyncMutex::new(()))
    }

    type SnapshotEntry = (CompositeKey, Option<Unit>, Option<SharedString>, DebugValue);

    fn has_api_label(key: &CompositeKey, label: &str) -> bool {
        key.key()
            .labels()
            .any(|l| l.key() == LABEL_API_KEY && l.value() == label)
    }

    fn counter_for_label(entries: &[SnapshotEntry], metric_name: &str, label: &str) -> u64 {
        entries
            .iter()
            .find_map(|(key, _, _, value)| {
                if key.key().name() != metric_name || !has_api_label(key, label) {
                    return None;
                }
                match value {
                    DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0)
    }

    fn gauge_for_label(entries: &[SnapshotEntry], metric_name: &str, label: &str) -> f64 {
        entries
            .iter()
            .find_map(|(key, _, _, value)| {
                if key.key().name() != metric_name || !has_api_label(key, label) {
                    return None;
                }
                match value {
                    DebugValue::Gauge(v) => Some(v.into_inner()),
                    _ => None,
                }
            })
            .unwrap_or(0.0)
    }

    fn counter_sum(entries: &[SnapshotEntry], metric_name: &str) -> u64 {
        entries
            .iter()
            .filter_map(|(key, _, _, value)| {
                if key.key().name() != metric_name {
                    return None;
                }
                match value {
                    DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .sum()
    }

    fn histogram_sample_count_for_label(
        entries: &[SnapshotEntry],
        metric_name: &str,
        label: &str,
    ) -> usize {
        entries
            .iter()
            .find_map(|(key, _, _, value)| {
                if key.key().name() != metric_name || !has_api_label(key, label) {
                    return None;
                }
                match value {
                    DebugValue::Histogram(v) => Some(v.len()),
                    _ => None,
                }
            })
            .unwrap_or(0)
    }

    // -- Tests -----------------------------------------------------------

    #[tokio::test]
    async fn request_records_metrics_for_reportable_api_key() {
        let _test_guard = test_lock().lock().await;
        let snapshotter = test_snapshotter();

        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(mock_echo_server(server));

        let conn = ServerConnectionInner::new(BufStream::new(client), usize::MAX, Arc::from("t"));
        *conn.api_versions.lock() = Some(ServerApiVersions::new(&[PbApiVersion {
            api_key: 1014,
            min_version: 0,
            max_version: 0,
        }]));

        let before: Vec<_> = snapshotter.snapshot().into_vec();
        let request_before = counter_for_label(&before, CLIENT_REQUESTS_TOTAL, "produce_log");
        let response_before = counter_for_label(&before, CLIENT_RESPONSES_TOTAL, "produce_log");
        let latency_samples_before =
            histogram_sample_count_for_label(&before, CLIENT_REQUEST_LATENCY_MS, "produce_log");

        conn.request(TestProduceRequest).await.unwrap();

        let after: Vec<_> = snapshotter.snapshot().into_vec();
        let request_after = counter_for_label(&after, CLIENT_REQUESTS_TOTAL, "produce_log");
        let response_after = counter_for_label(&after, CLIENT_RESPONSES_TOTAL, "produce_log");
        let latency_samples_after =
            histogram_sample_count_for_label(&after, CLIENT_REQUEST_LATENCY_MS, "produce_log");
        assert_eq!(
            request_after - request_before,
            1,
            "produce_log request counter should increment by 1"
        );
        assert_eq!(
            response_after - response_before,
            1,
            "produce_log completion counter should increment by 1"
        );
        assert_eq!(
            latency_samples_after - latency_samples_before,
            1,
            "request latency histogram sample count should increment by 1 for produce_log"
        );
    }

    #[tokio::test]
    async fn request_skips_metrics_for_non_reportable_api_key() {
        let _test_guard = test_lock().lock().await;
        let snapshotter = test_snapshotter();

        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(mock_echo_server(server));

        let conn = ServerConnectionInner::new(BufStream::new(client), usize::MAX, Arc::from("t"));
        *conn.api_versions.lock() = Some(ServerApiVersions::new(&[PbApiVersion {
            api_key: 1012,
            min_version: 0,
            max_version: 0,
        }]));
        let before: Vec<_> = snapshotter.snapshot().into_vec();
        let request_sum_before = counter_sum(&before, CLIENT_REQUESTS_TOTAL);
        let response_sum_before = counter_sum(&before, CLIENT_RESPONSES_TOTAL);

        conn.request(TestMetadataRequest).await.unwrap();

        let snapshot: Vec<_> = snapshotter.snapshot().into_vec();
        let request_sum_after = counter_sum(&snapshot, CLIENT_REQUESTS_TOTAL);
        let response_sum_after = counter_sum(&snapshot, CLIENT_RESPONSES_TOTAL);
        assert_eq!(
            request_sum_after, request_sum_before,
            "non-reportable API keys must not change request counters"
        );
        assert_eq!(
            response_sum_after, response_sum_before,
            "non-reportable API keys must not change response counters"
        );

        // No metric entry should carry a non-reportable API key label.
        let non_reportable = snapshot
            .iter()
            .any(|(key, _, _, _)| has_api_label(key, "metadata"));
        assert!(
            !non_reportable,
            "non-reportable API keys must not appear in metrics"
        );
    }

    #[tokio::test]
    async fn request_records_completion_metrics_when_send_fails() {
        let _test_guard = test_lock().lock().await;
        let snapshotter = test_snapshotter();

        let (client, server) = tokio::io::duplex(64);
        drop(server); // force write failure on request path
        let conn = ServerConnectionInner::new(BufStream::new(client), usize::MAX, Arc::from("t"));
        *conn.api_versions.lock() = Some(ServerApiVersions::new(&[PbApiVersion {
            api_key: 1014,
            min_version: 0,
            max_version: 0,
        }]));

        let before: Vec<_> = snapshotter.snapshot().into_vec();
        let request_before = counter_for_label(&before, CLIENT_REQUESTS_TOTAL, "produce_log");
        let response_before = counter_for_label(&before, CLIENT_RESPONSES_TOTAL, "produce_log");
        let bytes_received_before =
            counter_for_label(&before, CLIENT_BYTES_RECEIVED_TOTAL, "produce_log");
        let result = conn.request(TestProduceRequest).await;
        assert!(
            result.is_err(),
            "request should fail when transport is closed"
        );
        let after: Vec<_> = snapshotter.snapshot().into_vec();
        let request_after = counter_for_label(&after, CLIENT_REQUESTS_TOTAL, "produce_log");
        let response_after = counter_for_label(&after, CLIENT_RESPONSES_TOTAL, "produce_log");
        let bytes_received_after =
            counter_for_label(&after, CLIENT_BYTES_RECEIVED_TOTAL, "produce_log");
        let inflight_after = gauge_for_label(&after, CLIENT_REQUESTS_IN_FLIGHT, "produce_log");

        assert_eq!(
            request_after - request_before,
            1,
            "failed request should still count as request"
        );
        assert_eq!(
            response_after - response_before,
            1,
            "failed request should still count as a completion like Java ConnectionMetrics"
        );
        assert_eq!(
            bytes_received_after - bytes_received_before,
            0,
            "failed send should record zero received bytes"
        );
        assert_eq!(
            inflight_after, 0.0,
            "in-flight gauge must return to zero after failure"
        );
    }

    #[tokio::test]
    async fn request_records_completion_metrics_when_server_returns_api_error() {
        let _test_guard = test_lock().lock().await;
        let snapshotter = test_snapshotter();

        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(mock_error_server(server));

        let conn = ServerConnectionInner::new(BufStream::new(client), usize::MAX, Arc::from("t"));
        *conn.api_versions.lock() = Some(ServerApiVersions::new(&[PbApiVersion {
            api_key: 1014,
            min_version: 0,
            max_version: 0,
        }]));

        let before: Vec<_> = snapshotter.snapshot().into_vec();
        let response_before = counter_for_label(&before, CLIENT_RESPONSES_TOTAL, "produce_log");
        let bytes_received_before =
            counter_for_label(&before, CLIENT_BYTES_RECEIVED_TOTAL, "produce_log");

        let result = conn.request(TestProduceRequest).await;
        assert!(
            matches!(result, Err(Error::FlussAPIError { .. })),
            "request should fail with FlussAPIError when server returns error_response"
        );

        let after: Vec<_> = snapshotter.snapshot().into_vec();
        let response_after = counter_for_label(&after, CLIENT_RESPONSES_TOTAL, "produce_log");
        let bytes_received_after =
            counter_for_label(&after, CLIENT_BYTES_RECEIVED_TOTAL, "produce_log");
        let inflight_after = gauge_for_label(&after, CLIENT_REQUESTS_IN_FLIGHT, "produce_log");

        assert_eq!(
            response_after - response_before,
            1,
            "API error response should count as completion like Java"
        );
        assert_eq!(
            bytes_received_after - bytes_received_before,
            0,
            "API error response should record zero body bytes like Java onRequestFailure"
        );
        assert_eq!(
            inflight_after, 0.0,
            "in-flight gauge must return to zero after API error"
        );
    }

    /// Proves the connection's socket I/O is fully decoupled from the caller's
    /// runtime: the connection is constructed with NO ambient runtime (plain
    /// `#[test]`), and `request` is later driven from a freshly-created runtime.
    /// The I/O task itself runs on the dedicated `fluss-io` runtime, so the read
    /// and write halves are never split across the caller's runtime. Under the
    /// old bare-`tokio::spawn` design this would have panicked (no runtime in
    /// `new`) or deadlocked (I/O bound to a different runtime than the caller).
    #[test]
    fn connection_io_runs_on_dedicated_runtime_independent_of_caller() {
        // Serialize against the other metrics tests, which share a global
        // recorder: this test also drives a `produce_log` request.
        let _test_guard = test_lock().blocking_lock();

        // Run the echo server on the dedicated I/O runtime handle so the mock
        // peer is also independent of the caller's runtime.
        let (client, server) = tokio::io::duplex(4096);
        io_runtime_handle().spawn(mock_echo_server(server));

        // Construct the connection with no ambient tokio runtime present.
        let conn = ServerConnectionInner::new(BufStream::new(client), usize::MAX, Arc::from("t"));
        *conn.api_versions.lock() = Some(ServerApiVersions::new(&[PbApiVersion {
            api_key: 1014,
            min_version: 0,
            max_version: 0,
        }]));
        let conn = Arc::new(conn);

        // Drive `request` from a brand-new runtime, on a separate thread, with a
        // hard timeout so a regression deadlocks the test instead of hanging it.
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build caller runtime");
            let result = rt.block_on(conn.request(TestProduceRequest));
            done_tx.send(result.is_ok()).ok();
        });

        let ok = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("request must complete without deadlocking across runtimes");
        worker.join().expect("worker thread panicked");
        assert!(ok, "request driven from a foreign runtime should succeed");
    }

    /// Injection works and is caller-runtime-agnostic: a connection built via
    /// `new_on` with an explicit "host" runtime handle is driven successfully
    /// from a completely unrelated `new_current_thread` runtime on another
    /// thread. The socket I/O lives on the injected `host` runtime regardless of
    /// where `request` is called.
    #[test]
    fn injected_io_handle_is_caller_runtime_agnostic() {
        let _test_guard = test_lock().blocking_lock();

        // A long-lived "host" runtime that will own the connection's I/O.
        let host = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("host-injected-io")
            .build()
            .expect("build host runtime");

        let (client, server) = tokio::io::duplex(4096);
        // Run the echo server on the host handle too, so the peer is also
        // independent of the caller's runtime.
        host.handle().spawn(mock_echo_server(server));

        let conn = ServerConnectionInner::new_on(
            host.handle().clone(),
            BufStream::new(client),
            usize::MAX,
            Arc::from("t"),
        );
        *conn.api_versions.lock() = Some(ServerApiVersions::new(&[PbApiVersion {
            api_key: 1014,
            min_version: 0,
            max_version: 0,
        }]));
        let conn = Arc::new(conn);

        // Drive `request` from a brand-new, unrelated runtime on a separate
        // thread, with a hard timeout so a regression fails instead of hanging.
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build caller runtime");
            let result = rt.block_on(conn.request(TestProduceRequest));
            done_tx.send(result.is_ok()).ok();
        });

        let ok = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("request via injected handle must complete without deadlocking");
        worker.join().expect("worker thread panicked");
        assert!(ok, "request driven from a foreign runtime should succeed");
    }

    /// The injected runtime is the one actually doing the I/O (distinct from the
    /// global `fluss-io`): after a first successful request through the injected
    /// `host` runtime, the host runtime is shut down. A subsequent `request`
    /// can then no longer complete — proving the connection's I/O genuinely
    /// lived on the injected runtime, not on the process-global default.
    ///
    /// Distinguishing strategy: deterministic runtime shutdown (rather than a
    /// thread-name hook). If the I/O task had been spawned on the global
    /// `fluss-io` runtime, shutting down `host` would not affect it and the
    /// second request would still succeed; the assertion that it fails/times
    /// out is therefore a precise witness that the injected handle is load-
    /// bearing.
    #[test]
    fn injected_io_handle_is_the_one_doing_io() {
        let _test_guard = test_lock().blocking_lock();

        let host = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("host-injected-io")
            .build()
            .expect("build host runtime");

        let (client, server) = tokio::io::duplex(4096);
        host.handle().spawn(mock_echo_server(server));

        let conn = ServerConnectionInner::new_on(
            host.handle().clone(),
            BufStream::new(client),
            usize::MAX,
            Arc::from("t"),
        );
        *conn.api_versions.lock() = Some(ServerApiVersions::new(&[PbApiVersion {
            api_key: 1014,
            min_version: 0,
            max_version: 0,
        }]));
        let conn = Arc::new(conn);

        // Helper: drive one request from a fresh, unrelated runtime on its own
        // thread with a hard timeout. Returns Some(is_ok) or None on timeout.
        //
        // The worker is joined before returning so that any global-metrics
        // mutation performed by `RequestMetricsLifecycle::complete()` finishes
        // while `test_lock` is still held by the caller. Without the join the
        // worker could outlive `drive_request` and race the other serialized
        // metrics tests on the global `produce_log` counters. After a request
        // resolves (Ok or Err) the worker's `block_on` returns promptly, so the
        // join does not block — including the post-shutdown case, where the
        // request errors out rather than hanging because the injected runtime's
        // I/O task is gone. A bounded join still guards against an unexpected
        // hang turning a flake into a deadlock.
        fn drive_request<RW>(conn: Arc<ServerConnectionInner<RW>>) -> Option<bool>
        where
            RW: AsyncRead + AsyncWrite + Send + Sync + 'static,
        {
            let (tx, rx) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build caller runtime");
                let result = rt.block_on(conn.request(TestProduceRequest));
                // Send the outcome only after `block_on` has fully returned, so
                // the metrics lifecycle for this request has already completed.
                tx.send(result.is_ok()).ok();
            });
            let outcome = rx.recv_timeout(Duration::from_secs(2)).ok();
            // Join the worker so no thread can still be touching the global
            // metrics recorder after this helper returns and `test_lock` is
            // released. The send happens-before the worker's last metric write
            // completes within `block_on`, and the channel send is the worker's
            // final action, so the join returns immediately once `outcome` is
            // observed; on the timeout path the worker has nonetheless finished
            // its request (error, not hang) and is joinable.
            worker.join().expect("drive_request worker thread panicked");
            outcome
        }

        // First request succeeds while the host runtime is alive.
        let first = drive_request(Arc::clone(&conn));
        assert_eq!(
            first,
            Some(true),
            "first request via injected runtime should succeed"
        );

        // Shut the injected runtime down: the I/O task lived there, so it stops.
        host.shutdown_background();

        // A subsequent request can no longer complete: it either errors (the
        // I/O task is gone / channel closed) or times out. Either outcome proves
        // the I/O was bound to the injected runtime, not the global default.
        let second = drive_request(Arc::clone(&conn));
        assert!(
            !matches!(second, Some(true)),
            "after shutting down the injected runtime, the request must not \
             succeed (got {second:?}); this proves I/O lived on the injected \
             runtime, not the global fluss-io"
        );
    }

    /// `is_healthy()` must detect an I/O task that stopped WITHOUT poisoning.
    ///
    /// When the runtime hosting the I/O task is dropped, the task is cancelled and
    /// can never run its poison path, so `is_poisoned()` stays false — exactly the
    /// case `get_connection` used to miss, leaving a dead connection cached
    /// forever. The peer (`_server`) is kept open so the only thing that stops the
    /// task is the runtime teardown, not a socket EOF (which would poison).
    #[test]
    fn is_healthy_detects_io_task_stopped_without_poison() {
        let host = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("host-healthcheck-io")
            .build()
            .expect("build host I/O runtime");

        let (client, _server) = tokio::io::duplex(4096);
        let conn = ServerConnectionInner::new_on(
            host.handle().clone(),
            BufStream::new(client),
            usize::MAX,
            Arc::from("t"),
        );

        assert!(conn.is_healthy(), "a fresh connection should be healthy");
        assert!(!conn.is_poisoned());

        // Drop the runtime: the I/O task is cancelled, dropping the receiving half
        // of the outgoing channel. Cancellation cannot run the poison path.
        drop(host);

        assert!(
            !conn.is_poisoned(),
            "a runtime-cancelled I/O task cannot set poison"
        );
        assert!(
            !conn.is_healthy(),
            "is_healthy must detect the stopped I/O task via the closed outgoing channel"
        );
    }

    #[tokio::test]
    async fn server_api_versions_negotiation() {
        assert_eq!(
            resolve_api_version_for(None, ApiKey::ApiVersion).unwrap(),
            ApiVersion(0)
        );

        assert_eq!(
            resolve_api_version_for(None, ApiKey::PutKv).unwrap(),
            ApiVersion(1)
        );

        let server_versions = vec![
            // PutKv: server v0..v3, client v0 only (v1 key encoding not yet implemented) → negotiated v0
            PbApiVersion {
                api_key: 1016,
                min_version: 0,
                max_version: 3,
            },
            // ProduceLog: server v0..v2, client v0 only → negotiated v0
            PbApiVersion {
                api_key: 1014,
                min_version: 0,
                max_version: 2,
            },
            // Disjoint: server v5..v7, client v0 only → error
            PbApiVersion {
                api_key: 1015,
                min_version: 5,
                max_version: 7,
            },
            // Unknown key (9999) → skipped
            PbApiVersion {
                api_key: 9999,
                min_version: 0,
                max_version: 5,
            },
        ];
        let negotiated = ServerApiVersions::new(&server_versions);

        // Successful negotiation cases
        assert_eq!(
            negotiated.highest_available_version(ApiKey::PutKv).unwrap(),
            ApiVersion(1)
        );
        assert_eq!(
            negotiated
                .highest_available_version(ApiKey::ProduceLog)
                .unwrap(),
            ApiVersion(0)
        );

        // Disjoint range → error
        assert!(
            negotiated
                .highest_available_version(ApiKey::FetchLog)
                .unwrap_err()
                .to_string()
                .contains(&format!(
                    "The server does not support {:?}",
                    ApiKey::FetchLog
                ))
        );

        // Unknown key is skipped → not in map → error
        assert!(
            negotiated
                .highest_available_version(ApiKey::Unknown(9999))
                .is_err()
        );

        // Key not advertised by server → error
        assert!(
            ServerApiVersions::new(&[])
                .highest_available_version(ApiKey::FetchLog)
                .is_err()
        );
    }

    #[test]
    fn server_type_validation() {
        // Happy path: server advertises the expected type.
        assert!(
            validate_server_type(
                &ServerType::CoordinatorServer,
                Some(ServerType::CoordinatorServer.to_type_id()),
            )
            .is_ok()
        );
        assert!(
            validate_server_type(
                &ServerType::TabletServer,
                Some(ServerType::TabletServer.to_type_id()),
            )
            .is_ok()
        );

        // Mismatch: connected to a coordinator while expecting a tablet server
        // (and vice versa).
        let err = validate_server_type(
            &ServerType::TabletServer,
            Some(ServerType::CoordinatorServer.to_type_id()),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::InvalidServerType { .. }),
            "expected InvalidServerType, got: {err:?}"
        );

        assert!(matches!(
            validate_server_type(
                &ServerType::CoordinatorServer,
                Some(ServerType::TabletServer.to_type_id()),
            ),
            Err(Error::InvalidServerType { .. })
        ));

        validate_server_type(&ServerType::TabletServer, None).ok();
        // Unknown / unmapped type id still fails, with the raw id surfaced so
        // operators can diagnose protocol drift.
        assert!(matches!(
            validate_server_type(&ServerType::CoordinatorServer, Some(99),),
            Err(Error::InvalidServerType { .. })
        ));
    }
}
