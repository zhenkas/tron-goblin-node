//! Durable, failure-isolated critical notifications. No business or signing APIs.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::{self, Display, Write as _},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        net::UnixStream,
    },
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, OnceLock},
    thread,
    time::{Duration, Instant},
};
const MAX_PENDING: usize = 256;
const MAX_BODY: usize = 16 * 1024;
const IO_TIMEOUT: Duration = Duration::from_millis(750);

/// Only static summaries/codes, a local error digest, and numeric context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CriticalEvent {
    code: &'static str,
    summary: &'static str,
    fingerprint: Option<String>,
    context: BTreeMap<&'static str, i64>,
}
impl CriticalEvent {
    pub fn new(code: &'static str, summary: &'static str) -> Self {
        Self {
            code,
            summary,
            fingerprint: None,
            context: BTreeMap::new(),
        }
    }
    /// Stream through SHA-256; never retain or send the raw error.
    pub fn fingerprint(mut self, error: impl Display) -> Self {
        struct HashWriter(Sha256);
        impl fmt::Write for HashWriter {
            fn write_str(&mut self, value: &str) -> fmt::Result {
                self.0.update(value.as_bytes());
                Ok(())
            }
        }
        let mut writer = HashWriter(Sha256::new());
        let _ = write!(&mut writer, "{error}");
        self.fingerprint = Some(hex(&writer.0.finalize()));
        self
    }
    pub fn context(mut self, key: &'static str, value: i64) -> Self {
        self.context.insert(key, value);
        self
    }
    pub fn code(&self) -> &'static str {
        self.code
    }
    pub fn summary(&self) -> &'static str {
        self.summary
    }
    pub fn numeric_context(&self) -> &BTreeMap<&'static str, i64> {
        &self.context
    }
    pub fn error_fingerprint(&self) -> Option<&str> {
        self.fingerprint.as_deref()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportResult {
    Disabled,
    Queued { request_id: String },
    Duplicate { request_id: String },
}
/// Domain-test seam; business components never depend on HTTP details.
pub trait EventSink {
    fn critical(&self, event: CriticalEvent) -> io::Result<ReportResult>;
    fn flush(&self, timeout: Duration) -> bool;
}
#[derive(Clone, Default)]
pub struct Reporter {
    inner: Option<Arc<Inner>>,
}
static ENV_REPORTER: OnceLock<Reporter> = OnceLock::new();
#[derive(Clone)]
enum Endpoint {
    Http { address: SocketAddr, host: String },
    Unix(PathBuf),
}
struct Settings {
    endpoint: Endpoint,
    token: String, // Never Debug/Serialize/log this value.
    component: String,
    directory: PathBuf,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    schema: u8,
    request_id: String,
    text: String,
    severity: String,
}
struct Queue {
    pending: VecDeque<Pending>,
    seen: HashMap<String, String>,
    stopped: bool,
}
struct Inner {
    settings: Settings,
    queue: Mutex<Queue>,
    wake: Condvar,
}
impl Reporter {
    /// One reporter per process. Missing bridge variables disables it. An invalid
    /// enabled installation emits one fixed warning, without configuration values.
    pub fn from_env() -> Self {
        ENV_REPORTER
            .get_or_init(|| {
                let requested = [
                    "CRITICAL_NOTIFY_URL",
                    "CRITICAL_NOTIFY_SOCKET",
                    "CRITICAL_NOTIFY_TOKEN",
                ]
                .iter()
                .any(|key| std::env::var_os(key).is_some());
                if !requested {
                    return Self::default();
                }
                match settings_from_env().and_then(Self::start) {
                    Ok(reporter) => reporter,
                    Err(_) => {
                        eprintln!(
                            "critical_notify disabled: invalid bridge or notification state"
                        );
                        Self::default()
                    }
                }
            })
            .clone()
    }
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }
    pub fn critical(&self, event: CriticalEvent) -> io::Result<ReportResult> {
        self.enqueue(event, "critical")
    }
    /// Only for explicit completion of a requested long-running script.
    pub fn completion(
        &self,
        code: &'static str,
        summary: &'static str,
    ) -> io::Result<ReportResult> {
        self.enqueue(CriticalEvent::new(code, summary), "info")
    }
    pub fn flush(&self, timeout: Duration) -> bool {
        let Some(inner) = &self.inner else {
            return true;
        };
        let deadline = Instant::now() + timeout;
        let Ok(mut queue) = inner.queue.lock() else {
            return false;
        };
        inner.wake.notify_all();
        while !queue.pending.is_empty() {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            let Ok((next, status)) = inner.wake.wait_timeout(queue, remaining) else {
                return false;
            };
            queue = next;
            if status.timed_out() && !queue.pending.is_empty() {
                return false;
            }
        }
        true
    }
    fn enqueue(&self, event: CriticalEvent, severity: &str) -> io::Result<ReportResult> {
        self.enqueue_with_sync(event, severity, sync_directory)
    }
    fn enqueue_with_sync(
        &self,
        event: CriticalEvent,
        severity: &str,
        sync: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<ReportResult> {
        let Some(inner) = &self.inner else {
            return Ok(ReportResult::Disabled);
        };
        if !valid_name(event.code, 128)
            || event.summary.is_empty()
            || event.summary.len() > 1024
            || event.context.len() > 16
            || event.context.keys().any(|key| !valid_name(key, 64))
        {
            return Err(invalid());
        }
        let text = serde_json::to_string(&serde_json::json!({
            "component": inner.settings.component, "code": event.code, "summary": event.summary,
            "fingerprint": event.fingerprint, "context": event.context,
        }))
        .map_err(|_| invalid())?;
        if text.contains(&inner.settings.token) {
            return Err(invalid());
        }
        let signature = digest(&format!("{severity}\n{text}"));
        let mut queue = inner.queue.lock().map_err(|_| invalid())?;
        if let Some(request_id) = queue.seen.get(&signature) {
            // A prior rename may have succeeded while directory sync failed.
            // Retain and confirm its exact ID rather than creating a second intent.
            sync(&inner.settings.directory)?;
            return Ok(ReportResult::Duplicate {
                request_id: request_id.clone(),
            });
        }
        if queue.pending.len() >= MAX_PENDING {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "notification outbox is full",
            ));
        }
        let pending = Pending {
            schema: 1,
            request_id: new_id()?,
            text,
            severity: severity.into(),
        };
        persist(&inner.settings.directory, &pending)?;
        queue.seen.insert(signature, pending.request_id.clone());
        queue.pending.push_back(pending.clone());
        inner.wake.notify_all();
        // Remember identity before the final durability barrier: its failure is
        // an unknown persistence outcome, not authority to allocate another ID.
        sync(&inner.settings.directory)?;
        Ok(ReportResult::Queued {
            request_id: pending.request_id,
        })
    }
    fn start(settings: Settings) -> io::Result<Self> {
        prepare_directory(&settings.directory)?;
        let mut pending = VecDeque::new();
        let mut entries = fs::read_dir(&settings.directory)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            if !entry.file_type()?.is_file() || fs::metadata(&path)?.len() > MAX_BODY as u64 {
                return Err(invalid());
            }
            let item: Pending = serde_json::from_slice(&fs::read(&path)?).map_err(|_| invalid())?;
            let expected = format!("{}.json", item.request_id);
            if item.schema != 1
                || !valid_id(&item.request_id)
                || path.file_name().and_then(|s| s.to_str()) != Some(expected.as_str())
                || !["critical", "info", "error"].contains(&item.severity.as_str())
                || item.text.len() > MAX_BODY
                || item.text.contains(&settings.token)
                || pending.len() >= MAX_PENDING
            {
                return Err(invalid());
            }
            pending.push_back(item);
        }
        let seen = pending
            .iter()
            .map(|item| {
                (
                    digest(&format!("{}\n{}", item.severity, item.text)),
                    item.request_id.clone(),
                )
            })
            .collect();
        let inner = Arc::new(Inner {
            settings,
            queue: Mutex::new(Queue {
                pending,
                seen,
                stopped: false,
            }),
            wake: Condvar::new(),
        });
        let worker = inner.clone();
        thread::Builder::new()
            .name("critical-notify".into())
            .spawn(move || delivery_loop(worker))?;
        Ok(Self { inner: Some(inner) })
    }
    #[cfg(test)]
    fn stop(&self) {
        if let Some(inner) = &self.inner {
            inner.queue.lock().unwrap().stopped = true;
            inner.wake.notify_all();
        }
    }
}
impl EventSink for Reporter {
    fn critical(&self, event: CriticalEvent) -> io::Result<ReportResult> {
        Reporter::critical(self, event)
    }
    fn flush(&self, timeout: Duration) -> bool {
        Reporter::flush(self, timeout)
    }
}
fn settings_from_env() -> io::Result<Settings> {
    let token = std::env::var("CRITICAL_NOTIFY_TOKEN").map_err(|_| invalid())?;
    if token.is_empty() || token.len() > 4096 || token.contains(['\r', '\n']) {
        return Err(invalid());
    }
    let endpoint = if let Ok(url) = std::env::var("CRITICAL_NOTIFY_URL") {
        parse_http(&url)?
    } else {
        let path = std::env::var("CRITICAL_NOTIFY_SOCKET").map_err(|_| invalid())?;
        let path = PathBuf::from(path.strip_prefix("unix://").unwrap_or(&path));
        if !path.is_absolute() {
            return Err(invalid());
        }
        Endpoint::Unix(path)
    };
    let component = std::env::var("CRITICAL_NOTIFY_COMPONENT").map_err(|_| invalid())?;
    if !valid_name(&component, 80) {
        return Err(invalid());
    }
    let directory =
        PathBuf::from(std::env::var("CRITICAL_NOTIFY_STATE_DIR").map_err(|_| invalid())?);
    if !directory.is_absolute() {
        return Err(invalid());
    }
    Ok(Settings {
        endpoint,
        token,
        component,
        directory,
    })
}
fn parse_http(url: &str) -> io::Result<Endpoint> {
    let authority = url
        .strip_prefix("http://")
        .ok_or_else(invalid)?
        .trim_end_matches('/');
    if authority.contains(['/', '@', '?', '#', '\r', '\n']) {
        return Err(invalid());
    }
    let normalized = if let Some(port) = authority.strip_prefix("localhost:") {
        format!("127.0.0.1:{port}")
    } else {
        authority.to_owned()
    };
    let address: SocketAddr = normalized.parse().map_err(|_| invalid())?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err(invalid());
    }
    Ok(Endpoint::Http {
        address,
        host: authority.to_owned(),
    })
}
fn valid_name(s: &str, max: usize) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn valid_id(s: &str) -> bool {
    s.strip_prefix("notify-")
        .is_some_and(|id| id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn digest(value: &str) -> String {
    hex(&Sha256::digest(value.as_bytes()))
}
fn new_id() -> io::Result<String> {
    let mut bytes = [0; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(format!("notify-{}", hex(&bytes)))
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid notification state or configuration",
    )
}
fn prepare_directory(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(invalid());
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}
fn persist(directory: &Path, pending: &Pending) -> io::Result<()> {
    let data = serde_json::to_vec(pending).map_err(|_| invalid())?;
    if data.len() > MAX_BODY {
        return Err(invalid());
    }
    let temporary = directory.join(format!(".{}.tmp", pending.request_id));
    let destination = directory.join(format!("{}.json", pending.request_id));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&data)?;
    file.sync_all()?;
    fs::rename(temporary, destination)
}
fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}
fn retire_delivered_with_sync(
    directory: &Path,
    item: &Pending,
    sync: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    match fs::remove_file(directory.join(format!("{}.json", item.request_id))) {
        Ok(()) => {}
        // A previous acknowledged delivery may have removed the file before its
        // directory sync failed. Removal is idempotent; confirm the barrier again.
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    sync(directory)
}
fn delivery_loop(inner: Arc<Inner>) {
    let mut backoff = Duration::from_millis(100);
    loop {
        let item = {
            let Ok(mut queue) = inner.queue.lock() else {
                return;
            };
            while queue.pending.is_empty() && !queue.stopped {
                let Ok(next) = inner.wake.wait(queue) else {
                    return;
                };
                queue = next;
            }
            if queue.stopped {
                return;
            }
            match queue.pending.front().cloned() {
                Some(item) => item,
                None => continue,
            }
        };
        // No network side effect before an accepted spool rename is durable,
        // including the unknown-fsync branch of enqueue.
        if sync_directory(&inner.settings.directory).is_ok() && send(&inner.settings, &item).is_ok()
        {
            let result =
                retire_delivered_with_sync(&inner.settings.directory, &item, sync_directory);
            if result.is_ok() {
                let Ok(mut queue) = inner.queue.lock() else {
                    return;
                };
                queue.pending.pop_front();
                inner.wake.notify_all();
                backoff = Duration::from_millis(100);
                continue;
            }
        }
        let Ok(queue) = inner.queue.lock() else {
            return;
        };
        if queue.stopped {
            return;
        }
        let _ = inner.wake.wait_timeout(queue, backoff);
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}
trait Transport: Read + Write {
    fn timeout(&self, remaining: Duration) -> io::Result<()>;
}
impl Transport for TcpStream {
    fn timeout(&self, d: Duration) -> io::Result<()> {
        self.set_read_timeout(Some(d))?;
        self.set_write_timeout(Some(d))
    }
}
impl Transport for UnixStream {
    fn timeout(&self, d: Duration) -> io::Result<()> {
        self.set_read_timeout(Some(d))?;
        self.set_write_timeout(Some(d))
    }
}
fn connect_unix(path: &Path) -> io::Result<UnixStream> {
    let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
    socket.connect_timeout(&socket2::SockAddr::unix(path)?, IO_TIMEOUT)?;
    let descriptor: std::os::fd::OwnedFd = socket.into();
    Ok(UnixStream::from(descriptor))
}
fn send(settings: &Settings, item: &Pending) -> io::Result<()> {
    let (mut stream, host): (Box<dyn Transport>, &str) = match &settings.endpoint {
        Endpoint::Http { address, host } => (
            Box::new(TcpStream::connect_timeout(address, IO_TIMEOUT)?),
            host,
        ),
        Endpoint::Unix(path) => (Box::new(connect_unix(path)?), "localhost"),
    };
    let body = serde_json::to_vec(&serde_json::json!({
        "text":item.text,"request_id":item.request_id,"severity":item.severity,
    }))
    .map_err(|_| invalid())?;
    let headers=format!("POST /v1/notify HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",settings.token,body.len());
    stream.timeout(IO_TIMEOUT)?;
    stream.write_all(headers.as_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    let deadline = Instant::now() + IO_TIMEOUT;
    let mut response = Vec::new();
    let mut chunk = [0; 2048];
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| io::Error::from(io::ErrorKind::TimedOut))?;
        stream.timeout(remaining)?;
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        response.extend_from_slice(&chunk[..n]);
        if response.len() > MAX_BODY {
            return Err(invalid());
        }
        if let Some(index) = response.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = std::str::from_utf8(&response[..index]).map_err(|_| invalid())?;
            if let Some(length) = header.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            }) {
                if length > MAX_BODY {
                    return Err(invalid());
                }
                if response.len() >= index + 4 + length {
                    break;
                }
            }
        }
    }
    let index = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(invalid)?;
    let header = std::str::from_utf8(&response[..index]).map_err(|_| invalid())?;
    let status = header
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(invalid)?;
    if !(200..300).contains(&status) {
        return Err(io::Error::other("notification bridge did not acknowledge"));
    }
    let result: serde_json::Value =
        serde_json::from_slice(&response[index + 4..]).map_err(|_| invalid())?;
    if result
        .get("event_id")
        .and_then(|value| value.as_str())
        .is_none_or(|id| id.is_empty())
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::TcpListener, sync::mpsc};
    use tempfile::TempDir;
    const TOKEN: &str = "private-bridge-test-token";
    fn settings(directory: &Path, address: SocketAddr) -> Settings {
        Settings {
            endpoint: Endpoint::Http {
                address,
                host: address.to_string(),
            },
            token: TOKEN.into(),
            component: "test-component".into(),
            directory: directory.to_owned(),
        }
    }
    fn request(stream: &mut impl Read) -> (String, Vec<u8>) {
        let mut bytes = Vec::new();
        loop {
            let mut one = [0; 1];
            stream.read_exact(&mut one).unwrap();
            bytes.push(one[0]);
            if bytes.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let header = String::from_utf8(bytes).unwrap();
        let length = header
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        let mut body = vec![0; length];
        stream.read_exact(&mut body).unwrap();
        (header, body)
    }
    fn acknowledge(stream: &mut impl Write) {
        let body = br#"{"event_id":"evt_test"}"#;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(body).unwrap();
    }
    fn event() -> CriticalEvent {
        CriticalEvent::new("storage_failed", "Persistent storage failed")
            .fingerprint(format!("raw private database error {TOKEN}"))
            .context("block", 42)
    }
    #[test]
    fn durable_deduplicated_redacted_http_delivery() {
        let directory = TempDir::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (captured_tx, captured_rx) = mpsc::channel();
        let (allow_tx, allow_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (header, body) = request(&mut stream);
            captured_tx.send((header, body)).unwrap();
            allow_rx.recv().unwrap();
            acknowledge(&mut stream);
        });
        let reporter = Reporter::start(settings(directory.path(), address)).unwrap();
        let first = reporter.critical(event()).unwrap();
        let ReportResult::Queued { request_id } = first else {
            panic!("not queued")
        };
        let (header, body) = captured_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(header.contains(&format!("Authorization: Bearer {TOKEN}")));
        assert!(!String::from_utf8_lossy(&body).contains(TOKEN));
        let persisted = fs::read(directory.path().join(format!("{request_id}.json"))).unwrap();
        assert!(!String::from_utf8_lossy(&persisted).contains(TOKEN));
        let wire: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(wire["request_id"], request_id);
        let text: serde_json::Value = serde_json::from_str(wire["text"].as_str().unwrap()).unwrap();
        assert_eq!(text["context"]["block"], 42);
        assert_eq!(text["fingerprint"].as_str().unwrap().len(), 64);
        assert_eq!(
            reporter.critical(event()).unwrap(),
            ReportResult::Duplicate {
                request_id: request_id.clone()
            }
        );
        assert!(!reporter.flush(Duration::from_millis(10)));
        allow_tx.send(()).unwrap();
        assert!(reporter.flush(Duration::from_secs(2)));
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        assert_eq!(
            reporter.critical(event()).unwrap(),
            ReportResult::Duplicate { request_id }
        );
        reporter.stop();
        server.join().unwrap();
    }
    #[test]
    fn unknown_response_retries_identical_id_and_content() {
        let directory = TempDir::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let (_, original) = request(&mut first);
            thread::sleep(IO_TIMEOUT + Duration::from_millis(100));
            drop(first); // admitted outcome may be unknown after the client times out
            let (mut second, _) = listener.accept().unwrap();
            let (_, retry) = request(&mut second);
            assert_eq!(original, retry);
            acknowledge(&mut second);
        });
        let reporter = Reporter::start(settings(directory.path(), address)).unwrap();
        reporter.critical(event()).unwrap();
        assert!(reporter.flush(Duration::from_secs(3)));
        reporter.stop();
        server.join().unwrap();
    }
    #[test]
    fn restart_recovers_exact_pending_request() {
        let directory = TempDir::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let item = Pending {
            schema: 1,
            request_id: "notify-00000000000000000000000000000001".into(),
            text: r#"{"component":"test-component","code":"stopped"}"#.into(),
            severity: "critical".into(),
        };
        persist(directory.path(), &item).unwrap();
        let expected = serde_json::json!({"text":item.text,"request_id":item.request_id,"severity":item.severity});
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (_, body) = request(&mut stream);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                expected
            );
            acknowledge(&mut stream);
        });
        let reporter = Reporter::start(settings(directory.path(), address)).unwrap();
        assert!(reporter.flush(Duration::from_secs(2)));
        reporter.stop();
        server.join().unwrap();
    }
    #[test]
    fn rejected_auth_keeps_outbox_and_flush_is_bounded() {
        let directory = TempDir::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"
            )
            .unwrap();
        });
        let reporter = Reporter::start(settings(directory.path(), address)).unwrap();
        let queued = reporter.critical(event()).unwrap();
        let started = Instant::now();
        assert!(!reporter.flush(Duration::from_millis(75)));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
        assert!(matches!(queued, ReportResult::Queued { .. }));
        reporter.stop();
        server.join().unwrap();
    }
    #[test]
    fn socket_delivery_uses_same_authenticated_protocol() {
        use std::os::unix::net::UnixListener;
        let directory = TempDir::new().unwrap();
        let socket = directory.path().join("bridge.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (header, body) = request(&mut stream);
            assert!(header.contains(&format!("Authorization: Bearer {TOKEN}")));
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["severity"], "info");
            acknowledge(&mut stream);
        });
        let reporter = Reporter::start(Settings {
            endpoint: Endpoint::Unix(socket),
            token: TOKEN.into(),
            component: "script".into(),
            directory: directory.path().join("outbox"),
        })
        .unwrap();
        reporter
            .completion("script_complete", "Requested script completed")
            .unwrap();
        assert!(reporter.flush(Duration::from_secs(2)));
        reporter.stop();
        server.join().unwrap();
    }
    #[test]
    fn failed_directory_sync_keeps_the_exact_admitted_pending_identity() {
        let directory = TempDir::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let reporter =
            Reporter::start(settings(directory.path(), listener.local_addr().unwrap())).unwrap();
        let error = reporter
            .enqueue_with_sync(event(), "critical", |_| {
                Err(io::Error::other("simulated directory sync failure"))
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        let queue = reporter.inner.as_ref().unwrap().queue.lock().unwrap();
        assert_eq!(queue.pending.len(), 1);
        let admitted = queue.pending[0].request_id.clone();
        drop(queue);
        assert_eq!(
            reporter.critical(event()).unwrap(),
            ReportResult::Duplicate {
                request_id: admitted.clone()
            }
        );
        let files = fs::read_dir(directory.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
            .count();
        assert_eq!(files, 1);
        assert!(directory.path().join(format!("{admitted}.json")).exists());
        reporter.stop();
    }
    #[test]
    fn delivered_unlink_is_idempotent_after_directory_sync_failure() {
        let directory = TempDir::new().unwrap();
        let item = Pending {
            schema: 1,
            request_id: "notify-00000000000000000000000000000002".into(),
            text: "{}".into(),
            severity: "critical".into(),
        };
        persist(directory.path(), &item).unwrap();
        sync_directory(directory.path()).unwrap();
        assert!(retire_delivered_with_sync(directory.path(), &item, |_| {
            Err(io::Error::other("simulated directory sync failure"))
        })
        .is_err());
        assert!(!directory
            .path()
            .join(format!("{}.json", item.request_id))
            .exists());
        retire_delivered_with_sync(directory.path(), &item, sync_directory).unwrap();
    }
    #[test]
    fn saturated_unix_listener_cannot_block_notification_worker_connect() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("backlog.sock");
        let address = socket2::SockAddr::unix(&path).unwrap();
        let listener =
            socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None).unwrap();
        listener.bind(&address).unwrap();
        listener.listen(1).unwrap();
        let mut fillers = Vec::new();
        let mut full = false;
        for _ in 0..8 {
            let socket =
                socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None).unwrap();
            socket.set_nonblocking(true).unwrap();
            match socket.connect(&address) {
                Ok(()) => fillers.push(socket),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    full = true;
                    break;
                }
                Err(error) => panic!("unexpected local fixture connect: {error}"),
            }
        }
        assert!(full, "test must actually saturate the listener");
        let started = Instant::now();
        assert!(connect_unix(&path).is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
    }
    #[test]
    fn rejects_remote_endpoints_and_unsafe_fields() {
        for url in [
            "https://127.0.0.1:123",
            "http://192.0.2.1:123",
            "http://example.com:123",
            "http://user:pass@127.0.0.1:123",
            "http://127.0.0.1:123/extra",
            "http://127.0.0.1:0",
        ] {
            assert!(parse_http(url).is_err());
        }
        assert!(parse_http("http://[::1]:123/").is_ok());
        assert!(parse_http("http://localhost:123").is_ok());
        assert_eq!(
            Reporter::default().critical(event()).unwrap(),
            ReportResult::Disabled
        );
        assert!(Reporter::default().flush(Duration::ZERO));
    }
}
