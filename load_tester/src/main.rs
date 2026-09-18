use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const URL: &str = "ws://127.0.0.1:8080";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const RECV_TIMEOUT: Duration = Duration::from_secs(8);

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type Writer = futures_util::stream::SplitSink<Ws, Message>;
type Reader = futures_util::stream::SplitStream<Ws>;

#[derive(Serialize)]
struct SuiteReport {
    host: String,
    url: String,
    functional: FunctionalReport,
    connections: Vec<ConnectionReport>,
    ping: Vec<PingReport>,
    broadcast: Vec<BroadcastReport>,
    rooms: Vec<RoomReport>,
}

#[derive(Serialize)]
struct FunctionalReport {
    passed: bool,
    checks: Vec<Check>,
}

#[derive(Serialize)]
struct Check {
    name: String,
    ok: bool,
    detail: String,
}

#[derive(Serialize)]
struct ConnectionReport {
    target: usize,
    connected: usize,
    failed: usize,
    connect_secs: f64,
    ping_ok: usize,
    rss_mb: f64,
    cpu_pct: f64,
}

#[derive(Serialize)]
struct PingReport {
    clients: usize,
    duration_secs: f64,
    pongs: u64,
    errors: u64,
    rps: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    rss_mb: f64,
    cpu_pct: f64,
}

#[derive(Serialize)]
struct BroadcastReport {
    clients: usize,
    messages_per_client: usize,
    expected_deliveries: u64,
    received: u64,
    delivery_pct: f64,
    elapsed_secs: f64,
    deliveries_per_sec: f64,
    send_rps: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    rss_mb: f64,
    cpu_pct: f64,
}

#[derive(Serialize)]
struct RoomReport {
    pairs: usize,
    messages_per_pair: usize,
    expected_deliveries: u64,
    received: u64,
    delivery_pct: f64,
    elapsed_secs: f64,
    deliveries_per_sec: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    rss_mb: f64,
    cpu_pct: f64,
}

struct ProcSample {
    cpu_pct: f64,
    rss_mb: f64,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("all");
    let server_pid: Option<u32> = std::env::var("SERVER_PID").ok().and_then(|s| s.parse().ok());

    match mode {
        "functional" => {
            let r = run_functional().await;
            println!("{}", serde_json::to_string_pretty(&r).unwrap());
            if !r.passed {
                std::process::exit(1);
            }
        }
        "connections" => {
            let only = args.get(2).and_then(|s| s.parse().ok());
            let r = run_connection_suite(server_pid, only).await;
            println!("{}", serde_json::to_string_pretty(&r).unwrap());
        }
        "ping" => {
            let r = run_ping_suite(server_pid).await;
            println!("{}", serde_json::to_string_pretty(&r).unwrap());
        }
        "broadcast" => {
            let r = run_broadcast_suite(server_pid).await;
            println!("{}", serde_json::to_string_pretty(&r).unwrap());
        }
        "rooms" => {
            let r = run_room_suite(server_pid).await;
            println!("{}", serde_json::to_string_pretty(&r).unwrap());
        }
        "all" => {
            let functional = run_functional().await;
            if !functional.passed {
                eprintln!("functional tests failed; aborting load tests");
                println!("{}", serde_json::to_string_pretty(&functional).unwrap());
                std::process::exit(1);
            }
            tokio::time::sleep(Duration::from_millis(400)).await;

            let connections = run_connection_suite(server_pid, None).await;
            tokio::time::sleep(Duration::from_millis(800)).await;

            let ping = run_ping_suite(server_pid).await;
            tokio::time::sleep(Duration::from_millis(800)).await;

            let broadcast = run_broadcast_suite(server_pid).await;
            tokio::time::sleep(Duration::from_millis(800)).await;

            let rooms = run_room_suite(server_pid).await;

            let report = SuiteReport {
                host: "Apple M4 Pro, 12 cores, 24 GB, localhost loopback".to_string(),
                url: URL.to_string(),
                functional,
                connections,
                ping,
                broadcast,
                rooms,
            };
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
        }
        other => {
            eprintln!("unknown mode {other}");
            std::process::exit(2);
        }
    }
}

async fn connect() -> Result<Ws, String> {
    let mut last = "connect failed".to_string();
    for attempt in 0..6 {
        match timeout(CONNECT_TIMEOUT, connect_async(URL)).await {
            Ok(Ok((ws, _))) => return Ok(ws),
            Ok(Err(e)) => {
                last = e.to_string();
                let retryable = last.contains("Can't assign")
                    || last.contains("Address already in use")
                    || last.contains("Too many open files");
                if !retryable {
                    return Err(last);
                }
            }
            Err(_) => last = "connect timeout".to_string(),
        }
        tokio::time::sleep(Duration::from_millis(40 * (attempt as u64 + 1))).await;
    }
    Err(last)
}

async fn send_text(write: &mut Writer, text: &str) -> Result<(), String> {
    write
        .send(Message::Text(text.to_string()))
        .await
        .map_err(|e| e.to_string())
}

async fn recv_text(read: &mut Reader, wait: Duration) -> Result<String, String> {
    loop {
        let msg = timeout(wait, read.next())
            .await
            .map_err(|_| "recv timeout".to_string())?
            .ok_or_else(|| "connection closed".to_string())?
            .map_err(|e| e.to_string())?;
        match msg {
            Message::Text(t) => return Ok(t),
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            other => return Err(format!("unexpected {other}")),
        }
    }
}

async fn run_functional() -> FunctionalReport {
    let mut checks = Vec::new();
    let mut ok_all = true;

    let mut push = |name: &str, ok: bool, detail: String| {
        if !ok {
            ok_all = false;
        }
        checks.push(Check {
            name: name.to_string(),
            ok,
            detail,
        });
    };

    let a = connect().await;
    let b = connect().await;
    let c = connect().await;
    if a.is_err() || b.is_err() || c.is_err() {
        push(
            "connect",
            false,
            format!("a={:?} b={:?} c={:?}", a.err(), b.err(), c.as_ref().err()),
        );
        return FunctionalReport {
            passed: false,
            checks,
        };
    }
    push("connect three clients", true, "ws handshake ok".into());

    let (mut aw, mut ar) = a.unwrap().split();
    let (mut bw, mut br) = b.unwrap().split();
    let (mut cw, mut cr) = c.unwrap().split();

    // /ping
    let ping_ok = async {
        send_text(&mut aw, "/ping").await?;
        let reply = recv_text(&mut ar, RECV_TIMEOUT).await?;
        if reply == "pong" {
            Ok(reply)
        } else {
            Err(format!("got {reply}"))
        }
    }
    .await;
    match ping_ok {
        Ok(_) => push("/ping", true, "pong".into()),
        Err(e) => push("/ping", false, e),
    }

    // names
    let name_ok = async {
        send_text(&mut aw, "/name Alice").await?;
        let ra = recv_text(&mut ar, RECV_TIMEOUT).await?;
        send_text(&mut bw, "/name Bob").await?;
        let rb = recv_text(&mut br, RECV_TIMEOUT).await?;
        send_text(&mut cw, "/name Cara").await?;
        let rc = recv_text(&mut cr, RECV_TIMEOUT).await?;
        if ra.contains("Alice") && rb.contains("Bob") && rc.contains("Cara") {
            Ok(format!("{ra} | {rb} | {rc}"))
        } else {
            Err(format!("{ra} | {rb} | {rc}"))
        }
    }
    .await;
    match name_ok {
        Ok(d) => push("/name", true, d),
        Err(e) => push("/name", false, e),
    }

    let who_ok = async {
        send_text(&mut aw, "/who").await?;
        let reply = recv_text(&mut ar, RECV_TIMEOUT).await?;
        let has_bob = reply.contains("Bob");
        let has_cara = reply.contains("Cara");
        if has_bob && has_cara {
            Ok(reply)
        } else {
            Err(reply)
        }
    }
    .await;
    match who_ok {
        Ok(d) => push("/who", true, d),
        Err(e) => push("/who", false, e),
    }

    let count_ok = async {
        send_text(&mut aw, "/count").await?;
        let reply = recv_text(&mut ar, RECV_TIMEOUT).await?;
        if reply.contains("3") {
            Ok(reply)
        } else {
            Err(reply)
        }
    }
    .await;
    match count_ok {
        Ok(d) => push("/count", true, d),
        Err(e) => push("/count", false, e),
    }

    let chat_ok = async {
        send_text(&mut aw, "hello-lobby").await?;
        let rb = recv_text(&mut br, RECV_TIMEOUT).await?;
        let rc = recv_text(&mut cr, RECV_TIMEOUT).await?;
        if rb.contains("Alice: hello-lobby") && rc.contains("Alice: hello-lobby") {
            Ok(format!("bob={rb} cara={rc}"))
        } else {
            Err(format!("bob={rb} cara={rc}"))
        }
    }
    .await;
    match chat_ok {
        Ok(d) => push("lobby broadcast", true, d),
        Err(e) => push("lobby broadcast", false, e),
    }

    let dm_ok = async {
        send_text(&mut aw, "/dm Bob secret-dm").await?;
        let rb = recv_text(&mut br, RECV_TIMEOUT).await?;
        if rb.contains("(DM from Alice)") && rb.contains("secret-dm") {
            Ok(rb)
        } else {
            Err(rb)
        }
    }
    .await;
    match dm_ok {
        Ok(d) => push("/dm", true, d),
        Err(e) => push("/dm", false, e),
    }

    let join_ok = async {
        send_text(&mut aw, "/join rust").await?;
        let ra = recv_text(&mut ar, RECV_TIMEOUT).await?;
        if !ra.contains("Joined room: rust") {
            return Err(ra);
        }
        send_text(&mut aw, "only-rust").await?;
        // Bob is still in lobby; should not see this. Give a short window.
        let leak = timeout(Duration::from_millis(400), recv_text(&mut br, Duration::from_millis(400))).await;
        match leak {
            Ok(Ok(msg)) if msg.contains("only-rust") => Err(format!("lobby leaked room msg: {msg}")),
            _ => Ok("room isolation ok".into()),
        }
    }
    .await;
    match join_ok {
        Ok(d) => push("room isolation", true, d),
        Err(e) => push("room isolation", false, e),
    }

    let room_chat_ok = async {
        send_text(&mut bw, "/join rust").await?;
        let rb = recv_text(&mut br, RECV_TIMEOUT).await?;
        if !rb.contains("Joined room: rust") && !rb.contains("only-rust") {
            // history of only-rust may arrive first
        }
        // drain a couple of history/join lines
        for _ in 0..4 {
            let _ = timeout(Duration::from_millis(200), recv_text(&mut br, Duration::from_millis(200))).await;
        }
        send_text(&mut aw, "rust-hello").await?;
        // Bob should receive; Cara (lobby) should not
        let rb = recv_text(&mut br, RECV_TIMEOUT).await?;
        let leak = timeout(Duration::from_millis(300), recv_text(&mut cr, Duration::from_millis(300))).await;
        let isolated = !matches!(leak, Ok(Ok(ref msg)) if msg.contains("rust-hello"));
        if rb.contains("Alice: rust-hello") && isolated {
            Ok(format!("bob={rb}"))
        } else {
            Err(format!("bob={rb} isolated={isolated}"))
        }
    }
    .await;
    match room_chat_ok {
        Ok(d) => push("same-room chat", true, d),
        Err(e) => push("same-room chat", false, e),
    }

    let rooms_ok = async {
        send_text(&mut cw, "/get_rooms").await?;
        let reply = recv_text(&mut cr, RECV_TIMEOUT).await?;
        if reply.contains("lobby") && reply.contains("rust") {
            Ok(reply)
        } else {
            Err(reply)
        }
    }
    .await;
    match rooms_ok {
        Ok(d) => push("/get_rooms", true, d),
        Err(e) => push("/get_rooms", false, e),
    }

    drop(aw);
    drop(bw);
    drop(cw);
    drop(ar);
    drop(br);
    drop(cr);

    FunctionalReport {
        passed: ok_all,
        checks,
    }
}

async fn connect_many(n: usize, batch: usize) -> (Vec<Ws>, usize) {
    let mut sockets = Vec::with_capacity(n);
    let mut failed = 0usize;
    let mut i = 0usize;
    while i < n {
        let end = (i + batch).min(n);
        let mut handles = Vec::new();
        for _ in i..end {
            handles.push(tokio::spawn(async { connect().await }));
        }
        for h in handles {
            match h.await {
                Ok(Ok(ws)) => sockets.push(ws),
                _ => failed += 1,
            }
        }
        i = end;
    }
    (sockets, failed)
}

fn sample_proc(pid: Option<u32>) -> ProcSample {
    let Some(pid) = pid else {
        return ProcSample {
            cpu_pct: 0.0,
            rss_mb: 0.0,
        };
    };
    let out = std::process::Command::new("ps")
        .args(["-o", "%cpu=,rss=", "-p", &pid.to_string()])
        .output()
        .ok();
    let Some(out) = out else {
        return ProcSample {
            cpu_pct: 0.0,
            rss_mb: 0.0,
        };
    };
    let s = String::from_utf8_lossy(&out.stdout);
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() >= 2 {
        ProcSample {
            cpu_pct: parts[0].parse().unwrap_or(0.0),
            rss_mb: parts[1].parse::<f64>().unwrap_or(0.0) / 1024.0,
        }
    } else {
        ProcSample {
            cpu_pct: 0.0,
            rss_mb: 0.0,
        }
    }
}

fn percentiles(mut xs: Vec<f64>) -> (f64, f64, f64) {
    if xs.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| {
        let idx = ((p / 100.0) * (xs.len() as f64 - 1.0)).round() as usize;
        xs[idx.min(xs.len() - 1)]
    };
    (pct(50.0), pct(95.0), pct(99.0))
}

async fn run_connection_suite(pid: Option<u32>, only: Option<usize>) -> Vec<ConnectionReport> {
    let mut out = Vec::new();
    let counts: Vec<usize> = match only {
        Some(n) => vec![n],
        None => vec![100, 500, 1000, 2500, 5000],
    };
    for n in counts {
        eprintln!("connections: targeting {n} ...");
        let t0 = Instant::now();
        let (sockets, failed) = connect_many(n, 250).await;
        let connect_secs = t0.elapsed().as_secs_f64();
        let connected = sockets.len();
        let sample = sample_proc(pid);

        let ping_ok = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for ws in sockets {
            let ping_ok = Arc::clone(&ping_ok);
            handles.push(tokio::spawn(async move {
                let (mut w, mut r) = ws.split();
                if send_text(&mut w, "/ping").await.is_ok() {
                    if let Ok(msg) = recv_text(&mut r, Duration::from_secs(5)).await {
                        if msg == "pong" {
                            ping_ok.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        let report = ConnectionReport {
            target: n,
            connected,
            failed,
            connect_secs,
            ping_ok: ping_ok.load(Ordering::Relaxed),
            rss_mb: sample.rss_mb,
            cpu_pct: sample.cpu_pct,
        };
        eprintln!(
            "  connected={} failed={} ping_ok={} in {:.2}s rss={:.1}MB",
            report.connected, report.failed, report.ping_ok, report.connect_secs, report.rss_mb
        );
        let keep_going = report.connected as f64 / n as f64 > 0.95;
        out.push(report);
        tokio::time::sleep(Duration::from_millis(500)).await;
        if !keep_going {
            break;
        }
    }
    out
}

async fn run_ping_suite(pid: Option<u32>) -> Vec<PingReport> {
    let mut out = Vec::new();
    for n in [50usize, 200, 500, 1000] {
        eprintln!("ping: {n} clients ...");
        let (sockets, failed) = connect_many(n, 200).await;
        if failed > n / 10 {
            eprintln!("  too many connect failures ({failed}), skip");
            continue;
        }
        let duration = Duration::from_secs(8);
        let pongs = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let latencies = Arc::new(Mutex::new(Vec::<f64>::new()));
        let mut handles = Vec::new();
        let t0 = Instant::now();
        for ws in sockets {
            let pongs = Arc::clone(&pongs);
            let errors = Arc::clone(&errors);
            let latencies = Arc::clone(&latencies);
            handles.push(tokio::spawn(async move {
                let (mut w, mut r) = ws.split();
                let deadline = Instant::now() + duration;
                while Instant::now() < deadline {
                    let start = Instant::now();
                    if send_text(&mut w, "/ping").await.is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    match recv_text(&mut r, Duration::from_secs(3)).await {
                        Ok(msg) if msg == "pong" => {
                            pongs.fetch_add(1, Ordering::Relaxed);
                            let ms = start.elapsed().as_secs_f64() * 1000.0;
                            let mut g = latencies.lock().await;
                            if g.len() < 50_000 {
                                g.push(ms);
                            }
                        }
                        _ => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        let elapsed = t0.elapsed().as_secs_f64();
        let xs = latencies.lock().await.clone();
        let (p50, p95, p99) = percentiles(xs);
        let sample = sample_proc(pid);
        let pong_n = pongs.load(Ordering::Relaxed);
        let report = PingReport {
            clients: n,
            duration_secs: elapsed,
            pongs: pong_n,
            errors: errors.load(Ordering::Relaxed),
            rps: pong_n as f64 / elapsed,
            p50_ms: p50,
            p95_ms: p95,
            p99_ms: p99,
            rss_mb: sample.rss_mb,
            cpu_pct: sample.cpu_pct,
        };
        eprintln!(
            "  rps={:.0} p50={:.2}ms p99={:.2}ms errors={}",
            report.rps, report.p50_ms, report.p99_ms, report.errors
        );
        out.push(report);
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    out
}

async fn run_broadcast_suite(pid: Option<u32>) -> Vec<BroadcastReport> {
    let mut out = Vec::new();
    let cases = [(25usize, 20usize), (50, 10), (100, 10), (250, 5), (500, 3)];
    for (n, msgs) in cases {
        eprintln!("broadcast: {n} clients x {msgs} msgs ...");
        match run_broadcast(n, msgs, pid).await {
            Ok(r) => {
                eprintln!(
                    "  delivery={:.1}% dps={:.0} p50={:.2}ms p99={:.2}ms",
                    r.delivery_pct, r.deliveries_per_sec, r.p50_ms, r.p99_ms
                );
                let good = r.delivery_pct > 90.0;
                out.push(r);
                if !good {
                    break;
                }
            }
            Err(e) => {
                eprintln!("  failed: {e}");
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
    out
}

async fn run_broadcast(n: usize, msgs: usize, pid: Option<u32>) -> Result<BroadcastReport, String> {
    let (sockets, failed) = connect_many(n, 200).await;
    if failed > 0 {
        eprintln!("  connect failed={failed}");
    }
    let expected = (sockets.len() as u64) * (msgs as u64) * (sockets.len().saturating_sub(1) as u64);
    let received = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Mutex::new(Vec::<f64>::new()));
    let n_actual = sockets.len();
    let barrier = Arc::new(tokio::sync::Barrier::new(n_actual + 1));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut handles = Vec::new();

    for (idx, ws) in sockets.into_iter().enumerate() {
        let received = Arc::clone(&received);
        let latencies = Arc::clone(&latencies);
        let barrier = Arc::clone(&barrier);
        let mut stop_rx = stop_rx.clone();
        handles.push(tokio::spawn(async move {
            let (mut w, mut r) = ws.split();
            let _ = send_text(&mut w, &format!("/name u{idx}")).await;
            let _ = timeout(Duration::from_secs(3), async {
                loop {
                    let msg = recv_text(&mut r, Duration::from_secs(3)).await.ok()?;
                    if msg.starts_with("name set to") {
                        break Some(());
                    }
                }
            })
            .await;
            barrier.wait().await;

            let reader = tokio::spawn({
                let received = Arc::clone(&received);
                let latencies = Arc::clone(&latencies);
                async move {
                    loop {
                        match recv_text(&mut r, Duration::from_secs(20)).await {
                            Ok(msg) => {
                                if let Some(ms) = parse_bench_latency(&msg) {
                                    received.fetch_add(1, Ordering::Relaxed);
                                    let mut g = latencies.lock().await;
                                    if g.len() < 80_000 {
                                        g.push(ms);
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            });

            for seq in 0..msgs {
                let ts = now_ns();
                let payload = format!("BENCH|{idx}|{seq}|{ts}");
                if send_text(&mut w, &payload).await.is_err() {
                    break;
                }
            }
            let _ = stop_rx.wait_for(|stop| *stop).await;
            drop(w);
            let _ = reader.await;
        }));
    }

    if timeout(Duration::from_secs(25), barrier.wait()).await.is_err() {
        let _ = stop_tx.send(true);
        return Err("clients did not become ready".into());
    }
    let t0 = Instant::now();
    let deadline = Duration::from_secs(30);
    while received.load(Ordering::Relaxed) < expected && t0.elapsed() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let elapsed = t0.elapsed().as_secs_f64().max(0.001);
    let sample = sample_proc(pid);
    let _ = stop_tx.send(true);
    for h in handles {
        let _ = timeout(Duration::from_secs(5), h).await;
    }
    let got = received.load(Ordering::Relaxed);
    let xs = latencies.lock().await.clone();
    let (p50, p95, p99) = percentiles(xs);
    Ok(BroadcastReport {
        clients: n_actual,
        messages_per_client: msgs,
        expected_deliveries: expected,
        received: got,
        delivery_pct: if expected == 0 {
            0.0
        } else {
            100.0 * got as f64 / expected as f64
        },
        elapsed_secs: elapsed,
        deliveries_per_sec: got as f64 / elapsed,
        send_rps: (n_actual * msgs) as f64 / elapsed,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        rss_mb: sample.rss_mb,
        cpu_pct: sample.cpu_pct,
    })
}

async fn run_room_suite(pid: Option<u32>) -> Vec<RoomReport> {
    let mut out = Vec::new();
    for (pairs, msgs) in [(50usize, 10usize), (150, 8), (300, 5)] {
        eprintln!("rooms: {pairs} pairs x {msgs} msgs ...");
        match run_rooms(pairs, msgs, pid).await {
            Ok(r) => {
                eprintln!(
                    "  delivery={:.1}% dps={:.0} p50={:.2}ms p99={:.2}ms",
                    r.delivery_pct, r.deliveries_per_sec, r.p50_ms, r.p99_ms
                );
                let good = r.delivery_pct > 90.0;
                out.push(r);
                if !good {
                    break;
                }
            }
            Err(e) => {
                eprintln!("  failed: {e}");
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
    out
}

async fn run_rooms(pairs: usize, msgs: usize, pid: Option<u32>) -> Result<RoomReport, String> {
    let n = pairs * 2;
    let (sockets, failed) = connect_many(n, 200).await;
    if failed > 0 {
        eprintln!("  connect failed={failed}");
    }
    if sockets.len() < 2 {
        return Err("not enough sockets".into());
    }
    let n_actual = sockets.len();
    let actual_pairs = n_actual / 2;
    let expected = (actual_pairs as u64) * (msgs as u64) * 2;
    let received = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Mutex::new(Vec::<f64>::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(n_actual + 1));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut handles = Vec::new();

    for (idx, ws) in sockets.into_iter().enumerate() {
        let pair = idx / 2;
        let received = Arc::clone(&received);
        let latencies = Arc::clone(&latencies);
        let barrier = Arc::clone(&barrier);
        let mut stop_rx = stop_rx.clone();
        handles.push(tokio::spawn(async move {
            let (mut w, mut r) = ws.split();
            let _ = send_text(&mut w, &format!("/name r{idx}")).await;
            let _ = timeout(Duration::from_secs(3), recv_text(&mut r, Duration::from_secs(3))).await;
            let _ = send_text(&mut w, &format!("/join room{pair}")).await;
            let _ = timeout(Duration::from_secs(3), recv_text(&mut r, Duration::from_secs(3))).await;
            for _ in 0..8 {
                if timeout(Duration::from_millis(40), recv_text(&mut r, Duration::from_millis(40)))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .is_none()
                {
                    break;
                }
            }
            barrier.wait().await;

            let reader = tokio::spawn({
                let received = Arc::clone(&received);
                let latencies = Arc::clone(&latencies);
                async move {
                    loop {
                        match recv_text(&mut r, Duration::from_secs(20)).await {
                            Ok(msg) => {
                                if let Some(ms) = parse_bench_latency(&msg) {
                                    received.fetch_add(1, Ordering::Relaxed);
                                    let mut g = latencies.lock().await;
                                    if g.len() < 80_000 {
                                        g.push(ms);
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            });

            for seq in 0..msgs {
                let ts = now_ns();
                let payload = format!("BENCH|{idx}|{seq}|{ts}");
                let _ = send_text(&mut w, &payload).await;
            }
            let _ = stop_rx.wait_for(|stop| *stop).await;
            drop(w);
            let _ = reader.await;
        }));
    }

    if timeout(Duration::from_secs(25), barrier.wait()).await.is_err() {
        let _ = stop_tx.send(true);
        return Err("clients did not become ready".into());
    }
    let t0 = Instant::now();
    let deadline = Duration::from_secs(25);
    while received.load(Ordering::Relaxed) < expected && t0.elapsed() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let elapsed = t0.elapsed().as_secs_f64().max(0.001);
    let sample = sample_proc(pid);
    let _ = stop_tx.send(true);
    for h in handles {
        let _ = timeout(Duration::from_secs(5), h).await;
    }
    let got = received.load(Ordering::Relaxed);
    let xs = latencies.lock().await.clone();
    let (p50, p95, p99) = percentiles(xs);
    Ok(RoomReport {
        pairs: actual_pairs,
        messages_per_pair: msgs,
        expected_deliveries: expected,
        received: got,
        delivery_pct: if expected == 0 {
            0.0
        } else {
            100.0 * got as f64 / expected as f64
        },
        elapsed_secs: elapsed,
        deliveries_per_sec: got as f64 / elapsed,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        rss_mb: sample.rss_mb,
        cpu_pct: sample.cpu_pct,
    })
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn parse_bench_latency(msg: &str) -> Option<f64> {
    // "u12: BENCH|12|3|<nanos>"
    let payload = msg.split_once(": ")?.1;
    let mut parts = payload.split('|');
    if parts.next()? != "BENCH" {
        return None;
    }
    let _id = parts.next()?;
    let _seq = parts.next()?;
    let ts: u128 = parts.next()?.parse().ok()?;
    let now = now_ns();
    if now < ts {
        return Some(0.0);
    }
    Some((now - ts) as f64 / 1_000_000.0)
}
