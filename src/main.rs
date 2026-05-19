//! jay-miner — headless Rust client that mirrors the Jay Network browser-mining protocol.
//!
//! What this binary does (and does NOT do):
//!
//! - It does NOT compute any real proof-of-work. The Jay Network is a Cosmos SDK +
//!   CometBFT (Tendermint) chain, which is Byzantine-Fault-Tolerant Proof-of-Stake —
//!   there are no PoW puzzles to solve.
//! - The pool at `wss://api-pool.winnode.xyz` accepts `submit_share` messages whose
//!   `hash` field is a random 64-character hex string. We reproduce that protocol
//!   verbatim so a single-wallet operator can mine from a headless box (e.g. a VPS)
//!   without keeping a browser open.
//!
//! Use responsibly. See README for the full disclaimer.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rand::{rngs::OsRng, Rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, sleep, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};
use tracing_subscriber::fmt::format::FmtSpan;
use url::Url;

const DEFAULT_TOKEN_URL: &str = "https://mining.thejaynetwork.com/api/ws-token";
const DEFAULT_POOL_WS: &str = "wss://api-pool.winnode.xyz";
const DEFAULT_REST_URL: &str = "https://api-jayn.winnode.xyz";
const PING_INTERVAL: Duration = Duration::from_secs(30);
const SHARE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const SHARE_DIFFICULTY: u64 = 1_000_000;
const TOKEN_DENOM: &str = "ujay";
const TOKEN_DECIMALS: u32 = 6;

/// Chrome 120 on Linux — keeps the request from looking obviously bot-shaped.
const BROWSER_UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) \
     Chrome/120.0.0.0 Safari/537.36";

/// Headless miner for The Jay Network browser pool (`api-pool.winnode.xyz`).
///
/// Mirrors the wire protocol the official frontend at https://mining.thejaynetwork.com
/// speaks over WebSocket. Use a single wallet / single device per running instance
/// to stay within the pool's expected usage shape.
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Bech32 wallet address that should receive mining rewards (must start with `yjay`).
    #[arg(long, env = "JAY_WALLET")]
    wallet: String,

    /// Number of worker threads to report to the pool.
    ///
    /// This is what the pool uses to compute its reported "hashrate" for your session;
    /// it is NOT used to spawn CPU threads on your machine (no actual hashing happens).
    #[arg(long, default_value_t = 4, env = "JAY_THREADS")]
    threads: u32,

    /// Probability (0.0..=1.0) of emitting a share each second. Mirrors the frontend's
    /// "intensity" slider. Lower values look less aggressive to anti-abuse heuristics.
    #[arg(long, default_value_t = 1.0, env = "JAY_INTENSITY")]
    intensity: f64,

    /// Where to fetch the WebSocket token from (HTTPS POST that returns `{"token": "..."}`).
    #[arg(long, default_value = DEFAULT_TOKEN_URL, env = "JAY_TOKEN_URL")]
    token_url: String,

    /// Base WebSocket URL of the pool. The token is appended as `?token=<value>`.
    #[arg(long, default_value = DEFAULT_POOL_WS, env = "JAY_POOL_WS")]
    pool_ws: String,

    /// Base URL of the Cosmos REST API used to query on-chain balance.
    /// Path queried is `/cosmos/bank/v1beta1/balances/{wallet}`.
    #[arg(long, default_value = DEFAULT_REST_URL, env = "JAY_REST_URL")]
    rest_url: String,

    /// Path to persist the device ID across runs (mirrors the frontend's `localStorage`).
    ///
    /// Defaults to `./device_id` in the working directory. Delete the file to rotate
    /// the device identity.
    #[arg(long, default_value = "device_id", env = "JAY_DEVICE_ID_FILE")]
    device_id_file: PathBuf,

    /// How often (seconds) to print the periodic status panel. Set 0 to disable.
    #[arg(long, default_value_t = 10, env = "JAY_STATS_INTERVAL")]
    stats_interval: u64,

    /// How often (seconds) to refresh the on-chain JAY balance. Set 0 to disable.
    #[arg(long, default_value_t = 60, env = "JAY_BALANCE_INTERVAL")]
    balance_interval: u64,

    /// Reconnect attempt limit before exiting. Set 0 to retry forever. Mirrors the
    /// frontend's 5-attempt cap.
    #[arg(long, default_value_t = 0, env = "JAY_MAX_RECONNECTS")]
    max_reconnects: u32,

    /// Verbose logging (sets `RUST_LOG=debug` unless already set).
    #[arg(long)]
    verbose: bool,
}

#[derive(Debug, Clone)]
struct MinerIds {
    /// Per-run, regenerated each time `jay-miner` starts. Mirrors `sessionStorage`.
    session_id: String,
    /// Persistent across runs, kept in `--device-id-file`. Mirrors `localStorage`.
    device_id: String,
    /// Assigned by the server in `auth_success` / `mining_started`. None until then.
    miner_id: Arc<Mutex<Option<String>>>,
    /// Current job from the server (jobId pushed via the `job` message).
    job_id: Arc<Mutex<String>>,
}

impl MinerIds {
    async fn load(device_file: &PathBuf) -> Result<Self> {
        let session_id = format!("session_{}_{}", current_millis(), base36_random(13));

        let device_id = match tokio::fs::read_to_string(device_file).await {
            Ok(s) => {
                let trimmed = s.trim().to_string();
                if trimmed.is_empty() {
                    let id = new_device_id();
                    tokio::fs::write(device_file, &id).await.with_context(|| {
                        format!("writing device id to {}", device_file.display())
                    })?;
                    id
                } else {
                    trimmed
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let id = new_device_id();
                if let Some(parent) = device_file.parent() {
                    if !parent.as_os_str().is_empty() {
                        tokio::fs::create_dir_all(parent).await.ok();
                    }
                }
                tokio::fs::write(device_file, &id)
                    .await
                    .with_context(|| format!("writing device id to {}", device_file.display()))?;
                id
            }
            Err(e) => return Err(e).context("reading device id file"),
        };

        Ok(Self {
            session_id,
            device_id,
            miner_id: Arc::new(Mutex::new(None)),
            job_id: Arc::new(Mutex::new(String::new())),
        })
    }

    async fn miner_id_value(&self) -> Option<String> {
        self.miner_id.lock().await.clone()
    }

    async fn job_id_value(&self) -> String {
        self.job_id.lock().await.clone()
    }
}

/// Shared mining stats updated by the share/share-result/reward handlers and read
/// by the periodic stats panel + final summary.
#[derive(Debug, Default)]
struct Stats {
    shares_submitted: u64,
    shares_accepted: u64,
    shares_rejected: u64,
    reward_events: u64,
    total_reward_jay: f64,
    last_reward_jay: f64,
    last_reward_tx: Option<String>,
    last_submitted_hash: Option<String>,
    last_submitted_nonce: u32,
    last_balance_jay: Option<f64>,
    last_balance_at: Option<Instant>,
}

fn current_millis() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn base36_random(len: usize) -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut rng = OsRng;
    (0..len)
        .map(|_| {
            let idx = (rng.next_u32() as usize) % ALPHABET.len();
            ALPHABET[idx] as char
        })
        .collect()
}

fn new_device_id() -> String {
    format!("device_{}_{}", current_millis(), base36_random(13))
}

/// 64-character hex string, identical in shape to what the frontend emits:
/// `Array.from({length:64}, () => Math.floor(16*Math.random()).toString(16)).join("")`.
fn random_hex_64() -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut rng = OsRng;
    let mut out = String::with_capacity(64);
    for _ in 0..64 {
        let idx = (rng.next_u32() & 0xF) as usize;
        out.push(HEX[idx] as char);
    }
    out
}

/// Format a `Duration` as `1h02m03s` / `5m23s` / `42s`.
fn format_uptime(d: Duration) -> String {
    let total = d.as_secs();
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    if h > 0 {
        format!("{h}h{m:02}m{s:02}s")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}

/// Shorten a 64-char hex hash to `abcd1234…ef567890` for compact log display.
fn short_hash(h: &str) -> String {
    if h.len() <= 18 {
        h.to_string()
    } else {
        format!("{}…{}", &h[..8], &h[h.len() - 8..])
    }
}

/// Trim a possibly-huge body (Vercel security checkpoint pages can be many KB of
/// HTML/SVG) down to a single line of at most `max` chars, with a clear marker
/// when truncated. Whitespace runs collapse to single spaces so the log stays
/// on one line.
fn truncate_body(body: &str, max: usize) -> String {
    let mut out = String::with_capacity(body.len().min(max + 32));
    let mut last_was_ws = false;
    let mut chars = 0;
    for ch in body.chars() {
        if ch.is_whitespace() {
            if !last_was_ws {
                out.push(' ');
                chars += 1;
                last_was_ws = true;
            }
        } else {
            out.push(ch);
            chars += 1;
            last_was_ws = false;
        }
        if chars >= max {
            out.push_str(&format!(" …[truncated, full body {} bytes]", body.len()));
            return out;
        }
    }
    out
}

/// Detect Vercel's anti-bot challenge page so we can surface a clearer error
/// message than "HTTP 200, body is HTML".
fn is_vercel_checkpoint(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.contains("vercel security checkpoint") || lower.contains("enable javascript to continue")
}

#[derive(Deserialize)]
struct TokenResponse {
    token: String,
}

/// Build a `reqwest::Client` that looks like Chrome — UA, Accept-Language,
/// cookie jar (needed for some Vercel/Cloudflare flows). Cookies persist for
/// the lifetime of the process so a successful first request can keep us
/// inside the trusted bucket on subsequent calls.
fn build_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(BROWSER_UA)
        .timeout(Duration::from_secs(15))
        .cookie_store(true)
        .gzip(true)
        .build()
        .context("building HTTP client")
}

async fn fetch_ws_token(client: &reqwest::Client, token_url: &str) -> Result<String> {
    let resp = client
        .post(token_url)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/plain, */*")
        .header("accept-language", "en-US,en;q=0.9")
        .header("origin", "https://mining.thejaynetwork.com")
        .header("referer", "https://mining.thejaynetwork.com/")
        .header("sec-fetch-dest", "empty")
        .header("sec-fetch-mode", "cors")
        .header("sec-fetch-site", "same-origin")
        .body("{}")
        .send()
        .await
        .with_context(|| format!("POST {token_url}"))?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        if is_vercel_checkpoint(&body) {
            return Err(anyhow!(
                "ws-token blocked by Vercel security checkpoint (HTTP {status}). \
                 Try again from a different IP, or wait a few minutes for the rate-limit \
                 to expire."
            ));
        }
        return Err(anyhow!(
            "ws-token request failed: HTTP {status}: {}",
            truncate_body(&body, 200)
        ));
    }

    let parsed: TokenResponse = serde_json::from_str(&body).with_context(|| {
        if is_vercel_checkpoint(&body) {
            "ws-token response was Vercel checkpoint HTML — pool is throttling this IP. \
             Slow down reconnects or rotate IP."
                .to_string()
        } else {
            format!(
                "decoding ws-token response as {{\"token\":\"...\"}} — got: {}",
                truncate_body(&body, 200)
            )
        }
    })?;
    if parsed.token.is_empty() {
        return Err(anyhow!("ws-token response contained empty token"));
    }
    Ok(parsed.token)
}

/// Cosmos REST API balance response shape: `{ balances: [{denom, amount}], pagination }`.
#[derive(Deserialize)]
struct BalancesResponse {
    balances: Vec<Coin>,
}

#[derive(Deserialize)]
struct Coin {
    denom: String,
    amount: String,
}

/// Query the `ujay` balance for `wallet` and return it as JAY (divided by 10^6).
async fn fetch_balance(client: &reqwest::Client, rest_url: &str, wallet: &str) -> Result<f64> {
    let url = format!(
        "{}/cosmos/bank/v1beta1/balances/{}",
        rest_url.trim_end_matches('/'),
        wallet
    );
    let resp = client
        .get(&url)
        .header("accept", "application/json")
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "balance query HTTP {status}: {}",
            truncate_body(&body, 200)
        ));
    }
    let parsed: BalancesResponse = serde_json::from_str(&body).with_context(|| {
        format!(
            "decoding balance response — got: {}",
            truncate_body(&body, 200)
        )
    })?;
    let raw = parsed
        .balances
        .iter()
        .find(|c| c.denom == TOKEN_DENOM)
        .map(|c| c.amount.parse::<u128>().unwrap_or(0))
        .unwrap_or(0);
    let divisor = 10f64.powi(TOKEN_DECIMALS as i32);
    Ok(raw as f64 / divisor)
}

#[derive(Debug, Serialize)]
struct ClientMessage<'a> {
    #[serde(rename = "type")]
    msg_type: &'a str,
    payload: Value,
}

async fn run_session(
    cli: &Cli,
    ids: &MinerIds,
    stats: &Arc<Mutex<Stats>>,
    http: &reqwest::Client,
    started_at: Instant,
) -> Result<()> {
    info!("Requesting WebSocket token from {}", cli.token_url);
    let token = fetch_ws_token(http, &cli.token_url).await?;
    debug!("got ws-token ({} bytes)", token.len());

    let mut ws_url = Url::parse(&cli.pool_ws)
        .with_context(|| format!("parsing pool ws URL: {}", cli.pool_ws))?;
    ws_url
        .query_pairs_mut()
        .clear()
        .append_pair("token", &token);
    info!("Connecting to pool: {}", redact_token(&ws_url));

    let (ws_stream, _resp) = tokio_tungstenite::connect_async(ws_url.as_str())
        .await
        .with_context(|| "WebSocket connect failed")?;
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    send_json(
        &mut ws_tx,
        &ClientMessage {
            msg_type: "status",
            payload: json!({
                "sessionId": ids.session_id,
                "deviceId": ids.device_id,
                "status": "online",
                "wallet": cli.wallet,
            }),
        },
    )
    .await?;

    let start_payload = {
        let miner_id = ids.miner_id_value().await;
        let mut payload = json!({
            "wallet": cli.wallet,
            "threads": cli.threads,
            "sessionId": ids.session_id,
            "deviceId": ids.device_id,
        });
        if let Some(m) = miner_id {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("minerId".into(), Value::String(m));
            }
        }
        payload
    };
    send_json(
        &mut ws_tx,
        &ClientMessage {
            msg_type: "start_mining",
            payload: start_payload,
        },
    )
    .await?;
    info!(
        "mining session started — wallet={} threads={} intensity={:.2}",
        cli.wallet, cli.threads, cli.intensity
    );

    // Outbound channel feeding the WebSocket writer task.
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(64);

    // --- ping every 30 s
    let ping_tx = out_tx.clone();
    let ids_for_ping = ids.clone();
    let ping_task = tokio::spawn(async move {
        let mut tick = interval(PING_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            let payload = json!({
                "sessionId": ids_for_ping.session_id,
                "deviceId": ids_for_ping.device_id,
                "status": "online",
            });
            let msg = serde_json::to_string(&ClientMessage {
                msg_type: "ping",
                payload,
            })
            .expect("ping serialization");
            if ping_tx.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // --- submit_share every 1 s (gated by intensity)
    let share_tx = out_tx.clone();
    let ids_for_share = ids.clone();
    let wallet_for_share = cli.wallet.clone();
    let intensity = cli.intensity.clamp(0.0, 1.0);
    let stats_for_share = Arc::clone(stats);
    let share_task = tokio::spawn(async move {
        let mut tick = interval(SHARE_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            let roll: f64 = OsRng.gen();
            if roll >= intensity {
                continue;
            }
            let miner_id = ids_for_share.miner_id_value().await.unwrap_or_default();
            let job_id = ids_for_share.job_id_value().await;
            let nonce: u32 = OsRng.gen_range(0..1_000_000);
            let hash = random_hex_64();
            let payload = json!({
                "nonce": nonce,
                "hash": hash,
                "jobId": job_id,
                "difficulty": SHARE_DIFFICULTY,
                "sessionId": ids_for_share.session_id,
                "deviceId": ids_for_share.device_id,
                "minerId": miner_id,
                "wallet": wallet_for_share,
            });
            let msg = serde_json::to_string(&ClientMessage {
                msg_type: "submit_share",
                payload,
            })
            .expect("share serialization");
            {
                let mut s = stats_for_share.lock().await;
                s.shares_submitted += 1;
                s.last_submitted_hash = Some(hash);
                s.last_submitted_nonce = nonce;
            }
            if share_tx.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // --- periodic on-chain balance fetch
    let balance_task = if cli.balance_interval > 0 {
        let http = http.clone();
        let rest_url = cli.rest_url.clone();
        let wallet = cli.wallet.clone();
        let stats = Arc::clone(stats);
        let interval_secs = cli.balance_interval;
        Some(tokio::spawn(async move {
            // First fetch happens immediately so the stats panel has a value to show.
            loop {
                match fetch_balance(&http, &rest_url, &wallet).await {
                    Ok(bal) => {
                        let mut s = stats.lock().await;
                        s.last_balance_jay = Some(bal);
                        s.last_balance_at = Some(Instant::now());
                    }
                    Err(e) => {
                        debug!("balance refresh failed: {e:#}");
                    }
                }
                sleep(Duration::from_secs(interval_secs)).await;
            }
        }))
    } else {
        None
    };

    // --- periodic stats panel
    let stats_task = if cli.stats_interval > 0 {
        let stats = Arc::clone(stats);
        let interval_secs = cli.stats_interval;
        Some(tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(interval_secs));
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tick.tick().await; // skip the immediate first tick so the panel appears after one period
            loop {
                tick.tick().await;
                let snapshot = {
                    let s = stats.lock().await;
                    (
                        s.shares_submitted,
                        s.shares_accepted,
                        s.shares_rejected,
                        s.reward_events,
                        s.total_reward_jay,
                        s.last_reward_jay,
                        s.last_submitted_hash.clone(),
                        s.last_submitted_nonce,
                        s.last_balance_jay,
                    )
                };
                let (sub, acc, rej, rewards, total, last_r, hash, nonce, bal) = snapshot;
                let uptime = format_uptime(started_at.elapsed());
                let hash_disp = hash
                    .as_deref()
                    .map(short_hash)
                    .unwrap_or_else(|| "—".to_string());
                let bal_disp = bal
                    .map(|b| format!("{b:.6} JAY"))
                    .unwrap_or_else(|| "(querying)".to_string());
                info!(
                    "[stats] up {uptime} | shares {acc}/{sub} ok ({rej} rej) | \
                     rewards {rewards} ({total:.6} JAY, last {last_r:.6}) | \
                     balance {bal_disp} | last_hash {hash_disp} nonce={nonce}"
                );
            }
        }))
    } else {
        None
    };

    // --- WebSocket writer
    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if let Err(e) = ws_tx.send(msg).await {
                warn!("WebSocket send failed: {e}");
                break;
            }
        }
        let _ = ws_tx.close().await;
    });

    let shutdown = tokio::spawn(async {
        wait_for_shutdown().await;
    });

    let read_loop = async {
        while let Some(frame) = ws_rx.next().await {
            match frame {
                Ok(Message::Text(text)) => {
                    handle_server_message(&text, ids, stats).await;
                }
                Ok(Message::Binary(_)) => debug!("ignoring binary frame"),
                Ok(Message::Ping(p)) => {
                    let _ = out_tx.send(Message::Pong(p)).await;
                }
                Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                Ok(Message::Close(c)) => {
                    info!("pool closed the connection: {c:?}");
                    break;
                }
                Err(e) => {
                    warn!("WebSocket read error: {e}");
                    break;
                }
            }
        }
    };

    tokio::select! {
        _ = read_loop => {
            info!("WebSocket stream ended");
        }
        _ = shutdown => {
            info!("Shutdown signal received; sending stop_mining + offline status");
            let miner_id = ids.miner_id_value().await.unwrap_or_default();
            let stop = serde_json::to_string(&ClientMessage {
                msg_type: "stop_mining",
                payload: json!({
                    "sessionId": ids.session_id,
                    "deviceId": ids.device_id,
                    "minerId": miner_id,
                    "wallet": cli.wallet,
                }),
            }).unwrap();
            let _ = out_tx.send(Message::Text(stop)).await;
            let offline = serde_json::to_string(&ClientMessage {
                msg_type: "status",
                payload: json!({
                    "sessionId": ids.session_id,
                    "deviceId": ids.device_id,
                    "status": "offline",
                    "wallet": cli.wallet,
                }),
            }).unwrap();
            let _ = out_tx.send(Message::Text(offline)).await;
            sleep(Duration::from_millis(250)).await;
        }
    }

    drop(out_tx);
    ping_task.abort();
    share_task.abort();
    if let Some(t) = balance_task {
        t.abort();
    }
    if let Some(t) = stats_task {
        t.abort();
    }
    let _ = writer.await;

    let final_snap = {
        let s = stats.lock().await;
        (
            s.shares_accepted,
            s.shares_rejected,
            s.shares_submitted,
            s.total_reward_jay,
            s.reward_events,
        )
    };
    info!(
        "session finished — shares {}/{} ok ({} rej), {} reward events totaling {:.6} JAY",
        final_snap.0, final_snap.2, final_snap.1, final_snap.4, final_snap.3
    );
    Ok(())
}

async fn send_json<S>(sink: &mut S, msg: &ClientMessage<'_>) -> Result<()>
where
    S: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let text = serde_json::to_string(msg).context("serializing client message")?;
    debug!("ws -> pool: {text}");
    sink.send(Message::Text(text))
        .await
        .context("WebSocket send")
}

async fn handle_server_message(text: &str, ids: &MinerIds, stats: &Arc<Mutex<Stats>>) {
    let parsed: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "non-JSON server message ({e}): {}",
                truncate_body(text, 120)
            );
            return;
        }
    };

    let msg_type = parsed
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");
    let payload = parsed.get("payload").cloned().unwrap_or(Value::Null);

    match msg_type {
        "auth_success" | "mining_started" => {
            if let Some(miner_id) = payload.get("minerId").and_then(Value::as_str) {
                {
                    let mut slot = ids.miner_id.lock().await;
                    *slot = Some(miner_id.to_string());
                }
                info!("{msg_type} (minerId={miner_id})");
            } else {
                info!("{msg_type}");
            }
        }
        "job" => {
            if let Some(job_id) = payload.get("jobId").and_then(Value::as_str) {
                let target = payload
                    .get("target")
                    .and_then(Value::as_str)
                    .unwrap_or("ffffffff");
                {
                    let mut slot = ids.job_id.lock().await;
                    *slot = job_id.to_string();
                }
                debug!("new job: id={job_id} target={target}");
            }
        }
        "share_accepted" => {
            let mut s = stats.lock().await;
            s.shares_accepted += 1;
            debug!("share accepted (total {})", s.shares_accepted);
        }
        "share_rejected" => {
            let reason = payload.get("reason").and_then(Value::as_str).unwrap_or("?");
            let mut s = stats.lock().await;
            s.shares_rejected += 1;
            warn!(
                "share rejected ({reason}) — total rejected {}",
                s.shares_rejected
            );
        }
        "mining_reward" => {
            let amount = payload.get("amount").and_then(Value::as_f64).unwrap_or(0.0);
            let shares = payload.get("shares").and_then(Value::as_u64).unwrap_or(0);
            let tx_hash = payload
                .get("txHash")
                .and_then(Value::as_str)
                .map(String::from);
            let tx_disp = tx_hash
                .as_deref()
                .map(short_hash)
                .unwrap_or_else(|| "—".to_string());
            {
                let mut s = stats.lock().await;
                s.reward_events += 1;
                s.total_reward_jay += amount;
                s.last_reward_jay = amount;
                s.last_reward_tx = tx_hash;
            }
            let running = {
                let s = stats.lock().await;
                s.total_reward_jay
            };
            info!(
                "reward +{amount:.6} JAY (shares {shares}, tx {tx_disp}) — running total {running:.6} JAY"
            );
        }
        "block_found" => info!("block_found: {payload}"),
        "pool_stats" => debug!("pool_stats: {payload}"),
        "pong" => debug!("pong"),
        other => debug!("server message {other}: {payload}"),
    }
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                warn!("failed to install SIGTERM handler ({e}); only Ctrl+C will shut down");
                tokio::signal::ctrl_c().await.ok();
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn redact_token(url: &Url) -> String {
    let mut copy = url.clone();
    let pairs: Vec<(String, String)> = copy
        .query_pairs()
        .map(|(k, v)| {
            let v = if k == "token" {
                format!("<{}-char-redacted>", v.len())
            } else {
                v.into_owned()
            };
            (k.into_owned(), v)
        })
        .collect();
    copy.query_pairs_mut().clear();
    for (k, v) in pairs {
        copy.query_pairs_mut().append_pair(&k, &v);
    }
    copy.to_string()
}

fn validate_wallet(addr: &str) -> Result<()> {
    if !addr.starts_with("yjay") {
        return Err(anyhow!(
            "wallet must be a Jay Network bech32 address starting with `yjay` (got: {addr})"
        ));
    }
    if addr.len() < 20 {
        return Err(anyhow!("wallet address looks too short: {addr}"));
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let log_default = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_default)),
        )
        .with_target(false)
        .with_span_events(FmtSpan::NONE)
        .compact()
        .init();

    validate_wallet(&cli.wallet)?;
    if !(0.0..=1.0).contains(&cli.intensity) {
        return Err(anyhow!(
            "--intensity must be in 0.0..=1.0 (got {})",
            cli.intensity
        ));
    }

    let ids = MinerIds::load(&cli.device_id_file).await?;
    info!(
        "miner identifiers ready — sessionId={} deviceId={}",
        ids.session_id, ids.device_id
    );

    let stats = Arc::new(Mutex::new(Stats::default()));
    let http = build_http_client()?;
    let started_at = Instant::now();

    let mut attempt: u32 = 0;
    loop {
        match run_session(&cli, &ids, &stats, &http, started_at).await {
            Ok(()) => {
                info!("session ended cleanly");
                break Ok(());
            }
            Err(e) => {
                attempt += 1;
                // `{e:#}` gives "outer cause: inner cause: leaf" without a 200-line
                // multi-line Debug dump.
                warn!("session error (attempt {attempt}): {e:#}");
                if cli.max_reconnects != 0 && attempt >= cli.max_reconnects {
                    return Err(e.context(format!(
                        "giving up after {} reconnect attempts",
                        cli.max_reconnects
                    )));
                }
                let backoff = Duration::from_millis(
                    1000u64
                        .saturating_mul(1u64 << attempt.min(5))
                        .min(MAX_BACKOFF.as_millis() as u64),
                );
                info!("reconnecting in {}s (attempt {attempt})", backoff.as_secs());
                sleep(backoff).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_hex_64_has_expected_shape() {
        for _ in 0..1000 {
            let h = random_hex_64();
            assert_eq!(h.len(), 64, "random_hex_64 must be exactly 64 chars");
            assert!(
                h.chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                "random_hex_64 must be lowercase hex: {h}"
            );
        }
    }

    #[test]
    fn base36_random_length_and_alphabet() {
        for len in [1usize, 5, 13, 32] {
            let s = base36_random(len);
            assert_eq!(s.len(), len);
            assert!(
                s.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
                "base36_random must be lowercase alphanumeric: {s}"
            );
        }
    }

    #[test]
    fn new_device_id_format() {
        let id = new_device_id();
        assert!(id.starts_with("device_"), "got {id}");
        let parts: Vec<&str> = id.splitn(3, '_').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "device");
        assert!(
            parts[1].parse::<u128>().is_ok(),
            "millis should be numeric: {id}"
        );
        assert_eq!(parts[2].len(), 13);
    }

    #[test]
    fn validate_wallet_accepts_yjay_prefix() {
        assert!(validate_wallet("yjay1qpzryashv7v77p6dmkmsswhcqtl3ftxsd2y2").is_ok());
    }

    #[test]
    fn validate_wallet_rejects_other_prefixes() {
        assert!(validate_wallet("cosmos1abc").is_err());
        assert!(validate_wallet("yjay").is_err()); // too short
        assert!(validate_wallet("").is_err());
    }

    #[test]
    fn redact_token_hides_value_but_keeps_url() {
        let u = Url::parse("wss://api-pool.winnode.xyz/?token=supersecret123").unwrap();
        let s = redact_token(&u);
        assert!(s.contains("token="), "token kv must remain: {s}");
        assert!(!s.contains("supersecret123"), "secret leaked: {s}");
        assert!(s.contains("redacted"), "redacted marker missing: {s}");
    }

    #[test]
    fn truncate_body_collapses_whitespace_and_caps_length() {
        let html = "<html>\n   <body>\n      <p>hello\nworld</p>\n   </body>\n</html>";
        let t = truncate_body(html, 200);
        assert!(!t.contains('\n'), "must be single-line: {t}");
        assert!(!t.contains("  "), "double spaces should collapse: {t}");
    }

    #[test]
    fn truncate_body_marks_when_truncated() {
        let body = "a".repeat(5000);
        let t = truncate_body(&body, 100);
        assert!(t.contains("truncated"), "should mark truncation: {t}");
        assert!(t.contains("5000 bytes"), "should report full size: {t}");
    }

    #[test]
    fn vercel_checkpoint_detection() {
        assert!(is_vercel_checkpoint(
            "blah <p>Vercel Security Checkpoint</p> blah"
        ));
        assert!(is_vercel_checkpoint("Enable JavaScript to continue"));
        assert!(!is_vercel_checkpoint("{\"token\":\"abc\"}"));
    }

    #[test]
    fn short_hash_format() {
        let h = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let s = short_hash(h);
        assert_eq!(s, "01234567…89abcdef");
        assert_eq!(short_hash("abcd"), "abcd");
    }

    #[test]
    fn format_uptime_buckets() {
        assert_eq!(format_uptime(Duration::from_secs(5)), "5s");
        assert_eq!(format_uptime(Duration::from_secs(65)), "1m05s");
        assert_eq!(format_uptime(Duration::from_secs(3725)), "1h02m05s");
    }
}
