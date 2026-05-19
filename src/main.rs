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
use std::time::Duration;

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
use url::Url;

const DEFAULT_TOKEN_URL: &str = "https://mining.thejaynetwork.com/api/ws-token";
const DEFAULT_POOL_WS: &str = "wss://api-pool.winnode.xyz";
const PING_INTERVAL: Duration = Duration::from_secs(30);
const SHARE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const SHARE_DIFFICULTY: u64 = 1_000_000;

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

    /// Path to persist the device ID across runs (mirrors the frontend's `localStorage`).
    ///
    /// Defaults to `./device_id` in the working directory. Delete the file to rotate
    /// the device identity.
    #[arg(long, default_value = "device_id", env = "JAY_DEVICE_ID_FILE")]
    device_id_file: PathBuf,

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

#[derive(Deserialize)]
struct TokenResponse {
    token: String,
}

async fn fetch_ws_token(client: &reqwest::Client, token_url: &str) -> Result<String> {
    let resp = client
        .post(token_url)
        .header("content-type", "application/json")
        .header("origin", "https://mining.thejaynetwork.com")
        .header("referer", "https://mining.thejaynetwork.com/")
        .body("{}")
        .send()
        .await
        .with_context(|| format!("POST {token_url}"))?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("ws-token request failed: HTTP {status}: {body}"));
    }
    let parsed: TokenResponse = resp
        .json()
        .await
        .context("decoding ws-token response as {\"token\": \"...\"}")?;
    if parsed.token.is_empty() {
        return Err(anyhow!("ws-token response contained empty token"));
    }
    Ok(parsed.token)
}

#[derive(Debug, Serialize)]
struct ClientMessage<'a> {
    #[serde(rename = "type")]
    msg_type: &'a str,
    payload: Value,
}

async fn run_session(cli: &Cli, ids: &MinerIds) -> Result<()> {
    let http = reqwest::Client::builder()
        .user_agent(concat!(
            "jay-miner/",
            env!("CARGO_PKG_VERSION"),
            " (+https://github.com/Devin1-tri/jay-miner-rs)"
        ))
        .timeout(Duration::from_secs(15))
        .build()
        .context("building HTTP client")?;

    info!("Requesting WebSocket token from {}", cli.token_url);
    let token = fetch_ws_token(&http, &cli.token_url).await?;
    debug!(token_len = token.len(), "got ws-token");

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

    // status: online
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

    // start_mining
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
        wallet = %cli.wallet,
        threads = cli.threads,
        intensity = cli.intensity,
        "mining session started"
    );

    // Channel for outbound messages so the ping & share tasks can both feed the sink.
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(64);

    let ping_tx = out_tx.clone();
    let ids_for_ping = ids.clone();
    let ping_task = tokio::spawn(async move {
        let mut tick = interval(PING_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tick.tick().await; // first tick fires immediately; skip so the first real ping is at +30s
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

    let share_tx = out_tx.clone();
    let ids_for_share = ids.clone();
    let wallet_for_share = cli.wallet.clone();
    let intensity = cli.intensity.clamp(0.0, 1.0);
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
            let payload = json!({
                "nonce": nonce,
                "hash": random_hex_64(),
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
            if share_tx.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Writer task: pulls from `out_rx` and writes to the sink.
    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if let Err(e) = ws_tx.send(msg).await {
                warn!(err = %e, "WebSocket send failed; closing writer");
                break;
            }
        }
        let _ = ws_tx.close().await;
    });

    // Shutdown signal (Ctrl+C or SIGTERM).
    let shutdown = tokio::spawn(async {
        wait_for_shutdown().await;
    });

    let mut shares_accepted: u64 = 0;
    let mut shares_rejected: u64 = 0;
    let mut total_reward: f64 = 0.0;

    let read_loop = async {
        while let Some(frame) = ws_rx.next().await {
            match frame {
                Ok(Message::Text(text)) => {
                    handle_server_message(
                        &text,
                        ids,
                        &mut shares_accepted,
                        &mut shares_rejected,
                        &mut total_reward,
                    )
                    .await;
                }
                Ok(Message::Binary(_)) => debug!("ignoring binary frame"),
                Ok(Message::Ping(p)) => {
                    let _ = out_tx.send(Message::Pong(p)).await;
                }
                Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                Ok(Message::Close(c)) => {
                    info!(close = ?c, "pool closed the connection");
                    break;
                }
                Err(e) => {
                    warn!(err = %e, "WebSocket read error");
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
            // Allow writer a moment to flush, then close.
            sleep(Duration::from_millis(250)).await;
        }
    }

    // Drop the outbound channel sender so writer finishes naturally.
    drop(out_tx);
    ping_task.abort();
    share_task.abort();
    let _ = writer.await;

    info!(
        shares_accepted,
        shares_rejected,
        total_reward = format!("{:.6} JAY", total_reward),
        "session finished"
    );
    Ok(())
}

async fn send_json<S>(sink: &mut S, msg: &ClientMessage<'_>) -> Result<()>
where
    S: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let text = serde_json::to_string(msg).context("serializing client message")?;
    debug!(msg = %text, "ws -> pool");
    sink.send(Message::Text(text))
        .await
        .context("WebSocket send")
}

async fn handle_server_message(
    text: &str,
    ids: &MinerIds,
    shares_accepted: &mut u64,
    shares_rejected: &mut u64,
    total_reward: &mut f64,
) {
    let parsed: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            warn!(err = %e, raw = %text, "non-JSON server message");
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
                let mut slot = ids.miner_id.lock().await;
                *slot = Some(miner_id.to_string());
                info!(miner_id, "{}", msg_type);
            } else {
                info!(payload = %payload, "{}", msg_type);
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
                debug!(job_id, target, "new job from pool");
            }
        }
        "share_accepted" => {
            *shares_accepted += 1;
            debug!(total = *shares_accepted, "share accepted");
        }
        "share_rejected" => {
            *shares_rejected += 1;
            warn!(payload = %payload, total = *shares_rejected, "share rejected");
        }
        "mining_reward" => {
            let amount = payload.get("amount").and_then(Value::as_f64).unwrap_or(0.0);
            let shares = payload.get("shares").and_then(Value::as_u64).unwrap_or(0);
            let tx_hash = payload.get("txHash").and_then(Value::as_str).unwrap_or("");
            *total_reward += amount;
            info!(
                amount,
                shares,
                tx = tx_hash,
                running_total = format!("{:.6} JAY", *total_reward),
                "mining_reward"
            );
        }
        "block_found" => info!(payload = %payload, "block_found"),
        "pool_stats" => debug!(payload = %payload, "pool_stats"),
        "pong" => debug!("pong"),
        other => debug!(other, payload = %payload, "server message"),
    }
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                warn!(err = %e, "failed to install SIGTERM handler; only Ctrl+C will shut down");
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
        session_id = %ids.session_id,
        device_id = %ids.device_id,
        "miner identifiers ready"
    );

    let mut attempt: u32 = 0;
    loop {
        match run_session(&cli, &ids).await {
            Ok(()) => {
                info!("session ended cleanly");
                break Ok(());
            }
            Err(e) => {
                attempt += 1;
                warn!(err = %e, attempt, "session error");
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
                info!(
                    backoff_secs = backoff.as_secs(),
                    attempt, "reconnecting after backoff"
                );
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
}
