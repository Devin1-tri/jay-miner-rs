# jay-miner

Headless Rust client for the **Jay Network browser-mining pool** at
[mining.thejaynetwork.com](https://mining.thejaynetwork.com).

Mirrors the WebSocket protocol the official browser frontend speaks, so you can
"mine" from a VPS / headless box without keeping a browser tab open.

---

## Disclaimer (read first)

The Jay Network is a Cosmos SDK + CometBFT chain. It uses **Byzantine
Fault-Tolerant Proof-of-Stake** — there are no PoW puzzles to solve, and there
is **no real computational work happening in the browser frontend either**.

Specifically, the frontend's `submit_share` message generates its `hash` field
like this (lifted verbatim from the production Next.js bundle):

```js
hash: Array.from({length:64}, () => Math.floor(16*Math.random()).toString(16)).join("")
```

That is a uniformly-random 64-character hex string — **not a hash of anything**.
The pool grants rewards proportional to time-spent + accepted shares, gated by
session/device/IP fingerprinting. This is best understood as a **faucet /
airdrop dressed up in PoW vocabulary**, not a competitive mining market.

`jay-miner` reproduces this protocol faithfully. It does **not**:

- Spawn CPU/GPU workers.
- Compute SHA-256 or any other hash.
- Sign anything with your wallet (the protocol does not require it).
- Use Keplr or any other browser extension.

If you're looking for an actual PoW miner, this is the wrong project.

## Risks before you run

- **Pool ToS**: The pool is positioned as "browser mining for the community".
  Running a headless client is a gray area. The operator can blacklist your
  wallet, device fingerprint, or IP at any time, and is likely to introduce
  captchas / wallet-signature challenges if they detect non-browser clients.
- **Sybil farming is not supported here.** This binary is designed for
  **single-wallet, single-device** operation — one running instance per
  machine, one wallet, one device-id file. If you want to farm across
  many wallets you are on your own; do not open issues asking for help with
  that.
- **Token value**: JAY is not listed on any major exchange and supply is
  controlled by the operator. ROI may be zero or negative once you account
  for electricity and bandwidth.

## What's actually in the protocol

Captured from `https://mining.thejaynetwork.com/_next/static/chunks/app/page-*.js`:

| Step | Direction | Message |
|------|-----------|---------|
| 1 | `POST https://mining.thejaynetwork.com/api/ws-token` → `{"token": "..."}` |
| 2 | Open `wss://api-pool.winnode.xyz?token=<token>` |
| 3 | `→` `{"type":"status","payload":{"sessionId","deviceId","status":"online","wallet"}}` |
| 4 | `→` `{"type":"start_mining","payload":{"wallet","threads","sessionId","deviceId","minerId?"}}` |
| 5 | `→` every 30 s: `{"type":"ping","payload":{"sessionId","deviceId","status":"online"}}` |
| 6 | `→` every 1 s (× intensity): `{"type":"submit_share","payload":{"nonce","hash","jobId","difficulty":1000000,"sessionId","deviceId","minerId"}}` |
| 7 | `→` on shutdown: `stop_mining` + `status:offline`, then close. |
| ← | Server | `auth_success`, `mining_started` (carry `minerId`), `job` (`jobId`,`target`), `share_accepted`, `share_rejected`, `mining_reward` (`amount`,`shares`,`txHash`), `block_found`, `pool_stats`, `pong`. |

Reconnect: exponential backoff `min(1s · 2ⁿ, 30 s)`, like the frontend.

## Build

Requires Rust ≥ 1.86 (latest stable). A `rust-toolchain.toml` pins this.

```bash
git clone https://github.com/Devin1-tri/jay-miner-rs.git
cd jay-miner-rs
cargo build --release
```

Binary lands at `target/release/jay-miner` (~5 MB, statically-linked rustls).

## Run

```bash
./target/release/jay-miner \
  --wallet yjay1qpzryashv7v77p6dmkmsswhcqtl3ftxsd2y2 \
  --threads 4 \
  --intensity 1.0
```

All flags also accept env vars (e.g. `JAY_WALLET`, `JAY_THREADS`). Use
`--verbose` (or `RUST_LOG=debug`) to see the raw frames.

### Sample log output

Default `info`-level logs are compact, single-line, and include a periodic
`[stats]` panel (every 10 s) plus a real-time reward line on every
`mining_reward` event:

```
2026-05-19T06:30:00.000Z  INFO miner identifiers ready — sessionId=session_… deviceId=device_…
2026-05-19T06:30:00.123Z  INFO Requesting WebSocket token from https://mining.thejaynetwork.com/api/ws-token
2026-05-19T06:30:00.456Z  INFO Connecting to pool: wss://api-pool.winnode.xyz/?token=<256-char-redacted>
2026-05-19T06:30:00.789Z  INFO mining session started — wallet=yjay1… threads=4 intensity=1.00
2026-05-19T06:30:01.001Z  INFO auth_success (minerId=miner_abc123)
2026-05-19T06:30:42.345Z  INFO reward +0.001234 JAY (shares 42, tx 1a2b3c4d…ef567890) — running total 0.001234 JAY
2026-05-19T06:30:10.000Z  INFO [stats] up 10s | shares 9/10 ok (0 rej) | rewards 0 (0.000000 JAY, last 0.000000) | balance 0.000000 JAY | last_hash 0123abcd…ef456789 nonce=874213
```

If the token endpoint is throttled by Vercel's anti-bot challenge you'll get a
clean one-line error (no HTML/SVG dumped into the log):

```
2026-05-19T06:33:01.205Z  WARN session error (attempt 3): ws-token blocked by Vercel security checkpoint (HTTP 200). Try again from a different IP, or wait a few minutes for the rate-limit to expire.
2026-05-19T06:33:01.206Z  INFO reconnecting in 8s (attempt 3)
```

The miner writes `./device_id` on first run and reuses it on subsequent runs
(mirroring the frontend's `localStorage`). Delete the file to rotate the
device identity.

Press `Ctrl+C` (or send `SIGTERM`) to gracefully send `stop_mining` +
`status:offline` before closing the socket.

## Run as a systemd service

```ini
# /etc/systemd/system/jay-miner.service
[Unit]
Description=Jay Network headless miner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=jayminer
WorkingDirectory=/var/lib/jay-miner
ExecStart=/usr/local/bin/jay-miner \
  --wallet yjay1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx \
  --threads 4 \
  --intensity 0.8 \
  --device-id-file /var/lib/jay-miner/device_id
Restart=on-failure
RestartSec=10
# No real CPU work, but cap anyway as a tripwire.
CPUQuota=20%
MemoryMax=128M

[Install]
WantedBy=multi-user.target
```

```bash
sudo useradd --system --home /var/lib/jay-miner --shell /usr/sbin/nologin jayminer
sudo install -m 755 target/release/jay-miner /usr/local/bin/
sudo install -d -o jayminer -g jayminer /var/lib/jay-miner
sudo systemctl daemon-reload
sudo systemctl enable --now jay-miner
journalctl -u jay-miner -f
```

## CLI reference

```
jay-miner --help
```

| Flag | Env | Default | Notes |
|------|-----|---------|-------|
| `--wallet` | `JAY_WALLET` | *(required)* | Must start with `yjay`. |
| `--threads` | `JAY_THREADS` | `4` | Reported to pool; not used locally. |
| `--intensity` | `JAY_INTENSITY` | `1.0` | Share-emit probability per second, 0.0–1.0. |
| `--token-url` | `JAY_TOKEN_URL` | `https://mining.thejaynetwork.com/api/ws-token` | |
| `--pool-ws` | `JAY_POOL_WS` | `wss://api-pool.winnode.xyz` | |
| `--rest-url` | `JAY_REST_URL` | `https://api-jayn.winnode.xyz` | Cosmos REST API used to read on-chain JAY balance. |
| `--device-id-file` | `JAY_DEVICE_ID_FILE` | `./device_id` | |
| `--stats-interval` | `JAY_STATS_INTERVAL` | `10` (seconds) | How often to print the `[stats]` panel. `0` disables. |
| `--balance-interval` | `JAY_BALANCE_INTERVAL` | `60` (seconds) | How often to refresh the on-chain JAY balance. `0` disables. |
| `--max-reconnects` | `JAY_MAX_RECONNECTS` | `0` (forever) | Mirrors frontend's 5-attempt cap when set. |
| `--verbose` | — | off | Sets `RUST_LOG=debug` if not already set. |

## Tests & lint

```bash
cargo +stable test --release
cargo +stable clippy --all-targets -- -D warnings
cargo +stable fmt --check
```

## License

MIT — see [LICENSE](LICENSE).
