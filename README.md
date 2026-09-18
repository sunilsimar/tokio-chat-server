# Tokio Chat

A real-time WebSocket chat server in Rust (Tokio + tungstenite) with a React frontend. Clients can pick a name, join rooms, send DMs, share images, and see who is online.

The server listens on `0.0.0.0:8080`. The UI (Vite + React) hardcodes `ws://127.0.0.1:8080`, so it only reaches a server on the same machine. State is in-memory: no TLS, no auth, no persistence. A restart wipes names, rooms, and history.

The Cargo package/binary is named `chat-compressor`.

## Features

- Concurrent connections via Tokio tasks (one task per client)
- Named users (`/name`) and presence (`/who`, `/count`)
- Rooms (`/join`) with the last 50 of up to 100 stored messages replayed on join
- Direct messages (`/dm`)
- Lobby and room **text** broadcast
- Image messages as JSON (`{ "type": "image", "data": "<data-url>" }`) — sent to **every** connected client, not scoped to the sender’s room. Any valid JSON text takes this path, not only `type: "image"`.

## Run locally

**Server (release):**

```bash
cargo run --release
```

**Frontend:**

```bash
cd frontend
npm install
npm run dev
```

Open the UI at [http://localhost:3000](http://localhost:3000), set a name, and connect.

**Docker (server only — the image does not include the frontend):**

```bash
docker build -t tokio-chat .
docker run --rm -p 8080:8080 tokio-chat
```

Then run the UI locally with `npm run dev` so it can still open `ws://127.0.0.1:8080`.

## Chat commands

| Command | Effect |
| --- | --- |
| `/name <name>` | Set display name and **move to lobby** (renaming leaves the current room) |
| `/who` | List other connected users |
| `/count` | Number of connected clients |
| `/ping` | Reply `pong` |
| `/join <room>` | Switch rooms (up to 50 history lines replayed) |
| `/get_rooms` | Dump each client’s room (duplicates; departed users are not removed) |
| `/dm <user> <text>` | Private message |
| anything else (non-JSON) | Broadcast to the current room |

## Load tests

Headline results from a **release** binary on **localhost**, 15 Aug 2026, Apple M4 Pro (12 cores, 24 GB), client and server on the same Mac, small text frames only:

- **15,000** concurrent WebSocket connections, all answered `pong`
- **~183k ping/s** with 1,000 clients, 0 errors
- **100%** lobby broadcast delivery at 500 clients (~1M fan-out deliveries/s, p50 43 ms)
- **122 MB RSS** at 10,000 connections (~12 KB each)

Images were **not** load-tested. The image path also `println!`s on every send, so it would not match these text-frame numbers.

| Setup | Value |
| --- | --- |
| Target | `ws://127.0.0.1:8080` |
| Payload | Small text frames (not images, not TLS, not WAN) |
| Harness | `load_tester/` (Tokio WebSocket clients) |

Functional checks (3 live clients) all passed: handshake, `/ping`, `/name`, `/who`, `/count`, lobby broadcast, `/dm`, room isolation, same-room chat, `/get_rooms`.

### Concurrent connections

Each client completed a WebSocket handshake and a `/ping` → `pong` round trip. Runs were isolated (fresh server, drained `TIME_WAIT`) so macOS ephemeral ports were not exhausted.

| Target | Connected | Ping ok | Connect time | RSS |
| --- | ---: | ---: | ---: | ---: |
| 100 | 100 | 100 | 5 ms | 3.6 MB |
| 500 | 500 | 500 | 75 ms | 8.6 MB |
| 1,000 | 1,000 | 1,000 | 149 ms | 14.5 MB |
| 2,500 | 2,500 | 2,500 | 437 ms | 32.5 MB |
| 5,000 | 5,000 | 5,000 | 770 ms | 62.1 MB |
| 8,000 | 8,000 | 8,000 | 1.23 s | 98.0 MB |
| 10,000 | 10,000 | 10,000 | 1.57 s | 121.8 MB |
| 12,000 | 12,000 | 12,000 | 1.92 s | not sampled |
| 15,000 | 15,000 | 15,000 | 2.52 s | not sampled |

**15,000** is near the default localhost ephemeral port range (~16,383), so that is a client-OS ceiling, not a proven server maximum.

### Ping throughput

Concurrent clients looping `/ping` for 8 seconds. This path does not fan out to other users. **Zero errors** at every size. Server CPU from `ps` peaked around **280–310%** (~3 cores).

| Clients | Pong/s | p50 | p99 | Errors |
| ---: | ---: | ---: | ---: | ---: |
| 50 | 164k | 0.17 ms | 0.73 ms | 0 |
| 200 | 173k | 0.09 ms | 3.14 ms | 0 |
| 500 | 175k | 0.13 ms | 7.77 ms | 0 |
| 1,000 | **183k** | 0.13 ms | 13.0 ms | 0 |

p50 is sampled from early RTTs and is optimistic at high concurrency. Quote **pong/s** and **p99**.

### Lobby broadcast

N clients in the same room each send a few unique text messages. Expected deliveries = `N × messages × (N − 1)`. Throughput is fan-out completions (one send to one peer), not unique chat lines.

| Clients | Sends each | Expected | Received | Delivery | p50 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 25 | 20 | 12,000 | 12,000 | 100% | 5.6 ms | 10.6 ms |
| 50 | 10 | 24,500 | 24,500 | 100% | 12.4 ms | 22.9 ms |
| 100 | 10 | 99,000 | 99,000 | 100% | 37 ms | 74 ms |
| 250 | 5 | 311,250 | 311,250 | 100% | 42 ms | 81 ms |
| 500 | 3 | 748,500 | 748,500 | **100%** | 43 ms | 83 ms |

At 500 clients: **~1M deliveries/s**, ~131% CPU, 8.9 MB RSS.

### Private rooms

50 isolated pairs (100 clients, 50 rooms) delivered 1,000 peer messages at **100%**, p50 **5.2 ms**, p99 **8.2 ms**.

### How to re-run

Run suites separately. `load_tester all` chains ramps on one process and will hit macOS port exhaustion.

```bash
# terminal 1
cargo run --release

# terminal 2
cd load_tester
cargo build --release
PID=$(pgrep -n chat-compressor)
SERVER_PID=$PID ./target/release/load_tester functional
SERVER_PID=$PID ./target/release/load_tester connections 10000
SERVER_PID=$PID ./target/release/load_tester ping
SERVER_PID=$PID ./target/release/load_tester broadcast
SERVER_PID=$PID ./target/release/load_tester rooms
```

Drain TCP `TIME_WAIT` between large connection ramps (or wait ~30s after killing the server). A chained 10k run after earlier ramps failed from **port exhaustion**, not from the server dropping sockets.

## Notes

These numbers measure the Tokio event loop and in-memory fan-out on one laptop. They are **not** internet CCU, TLS RPS, or cross-region latency.

The client list is a single mutex around a `Vec` of sockets. A lobby message holds that lock and sends to every other socket sequentially. Delivery stayed at 100%; p50 rose from ~6 ms (25 people) to ~43 ms (500). A sharded map or per-room broadcast channel would be the next performance step. On disconnect, the connection is removed but the name/room maps are not, which is why `/get_rooms` can list stale rooms.
