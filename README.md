# loraterm

LoRa-mesh terminal emulator daemon. Bridges DMs received via a MeshCore
companion identity (running on a server) to a per-peer persistent bash PTY.
Whitelisted peer pubkeys are the only thing allowed to drive a shell session.

## Architecture

- A **dedicated companion identity** (e.g. `shell@cassius`) is created
  out-of-band via the bridge Web-UI; loraterm consumes a per-identity
  **bearer token** (`read,write` scope).
- The daemon opens an **SSE stream** to the bridge and watches for inbound
  DM events.
- Each whitelisted peer pubkey gets its own **bash PTY** with a private
  `$HOME` (`/var/lib/loraterm/peers/<short_pubkey>/`).
- PTY output is **ANSI-stripped**, chunked to ≤120 chars per DM (with
  `[i/N] ` prefix), rate-limited (per-peer + global) and sent back via
  `POST /api/v1/companion/messages/dm`.
- SIGHUP **hot-reloads** the whitelist. Removed peers get a polite
  `session closed: revoked` DM and their PTY is killed.

## Trust model

The whitelist is the **only** trust boundary. Any whitelisted peer can run
arbitrary commands as the daemon user. Run the daemon under an
unprivileged dedicated UID (`loraterm`), keep `/etc/loraterm` mode-0750
group-readable by `loraterm`, and put the bearer token in
`/etc/loraterm/token` (mode 0600) — handed in via systemd `LoadCredential=`.

The token is locked to a single companion identity and a single scope set
(`read,write` is sufficient for normal operation; `admin` would only be
needed if you wanted the daemon to issue adverts itself).

## Deployment

1. Build:

   ```sh
   cargo build --release
   sudo install -Dm0755 target/release/loraterm /usr/local/bin/loraterm
   ```

2. Create user + dirs:

   ```sh
   sudo useradd --system --home-dir /var/lib/loraterm --shell /usr/sbin/nologin loraterm
   sudo install -d -o root -g loraterm -m 0750 /etc/loraterm
   sudo install -d -o loraterm -g loraterm -m 0750 /var/lib/loraterm
   sudo install -d -o loraterm -g loraterm -m 0700 /var/lib/loraterm/state
   sudo install -d -o loraterm -g loraterm -m 0750 /var/lib/loraterm/peers
   ```

3. Companion identity + token (one-time, via the bridge Web-UI **or** curl):

   ```sh
   # Cookie-login (Web-UI flow); then create the identity + token.
   curl -c /tmp/c -d 'email=you@example.com&password=…' https://meshcore.dumke.me/login
   curl -b /tmp/c -d 'name=shell@cassius&scope=public' \
        https://meshcore.dumke.me/api/v1/companion/identities
   # → {"id": "<IDENT_UUID>", ...}
   curl -b /tmp/c -d 'name=loraterm&scopes=read,write' \
        https://meshcore.dumke.me/api/v1/companion/identities/<IDENT_UUID>/tokens
   # → {"token": "ABCDEFGH…32CHARS"}  (shown once — copy now)
   shred -u /tmp/c
   ```

4. Configure:

   ```sh
   sudo install -m0640 -o root -g loraterm \
     config/loraterm.toml.example /etc/loraterm/loraterm.toml
   sudo install -m0640 -o root -g loraterm \
     config/whitelist.toml.example /etc/loraterm/whitelist.toml
   sudo $EDITOR /etc/loraterm/whitelist.toml          # add real peer pubkeys

   # Token (raw, single line):
   echo -n 'ABCDEFGH…32CHARS' | sudo install -m0600 -o root -g root /dev/stdin /etc/loraterm/token
   ```

5. Install + start the service:

   ```sh
   sudo install -m0644 systemd/loraterm.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now loraterm.service
   journalctl -u loraterm.service -f
   ```

6. Reload whitelist after edits:

   ```sh
   sudo systemctl kill -s HUP loraterm.service
   ```

## SSE schema discovery

The exact JSON shape inside SSE frames isn't fully nailed down upstream. To capture
real events for schema verification, run with `--sse-raw-log`:

```sh
sudo -u loraterm loraterm --config /etc/loraterm/loraterm.toml \
  --sse-raw-log /tmp/sse.raw.log
```

Then DM something to your `shell@cassius` identity from your handheld
LoRa node. Inspect `/tmp/sse.raw.log`:

```
event=dm_received id=... data={...}
```

`src/api/sse.rs::parse_event` dispatches on the `event:` field name; if your
bridge emits something not yet mapped (e.g. `dm.recv`), drop a line for it
there or rely on the structural fallback (any object with `peer_pubkey_hex`
+ `text` + `direction == "in"` is treated as inbound DM).

## Operator workflow

From a whitelisted LoRa node, DM the `shell@cassius` identity:

```
pwd
```

Expect a return DM within ~5–10 s (LoRa RTT). Subsequent commands keep the
same bash session — cwd, env, history all persist for the per-peer
session until 30 min of idle. Closing the session is a normal `exit`.

LoRa packet payloads are small (~140 B); commands that produce a lot of
output will be **chunked + rate-limited** (1 DM / 4 s, burst 2). Very long
output (`find /`) is **truncated** at 50 chunks with a
`[output truncated …]` marker.

## Tests

```sh
cargo test               # offline unit + wiremock integration tests
```

## License

MIT.
