# loraterm

LoRa-mesh terminal emulator daemon. Bridges DMs received via a MeshCore
companion identity (running on a server) to a per-peer persistent bash PTY.
Whitelisted peer pubkeys are the only thing allowed to drive a shell session.

## Architecture

- One **dedicated companion identity** (default `shell@cassius`, scope `public`)
  is created on first run via the bridge REST API.
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
group-readable by `loraterm`, and put credentials in
`/etc/loraterm/credentials` (mode 0600) — handed in via systemd
`LoadCredential=`.

## Deployment (cassius)

1. Build for the target:

   ```sh
   cargo build --release
   install -Dm0755 target/release/loraterm /usr/local/bin/loraterm
   ```

2. Create user + dirs:

   ```sh
   useradd --system --home-dir /var/lib/loraterm --shell /usr/sbin/nologin loraterm
   install -d -o root -g loraterm -m 0750 /etc/loraterm
   install -d -o loraterm -g loraterm -m 0750 /var/lib/loraterm
   install -d -o loraterm -g loraterm -m 0700 /var/lib/loraterm/state
   install -d -o loraterm -g loraterm -m 0750 /var/lib/loraterm/peers
   ```

3. Configure:

   ```sh
   install -m0640 -o root -g loraterm \
     config/loraterm.toml.example /etc/loraterm/loraterm.toml
   install -m0640 -o root -g loraterm \
     config/whitelist.toml.example /etc/loraterm/whitelist.toml
   $EDITOR /etc/loraterm/whitelist.toml          # add real peer pubkeys

   # Credentials: two lines, email then password.
   umask 077 && cat > /etc/loraterm/credentials <<EOF
   you@example.com
   yourbridgepassword
   EOF
   chmod 0600 /etc/loraterm/credentials
   chown root:root /etc/loraterm/credentials     # systemd reads as root, copies into runtime
   ```

4. Install + start the service:

   ```sh
   install -m0644 systemd/loraterm.service /etc/systemd/system/
   systemctl daemon-reload
   systemctl enable --now loraterm.service
   journalctl -u loraterm.service -f
   ```

5. Reload whitelist after edits:

   ```sh
   systemctl kill -s HUP loraterm.service
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
cargo clippy --all-targets -- -D warnings
```

## License

MIT.
