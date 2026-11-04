# hue-connector

The local connector CLI for Hue: pairs your Mac or Linux machine with a Hue
server over WebSocket and runs the operations the agent asks for on that
machine (shell, file read/write, HTTP fetch). Your dashboard shows a live
feed of what the agent did on your machine — last 7 days, per machine.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/eli32-vlc/hue-connector/main/install.sh | sh
```

That's it. The script picks the right binary for your OS/arch, verifies its
SHA256 checksum against the release, and installs it to `~/.local/bin`
(no sudo).

## Pair

1. In the Hue dashboard, open **Connector** → **Pair a computer**. You get a
   one-time code (good for 10 minutes) and a pair command.
2. On the machine you want connected:

```sh
hue-connector pair <server-url> <code>
hue-connector run
```

`run` stays connected and executes operations as they arrive. Use
`hue-connector status` to see your pairing, `hue-connector version` for the
build version.

## Autostart

macOS (launchd):

```sh
cat > ~/Library/LaunchAgents/com.hue.connector.plist <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.hue.connector</string>
  <key>ProgramArguments</key>
  <array><string>%s/.local/bin/hue-connector</string><string>run</string></array>
  <key>RunAtLoad</key><true/>
</dict></plist>
PLIST
launchctl load ~/Library/LaunchAgents/com.hue.connector.plist
```

Linux (systemd --user):

```sh
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/hue-connector.service <<'UNIT'
[Unit]
Description=hue-connector
After=network-online.target

[Service]
ExecStart=%h/.local/bin/hue-connector run
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
UNIT
systemctl --user daemon-reload
systemctl --user enable --now hue-connector
```

## Security notes

- Pairing uses a one-time code; the resulting token is stored with 0600
  permissions at `~/.hue-local/config.json` and only ever sent to your
  configured server.
- The connector executes what the server sends. Never pair with a server you
  don't trust, and remove the pairing from the dashboard when you're done.

## Build from source

```sh
go build -o hue-connector ./cmd/hue-connector
```

## License

MIT. Copyright (c) 2026 Zenith Rifle <bridge@fmhy.bid>.
