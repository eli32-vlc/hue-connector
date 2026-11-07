# hue-connector

Lets a [Hue](https://github.com/eli32-vlc/Hue) bot work on the computer you install it on: files, shell, HTTP, and T3 Code threads.

It connects out to your Hue server and stays connected. Nothing listens on your machine.

## Install

Download the binary for your platform from [releases](https://github.com/eli32-vlc/hue-connector/releases), or build it:

```sh
go build ./cmd/hue-connector
```

## Pair

In Hue's dashboard, open **Connector**, press **Pair a computer**, and copy the command it shows:

```sh
hue-connector pair https://your-hue-server 123-456
hue-connector run
```

`pair` stores a token in `~/.hue-local/config.json` (mode 0600). `run` keeps the link open and reconnects with backoff if it drops.

## What it can do

The server asks; this computer does. Operations: `exec`, `read`, `write`, `export`, `fetch`, and `t3` (T3 Code sign-in and tool calls).

**It runs commands a language model decided on.** That is the whole point and the whole risk. Only pair a server you run. Remove the pairing from the dashboard to revoke it; the connector then stops instead of retrying.

## T3 Code

`hue-connector` holds the T3 Code sign-in on this machine. The token is written to `~/.hue-local/t3.json` (0600) and never leaves the machine — only results cross the socket. Revoke it from the dashboard at any time.

## Run it on login

macOS, `~/Library/LaunchAgents/com.hue.local.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.hue.local</string>
  <key>ProgramArguments</key>
  <array><string>/usr/local/bin/hue-connector</string><string>run</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/tmp/hue-connector.log</string>
  <key>StandardErrorPath</key><string>/tmp/hue-connector.log</string>
</dict>
</plist>
```

```sh
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.hue.local.plist
```

Linux: any process supervisor works — `ExecStart=/usr/local/bin/hue-connector run` with `Restart=always`.

## License

MIT
