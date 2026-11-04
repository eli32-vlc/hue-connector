# hue-connector

Pairs your Mac or Linux machine with a Hue server and runs the
operations the agent asks for on it (shell, files, HTTP). The
dashboard shows what the agent did on your machine — last 7 days.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/eli32-vlc/hue-connector/main/install.sh | sh
```

Verifies the release checksum, installs to `~/.local/bin` (no sudo).

## Use

```sh
hue-connector pair <server-url> <code>   # code from the dashboard
hue-connector run                        # stay connected
hue-connector status                     # show pairing
```

## Autostart

macOS:

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

Linux:

```sh
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/hue-connector.service <<'UNIT'
[Unit]
Description=hue-connector

[Service]
ExecStart=%h/.local/bin/hue-connector run
Restart=on-failure

[Install]
WantedBy=default.target
UNIT
systemctl --user daemon-reload && systemctl --user enable --now hue-connector
```

## License

MIT. © 2026 Zenith Rifle <bridge@fmhy.bid>
