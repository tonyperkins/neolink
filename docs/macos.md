# Running neolink natively on macOS

These steps build neolink from source on a Mac and run it as a background service, without Docker. They target Apple Silicon (M1 and later); Intel Macs work the same way. CI builds and runs the unit tests on macOS 15 (arm64). This guide has not yet been checked end to end on a real Mac with a camera, so please report anything that differs.

## 1. Install the build tools and GStreamer

You need [Homebrew](https://brew.sh). Then:

```sh
brew install pkg-config protobuf openssl@3 gstreamer
# Rust, via the official installer (installs into ~/.cargo)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env
```

Homebrew's `gstreamer` formula includes the RTSP server and the plugins neolink uses. To check that they are there:

```sh
gst-inspect-1.0 rtph264pay >/dev/null && gst-inspect-1.0 appsrc >/dev/null && echo ok
```

## 2. Build

```sh
git clone https://github.com/tonyperkins/neolink.git
cd neolink
export OPENSSL_DIR="$(brew --prefix openssl@3)"
cargo build --release
sudo mkdir -p /usr/local/bin
sudo install -m 755 target/release/neolink /usr/local/bin/neolink
neolink --version
```

To update later, run `git pull` and repeat the `cargo build` and `install` lines.

## 3. Configure

Create `~/neolink/neolink.toml` and make it private (it contains the camera password):

```toml
bind = "127.0.0.1"   # use "0.0.0.0" if Frigate runs on another machine
bind_port = 8554

[[cameras]]
name = "living_room"
username = "admin"
password = "{CAMERA_PASSWORD}"
address = "192.168.1.50:9000"    # the camera's IP; Baichuan port 9000
stream = "both"                  # serves /living_room/main and /living_room/sub
audio = "aac"                    # MP4-friendly audio; "none" for no audio
push_notifications = false
```

```sh
chmod 600 ~/neolink/neolink.toml
```

Before installing the service, run it once in the foreground to test it:

```sh
neolink rtsp --config ~/neolink/neolink.toml
# in another terminal:
ffprobe -rtsp_transport tcp rtsp://127.0.0.1:8554/living_room/main
```

## 4. Run it as a service (launchd)

Save the following as `~/Library/LaunchAgents/com.neolink.rtsp.plist`, and replace `YOURUSER`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.neolink.rtsp</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/neolink</string>
    <string>rtsp</string>
    <string>--config</string>
    <string>/Users/YOURUSER/neolink/neolink.toml</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>RUST_LOG</key><string>info</string>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>5</integer>
  <key>StandardOutPath</key><string>/Users/YOURUSER/neolink/neolink.log</string>
  <key>StandardErrorPath</key><string>/Users/YOURUSER/neolink/neolink.log</string>
</dict>
</plist>
```

```sh
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.neolink.rtsp.plist
launchctl print gui/$(id -u)/com.neolink.rtsp | grep -E "state|pid"
tail -f ~/neolink/neolink.log
```

To stop or reload it after changing the config:

```sh
launchctl bootout gui/$(id -u)/com.neolink.rtsp
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.neolink.rtsp.plist
```

A LaunchAgent runs while that user is logged in. On a Mac that should keep streaming after a reboot with nobody logged in, enable automatic login for that user, or install the plist as a LaunchDaemon in `/Library/LaunchDaemons` (owned by root) instead.

## 5. Point Frigate at it

Use the same go2rtc setup as on Linux:

```yaml
go2rtc:
  streams:
    living_room:
      - ffmpeg:rtsp://127.0.0.1:8554/living_room/main#video=copy#audio=aac
    living_room_sub:
      - ffmpeg:rtsp://127.0.0.1:8554/living_room/sub#video=copy
```

With `audio = "aac"` the audio can be copied as is, e.g. `ffmpeg:rtsp://127.0.0.1:8554/living_room/main#video=copy#audio=copy`. With the default PCM audio, keep `#audio=aac` so go2rtc converts it: Frigate stores recordings as MP4, which cannot hold PCM.

This also works with [Fregata](https://fregata.app), a native macOS port of Frigate, running on the same Mac. It reads a standard Frigate config, so `127.0.0.1` reaches neolink directly.

If Frigate runs in a container on the same Mac, use `host.docker.internal` instead of `127.0.0.1`, and set `bind = "0.0.0.0"` in `neolink.toml`.

## Troubleshooting

- **Stream freezes a few seconds after starting, or recordings fail**: usually the PCM audio. Set `audio = "aac"` (or `"none"`) in `neolink.toml`, or convert it with `#audio=aac` in go2rtc.
- **No PTZ controls in Frigate**: Frigate only controls PTZ over ONVIF, and neolink does not provide ONVIF. Cameras without ONVIF (e.g. the original E1) can be moved with `neolink ptz <camera> control 10 left` or over MQTT (`neolink/<camera>/control/ptz`).

- **"Local Network" permission.** Recent macOS versions ask before a program talks to devices on the LAN. If neolink logs that it cannot connect to the camera, check System Settings → Privacy & Security → Local Network. Running it once from Terminal (step 3) usually triggers the prompt.
- **Missing GStreamer elements** (e.g. `no element "rtph264pay"`): run `brew reinstall gstreamer`, then check with `gst-inspect-1.0` as in step 1.
- **Build cannot find OpenSSL**: make sure `OPENSSL_DIR` is exported, as in step 2.
