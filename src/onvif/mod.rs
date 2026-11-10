//! # ONVIF PTZ service
//!
//! Serves a small ONVIF device per camera so that ONVIF clients (Frigate in
//! particular) can pan, tilt and recall presets on cameras that have no ONVIF
//! of their own, such as the original E1. Commands are sent to the camera over
//! the Baichuan connection neolink already holds.
//!
//! Enable it per camera with a port:
//!
//! ```toml
//! [[cameras]]
//! name = "Garage"
//! onvif_port = 8001
//! ```
//!
//! Then point the client at `bind:8001`. Authentication uses the same
//! `[[users]]` and `permitted_users` as RTSP: without any users configured
//! anyone may connect.
//!
//! Supported: GetCapabilities, GetServices, GetDeviceInformation,
//! GetSystemDateAndTime, GetProfiles/GetProfile, GetVideoSources,
//! GetStreamUri, and the PTZ calls GetNodes/GetNode, GetConfigurations/
//! GetConfiguration, GetConfigurationOptions, GetServiceCapabilities,
//! GetPresets, GotoPreset, ContinuousMove, Stop and GetStatus.
//! Pan and tilt only: no zoom, absolute or relative moves (so no Frigate
//! autotracking), since the camera reports no position.
mod soap;

use anyhow::{anyhow, Context, Result};
use neolink_core::bc_protocol::{Direction as BcDirection, StreamKind};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch::Receiver as WatchReceiver,
    task::JoinHandle,
    time::{sleep, timeout, Duration},
};

use crate::{
    common::NeoInstance,
    config::{CameraConfig, Config},
};
use soap::*;

/// How long a whole request may take, from connect to reply
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a command may wait for the camera
const CAMERA_TIMEOUT: Duration = Duration::from_secs(5);
/// A ContinuousMove without a Timeout stops after this long (DefaultPTZTimeout)
const DEFAULT_MOVE_TIMEOUT: Duration = Duration::from_secs(10);
/// Retry delay after the service fails (e.g. port in use)
const RETRY_DELAY: Duration = Duration::from_secs(30);

const PTZ_CONFIG_TOKEN: &str = "ptz_config";
const PTZ_NODE_TOKEN: &str = "ptz_node";
const VIDEO_SOURCE_TOKEN: &str = "video_source";

/// Run the ONVIF service for one camera while `onvif_port` is set.
///
/// Never returns Ok: it waits for configuration changes and restarts the
/// service on them, and errors (e.g. the port is taken) are logged and retried.
pub(crate) async fn serve(camera: NeoInstance, config: WatchReceiver<Config>) -> Result<()> {
    let mut cam_config = camera.config().await?;
    loop {
        let (name, port) = {
            let c = cam_config.borrow_and_update();
            (c.name.clone(), c.onvif_port)
        };
        let Some(port) = port else {
            cam_config.changed().await?;
            continue;
        };
        let bind_addr = config.borrow().bind_addr.clone();
        let service = run(
            &camera,
            &name,
            &bind_addr,
            port,
            config.clone(),
            cam_config.clone(),
        );
        // Map the watch guard away inside the future so it is never held
        // across an await
        let port_changed = async {
            cam_config
                .wait_for(|c| c.onvif_port != Some(port))
                .await
                .map(|_| ())
        };
        let failed = tokio::select! {
            v = port_changed => {
                v?;
                log::info!("{name}: ONVIF port changed, restarting the ONVIF service");
                false
            }
            v = service => match v {
                Err(e) => {
                    log::warn!("{name}: ONVIF service stopped: {e:#}. Retrying in {RETRY_DELAY:?}");
                    true
                }
                Ok(()) => false,
            }
        };
        if failed {
            sleep(RETRY_DELAY).await;
        }
    }
}

async fn run(
    camera: &NeoInstance,
    name: &str,
    bind_addr: &str,
    port: u16,
    config: WatchReceiver<Config>,
    cam_config: WatchReceiver<CameraConfig>,
) -> Result<()> {
    let listener = TcpListener::bind((bind_addr, port))
        .await
        .with_context(|| format!("Could not listen on {bind_addr}:{port}"))?;
    log::info!("{name}: ONVIF PTZ service at {bind_addr}:{port}");
    let state = Arc::new(OnvifState {
        camera: camera.clone(),
        name: name.to_string(),
        config,
        cam_config,
        stop_timer: Mutex::new(None),
        moving: AtomicBool::new(false),
    });
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            match timeout(REQUEST_TIMEOUT, handle_connection(stream, &state)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    log::debug!("{}: ONVIF request from {peer} failed: {e:#}", state.name)
                }
                Err(_) => log::debug!("{}: ONVIF request from {peer} timed out", state.name),
            }
        });
    }
}

struct OnvifState {
    camera: NeoInstance,
    name: String,
    config: WatchReceiver<Config>,
    cam_config: WatchReceiver<CameraConfig>,
    /// Stops a ContinuousMove when its timeout expires
    stop_timer: Mutex<Option<JoinHandle<()>>>,
    moving: AtomicBool,
}

impl Drop for OnvifState {
    fn drop(&mut self) {
        if let Some(timer) = self.stop_timer.lock().ok().and_then(|mut t| t.take()) {
            timer.abort();
        }
    }
}

struct Reply {
    status: u16,
    body: String,
}

impl Reply {
    fn ok(body: String) -> Self {
        Self {
            status: 200,
            body: envelope(&body),
        }
    }
    fn sender_fault(subcode: &str, reason: &str) -> Self {
        Self {
            status: 400,
            body: fault("s:Sender", subcode, reason),
        }
    }
    fn receiver_fault(subcode: &str, reason: &str) -> Self {
        Self {
            status: 500,
            body: fault("s:Receiver", subcode, reason),
        }
    }
}

async fn handle_connection(mut stream: TcpStream, state: &Arc<OnvifState>) -> Result<()> {
    // Read the head
    let mut buf = Vec::with_capacity(4096);
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        if buf.len() > MAX_HEAD {
            return write_response(&mut stream, 431, "").await;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = parse_http_head(std::str::from_utf8(&buf[..head_end])?)?;
    if head.method != "POST" {
        return write_response(&mut stream, 405, "").await;
    }
    let Some(length) = head.content_length.filter(|_| !head.chunked) else {
        return write_response(&mut stream, 411, "").await;
    };
    if length > MAX_BODY {
        return write_response(&mut stream, 413, "").await;
    }

    // Read the body
    let mut body = buf.split_off(head_end + 4);
    if body.len() < length {
        let start = body.len();
        body.resize(length, 0);
        stream.read_exact(&mut body[start..]).await?;
    }
    body.truncate(length);

    let host = match head.host {
        Some(host) => host,
        None => stream.local_addr()?.to_string(),
    };
    let reply = match std::str::from_utf8(&body)
        .map_err(anyhow::Error::from)
        .and_then(parse_soap)
    {
        Ok(request) => state.handle(&head.path, &host, request).await,
        Err(e) => Reply::sender_fault("ter:WellFormed", &format!("Invalid request: {e}")),
    };
    write_response(&mut stream, reply.status, &reply.body).await
}

async fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        405 => "Method Not Allowed",
        411 => "Length Required",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        _ => "Internal Server Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/soap+xml; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

/// Translate an ONVIF pan/tilt velocity (each axis -1..1) to a camera
/// command. The camera moves along one axis at a time, so the larger axis
/// wins. Speed scales to the camera's 1..64 (0.5, Frigate's speed, is 32 as
/// used by the MQTT control). None means stop.
fn velocity_to_command(x: f32, y: f32) -> Option<(BcDirection, f32)> {
    let (x, y) = (
        if x.is_finite() {
            x.clamp(-1.0, 1.0)
        } else {
            0.0
        },
        if y.is_finite() {
            y.clamp(-1.0, 1.0)
        } else {
            0.0
        },
    );
    let (direction, magnitude) = if x.abs() >= y.abs() {
        (
            if x < 0.0 {
                BcDirection::Left
            } else {
                BcDirection::Right
            },
            x.abs(),
        )
    } else {
        (
            if y < 0.0 {
                BcDirection::Down
            } else {
                BcDirection::Up
            },
            y.abs(),
        )
    };
    if magnitude < 0.01 {
        return None;
    }
    Some((direction, (magnitude * 64.0).round().clamp(1.0, 64.0)))
}

/// The host part of a Host header value (drops the port)
fn host_only(host: &str) -> &str {
    if host.starts_with('[') {
        host.split_inclusive(']').next().unwrap_or(host)
    } else {
        host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host)
    }
}

fn stream_token(stream: StreamKind) -> &'static str {
    match stream {
        StreamKind::Main => "main",
        StreamKind::Sub => "sub",
        StreamKind::Extern => "extern",
    }
}

/// What the camera reports about one stream
#[derive(Clone, Copy, Default)]
struct StreamDetails {
    width: u32,
    height: u32,
    fps: u32,
    kbps: u32,
}

impl OnvifState {
    fn authorized(&self, header: Option<&Node>) -> bool {
        let config = self.config.borrow();
        if config.users.is_empty() {
            // As for RTSP: no users configured means no authentication
            return true;
        }
        let Some(token) = header.and_then(UsernameToken::from_header) else {
            return false;
        };
        let Some(user) = config.users.iter().find(|u| u.name == token.username) else {
            return false;
        };
        let permitted = match &self.cam_config.borrow().permitted_users {
            None => true,
            Some(users) => users.iter().any(|u| u == "anyone" || *u == token.username),
        };
        permitted && token.verify(user.pass.as_deref().unwrap_or_default())
    }

    async fn handle(self: &Arc<Self>, path: &str, host: &str, request: SoapRequest) -> Reply {
        let op = request.operation.name.as_str();
        // Clients read the time before authenticating to correct clock skew
        if op != "GetSystemDateAndTime" && !self.authorized(request.header.as_ref()) {
            log::debug!("{}: ONVIF {op} rejected: not authorized", self.name);
            return Reply::sender_fault("ter:NotAuthorized", "Sender not Authorized");
        }
        log::debug!("{}: ONVIF {op} on {path}", self.name);
        let result = match op {
            "GetSystemDateAndTime" => Ok(self.system_date_and_time()),
            "GetCapabilities" => Ok(self.capabilities(host)),
            "GetServices" => Ok(self.services(host)),
            "GetDeviceInformation" => Ok(self.device_information()),
            "GetProfiles" => Ok(self.profiles(None).await),
            "GetProfile" => {
                let token = request
                    .operation
                    .child("ProfileToken")
                    .map(|t| t.text().to_string())
                    .unwrap_or_default();
                Ok(self.profiles(Some(&token)).await)
            }
            "GetVideoSources" => Ok(self.video_sources().await),
            "GetStreamUri" => Ok(self.stream_uri(host, &request.operation)),
            "GetServiceCapabilities" if path.contains("ptz") => Ok(ptz_service_capabilities()),
            "GetNodes" => Ok(format!(
                "<tptz:GetNodesResponse>{}</tptz:GetNodesResponse>",
                ptz_node("tptz:PTZNode")
            )),
            "GetNode" => Ok(format!(
                "<tptz:GetNodeResponse>{}</tptz:GetNodeResponse>",
                ptz_node("tptz:PTZNode")
            )),
            "GetConfigurations" => Ok(format!(
                "<tptz:GetConfigurationsResponse>{}</tptz:GetConfigurationsResponse>",
                ptz_configuration("tptz:PTZConfiguration", self.profile_streams().len())
            )),
            "GetConfiguration" => Ok(format!(
                "<tptz:GetConfigurationResponse>{}</tptz:GetConfigurationResponse>",
                ptz_configuration("tptz:PTZConfiguration", self.profile_streams().len())
            )),
            "GetConfigurationOptions" => Ok(ptz_configuration_options()),
            "GetPresets" => self.presets().await,
            "GotoPreset" => self.goto_preset(&request.operation).await,
            "ContinuousMove" => self.continuous_move(&request.operation).await,
            "Stop" => self.stop(&request.operation).await,
            "GetStatus" => Ok(self.status()),
            _ => {
                log::debug!("{}: ONVIF {op} is not supported", self.name);
                return Reply::sender_fault(
                    "ter:ActionNotSupported",
                    &format!("{op} is not supported"),
                );
            }
        };
        match result {
            Ok(body) => Reply::ok(body),
            Err(e) => {
                log::warn!("{}: ONVIF {op} failed: {e:#}", self.name);
                Reply::receiver_fault("ter:Action", &format!("{op} failed: {e:#}"))
            }
        }
    }

    /// Send one PTZ command to the camera
    async fn ptz(&self, direction: BcDirection, speed: f32) -> Result<()> {
        timeout(
            CAMERA_TIMEOUT,
            self.camera.run_task(move |cam| {
                Box::pin(async move {
                    cam.send_ptz(direction, speed).await?;
                    Ok(())
                })
            }),
        )
        .await
        .map_err(|_| anyhow!("The camera did not respond"))?
    }

    fn cancel_stop_timer(&self) {
        if let Some(timer) = self.stop_timer.lock().expect("timer lock").take() {
            timer.abort();
        }
    }

    async fn continuous_move(self: &Arc<Self>, op: &Node) -> Result<String> {
        let pan_tilt = op.path(&["Velocity", "PanTilt"]);
        let axis = |name| {
            pan_tilt
                .and_then(|pt| pt.attr(name))
                .and_then(|v| v.parse::<f32>().ok())
                .unwrap_or(0.0)
        };
        let move_timeout = op
            .child("Timeout")
            .and_then(|t| parse_duration(t.text()))
            .unwrap_or(DEFAULT_MOVE_TIMEOUT)
            .min(Duration::from_secs(60));

        self.cancel_stop_timer();
        match velocity_to_command(axis("x"), axis("y")) {
            Some((direction, speed)) => {
                self.ptz(direction, speed).await?;
                self.moving.store(true, Ordering::Relaxed);
                let state = Arc::downgrade(self);
                let timer = tokio::spawn(async move {
                    sleep(move_timeout).await;
                    if let Some(state) = state.upgrade() {
                        if let Err(e) = state.ptz(BcDirection::Stop, 32.0).await {
                            log::warn!(
                                "{}: Could not stop the camera after the move timeout: {e:#}",
                                state.name
                            );
                        }
                        state.moving.store(false, Ordering::Relaxed);
                    }
                });
                *self.stop_timer.lock().expect("timer lock") = Some(timer);
            }
            // A zero velocity (or a zoom-only move, unsupported) stops
            None => {
                self.ptz(BcDirection::Stop, 32.0).await?;
                self.moving.store(false, Ordering::Relaxed);
            }
        }
        Ok("<tptz:ContinuousMoveResponse/>".to_string())
    }

    async fn stop(&self, op: &Node) -> Result<String> {
        let pan_tilt = op
            .child("PanTilt")
            .map(|n| n.text() != "false")
            .unwrap_or(true);
        if pan_tilt {
            self.cancel_stop_timer();
            self.ptz(BcDirection::Stop, 32.0).await?;
            self.moving.store(false, Ordering::Relaxed);
        }
        Ok("<tptz:StopResponse/>".to_string())
    }

    async fn presets(&self) -> Result<String> {
        let presets = timeout(
            CAMERA_TIMEOUT,
            self.camera.run_task(|cam| {
                Box::pin(async move { Ok(cam.get_ptz_preset().await?.preset_list.preset) })
            }),
        )
        .await
        .map_err(|_| anyhow!("The camera did not respond"))??;
        let body = presets
            .iter()
            .map(|p| {
                let name = p.name.clone().unwrap_or_else(|| format!("preset {}", p.id));
                format!(
                    r#"<tptz:Preset token="{}"><tt:Name>{}</tt:Name></tptz:Preset>"#,
                    p.id,
                    esc(&name)
                )
            })
            .collect::<String>();
        Ok(format!(
            "<tptz:GetPresetsResponse>{body}</tptz:GetPresetsResponse>"
        ))
    }

    async fn goto_preset(&self, op: &Node) -> Result<String> {
        let id: u8 = op
            .child("PresetToken")
            .map(|t| t.text())
            .unwrap_or_default()
            .parse()
            .context("Invalid PresetToken")?;
        self.cancel_stop_timer();
        timeout(
            CAMERA_TIMEOUT,
            self.camera.run_task(move |cam| {
                Box::pin(async move {
                    cam.moveto_ptz_preset(id).await?;
                    Ok(())
                })
            }),
        )
        .await
        .map_err(|_| anyhow!("The camera did not respond"))??;
        Ok("<tptz:GotoPresetResponse/>".to_string())
    }

    fn status(&self) -> String {
        let move_status = if self.moving.load(Ordering::Relaxed) {
            "MOVING"
        } else {
            "IDLE"
        };
        format!(
            "<tptz:GetStatusResponse><tptz:PTZStatus><tt:MoveStatus><tt:PanTilt>{move_status}</tt:PanTilt></tt:MoveStatus><tt:UtcTime>{}</tt:UtcTime></tptz:PTZStatus></tptz:GetStatusResponse>",
            xs_datetime(time::OffsetDateTime::now_utc())
        )
    }

    fn system_date_and_time(&self) -> String {
        let now = time::OffsetDateTime::now_utc();
        format!(
            "<tds:GetSystemDateAndTimeResponse><tds:SystemDateAndTime><tt:DateTimeType>Manual</tt:DateTimeType><tt:DaylightSavings>false</tt:DaylightSavings><tt:TimeZone><tt:TZ>UTC0</tt:TZ></tt:TimeZone><tt:UTCDateTime><tt:Time><tt:Hour>{}</tt:Hour><tt:Minute>{}</tt:Minute><tt:Second>{}</tt:Second></tt:Time><tt:Date><tt:Year>{}</tt:Year><tt:Month>{}</tt:Month><tt:Day>{}</tt:Day></tt:Date></tt:UTCDateTime></tds:SystemDateAndTime></tds:GetSystemDateAndTimeResponse>",
            now.hour(),
            now.minute(),
            now.second(),
            now.year(),
            u8::from(now.month()),
            now.day()
        )
    }

    fn capabilities(&self, host: &str) -> String {
        format!(
            "<tds:GetCapabilitiesResponse><tds:Capabilities><tt:Media><tt:XAddr>http://{host}/onvif/media_service</tt:XAddr><tt:StreamingCapabilities><tt:RTPMulticast>false</tt:RTPMulticast><tt:RTP_TCP>true</tt:RTP_TCP><tt:RTP_RTSP_TCP>true</tt:RTP_RTSP_TCP></tt:StreamingCapabilities></tt:Media><tt:PTZ><tt:XAddr>http://{host}/onvif/ptz_service</tt:XAddr></tt:PTZ></tds:Capabilities></tds:GetCapabilitiesResponse>",
            host = esc(host)
        )
    }

    fn services(&self, host: &str) -> String {
        let host = esc(host);
        let service = |ns: &str, path: &str, major: u32| {
            format!(
                "<tds:Service><tds:Namespace>{ns}</tds:Namespace><tds:XAddr>http://{host}/onvif/{path}</tds:XAddr><tds:Version><tt:Major>{major}</tt:Major><tt:Minor>0</tt:Minor></tds:Version></tds:Service>"
            )
        };
        format!(
            "<tds:GetServicesResponse>{}{}{}</tds:GetServicesResponse>",
            service(
                "http://www.onvif.org/ver10/device/wsdl",
                "device_service",
                2
            ),
            service("http://www.onvif.org/ver10/media/wsdl", "media_service", 2),
            service("http://www.onvif.org/ver20/ptz/wsdl", "ptz_service", 2),
        )
    }

    fn device_information(&self) -> String {
        format!(
            "<tds:GetDeviceInformationResponse><tds:Manufacturer>Reolink (via neolink)</tds:Manufacturer><tds:Model>neolink</tds:Model><tds:FirmwareVersion>{}</tds:FirmwareVersion><tds:SerialNumber>{}</tds:SerialNumber><tds:HardwareId>neolink</tds:HardwareId></tds:GetDeviceInformationResponse>",
            env!("CARGO_PKG_VERSION"),
            esc(&self.name)
        )
    }

    /// The streams offered as media profiles: the ones served over RTSP, or
    /// main if none are
    fn profile_streams(&self) -> Vec<StreamKind> {
        let streams = self.cam_config.borrow().stream.as_stream_kinds();
        if streams.is_empty() {
            vec![StreamKind::Main]
        } else {
            streams
        }
    }

    /// Resolution, fps and bitrate of each stream, as the camera reports
    /// them. Zero if the camera cannot be asked.
    async fn stream_details(&self, stream: StreamKind) -> StreamDetails {
        let name = stream.to_string();
        let info = timeout(
            CAMERA_TIMEOUT,
            self.camera.run_passive_task(|cam| {
                let name = name.clone();
                Box::pin(async move {
                    let info = cam.get_stream_info().await?;
                    let details = info
                        .stream_infos
                        .iter()
                        .flat_map(|i| i.encode_tables.iter())
                        .find(|e| e.name == name)
                        .map(|e| {
                            let pick = |table: &str, index: u32| {
                                table
                                    .split(',')
                                    .filter_map(|v| v.trim().parse::<u32>().ok())
                                    .nth(index as usize)
                                    .unwrap_or(index)
                            };
                            StreamDetails {
                                width: e.resolution.width,
                                height: e.resolution.height,
                                fps: pick(&e.framerate_table, e.default_framerate),
                                kbps: pick(&e.bitrate_table, e.default_bitrate),
                            }
                        })
                        .unwrap_or_default();
                    Ok(details)
                })
            }),
        )
        .await;
        match info {
            Ok(Ok(details)) => details,
            _ => StreamDetails::default(),
        }
    }

    async fn profiles(&self, only: Option<&str>) -> String {
        let streams = self.profile_streams();
        let count = streams.len();
        let mut body = String::new();
        for stream in streams {
            let token = stream_token(stream);
            if only.is_some_and(|only| only != token) {
                continue;
            }
            let d = self.stream_details(stream).await;
            body.push_str(&format!(
                concat!(
                    r#"<trt:{tag} token="{token}" fixed="true"><tt:Name>{token}</tt:Name>"#,
                    r#"<tt:VideoEncoderConfiguration token="venc_{token}"><tt:Name>{token}</tt:Name><tt:UseCount>1</tt:UseCount>"#,
                    "<tt:Encoding>H264</tt:Encoding><tt:Resolution><tt:Width>{w}</tt:Width><tt:Height>{h}</tt:Height></tt:Resolution><tt:Quality>5</tt:Quality>",
                    "<tt:RateControl><tt:FrameRateLimit>{fps}</tt:FrameRateLimit><tt:EncodingInterval>1</tt:EncodingInterval><tt:BitrateLimit>{kbps}</tt:BitrateLimit></tt:RateControl>",
                    "<tt:Multicast><tt:Address><tt:Type>IPv4</tt:Type><tt:IPv4Address>0.0.0.0</tt:IPv4Address></tt:Address><tt:Port>0</tt:Port><tt:TTL>0</tt:TTL><tt:AutoStart>false</tt:AutoStart></tt:Multicast>",
                    "<tt:SessionTimeout>PT60S</tt:SessionTimeout></tt:VideoEncoderConfiguration>",
                    "{ptz}</trt:{tag}>",
                ),
                tag = if only.is_some() { "Profile" } else { "Profiles" },
                token = token,
                w = d.width,
                h = d.height,
                fps = d.fps,
                kbps = d.kbps,
                ptz = ptz_configuration("tt:PTZConfiguration", count),
            ));
        }
        if only.is_some() {
            format!("<trt:GetProfileResponse>{body}</trt:GetProfileResponse>")
        } else {
            format!("<trt:GetProfilesResponse>{body}</trt:GetProfilesResponse>")
        }
    }

    async fn video_sources(&self) -> String {
        let d = self.stream_details(StreamKind::Main).await;
        format!(
            r#"<trt:GetVideoSourcesResponse><trt:VideoSources token="{VIDEO_SOURCE_TOKEN}"><tt:Framerate>{}</tt:Framerate><tt:Resolution><tt:Width>{}</tt:Width><tt:Height>{}</tt:Height></tt:Resolution></trt:VideoSources></trt:GetVideoSourcesResponse>"#,
            d.fps, d.width, d.height
        )
    }

    fn stream_uri(&self, host: &str, op: &Node) -> String {
        let token = op
            .child("ProfileToken")
            .map(|t| t.text())
            .filter(|t| ["main", "sub", "extern"].contains(t))
            .unwrap_or("main");
        let rtsp_port = self.config.borrow().bind_port;
        let uri = format!(
            "rtsp://{}:{rtsp_port}/{}/{token}",
            host_only(host),
            self.name
        );
        format!(
            "<trt:GetStreamUriResponse><trt:MediaUri><tt:Uri>{}</tt:Uri><tt:InvalidAfterConnect>false</tt:InvalidAfterConnect><tt:InvalidAfterReboot>false</tt:InvalidAfterReboot><tt:Timeout>PT0S</tt:Timeout></trt:MediaUri></trt:GetStreamUriResponse>",
            esc(&uri)
        )
    }
}

fn ptz_configuration(tag: &str, use_count: usize) -> String {
    format!(
        r#"<{tag} token="{PTZ_CONFIG_TOKEN}"><tt:Name>PTZ</tt:Name><tt:UseCount>{use_count}</tt:UseCount><tt:NodeToken>{PTZ_NODE_TOKEN}</tt:NodeToken><tt:DefaultContinuousPanTiltVelocitySpace>{SPACE_PT_VELOCITY}</tt:DefaultContinuousPanTiltVelocitySpace><tt:DefaultPTZSpeed><tt:PanTilt x="0.5" y="0.5" space="{SPACE_PT_SPEED}"/></tt:DefaultPTZSpeed><tt:DefaultPTZTimeout>PT{}S</tt:DefaultPTZTimeout></{tag}>"#,
        DEFAULT_MOVE_TIMEOUT.as_secs()
    )
}

fn velocity_space() -> String {
    format!(
        "<tt:ContinuousPanTiltVelocitySpace><tt:URI>{SPACE_PT_VELOCITY}</tt:URI><tt:XRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:XRange><tt:YRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:YRange></tt:ContinuousPanTiltVelocitySpace>"
    )
}

fn ptz_configuration_options() -> String {
    format!(
        "<tptz:GetConfigurationOptionsResponse><tptz:PTZConfigurationOptions><tt:Spaces>{}</tt:Spaces><tt:PTZTimeout><tt:Min>PT1S</tt:Min><tt:Max>PT60S</tt:Max></tt:PTZTimeout></tptz:PTZConfigurationOptions></tptz:GetConfigurationOptionsResponse>",
        velocity_space()
    )
}

fn ptz_node(tag: &str) -> String {
    format!(
        r#"<{tag} token="{PTZ_NODE_TOKEN}" FixedHomePosition="false"><tt:Name>PTZ</tt:Name><tt:SupportedPTZSpaces>{}</tt:SupportedPTZSpaces><tt:MaximumNumberOfPresets>64</tt:MaximumNumberOfPresets><tt:HomeSupported>false</tt:HomeSupported></{tag}>"#,
        velocity_space()
    )
}

fn ptz_service_capabilities() -> String {
    r#"<tptz:GetServiceCapabilitiesResponse><tptz:Capabilities EFlip="false" Reverse="false" GetCompatibleConfigurations="false"/></tptz:GetServiceCapabilitiesResponse>"#.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(x: f32, y: f32) -> Option<(&'static str, f32)> {
        velocity_to_command(x, y).map(|(d, s)| {
            (
                match d {
                    BcDirection::Up => "up",
                    BcDirection::Down => "down",
                    BcDirection::Left => "left",
                    BcDirection::Right => "right",
                    BcDirection::Stop => "stop",
                },
                s,
            )
        })
    }

    #[test]
    fn maps_velocity_to_camera_commands() {
        // What Frigate sends
        assert_eq!(cmd(-0.5, 0.0), Some(("left", 32.0)));
        assert_eq!(cmd(0.5, 0.0), Some(("right", 32.0)));
        assert_eq!(cmd(0.0, 0.5), Some(("up", 32.0)));
        assert_eq!(cmd(0.0, -0.5), Some(("down", 32.0)));
        // Diagonals use the larger axis
        assert_eq!(cmd(0.2, -0.9), Some(("down", 58.0)));
        // Clamped and never below the camera's minimum speed
        assert_eq!(cmd(5.0, 0.0), Some(("right", 64.0)));
        assert_eq!(cmd(0.011, 0.0), Some(("right", 1.0)));
        // Zero or garbage means stop
        assert_eq!(cmd(0.0, 0.0), None);
        assert_eq!(cmd(f32::NAN, 0.0), None);
    }

    #[test]
    fn strips_port_from_host() {
        assert_eq!(host_only("10.0.0.2:8001"), "10.0.0.2");
        assert_eq!(host_only("cam.local"), "cam.local");
        assert_eq!(host_only("[fe80::1]:8001"), "[fe80::1]");
    }

    #[test]
    fn responses_are_well_formed() {
        for body in [
            ptz_configuration("tt:PTZConfiguration", 2),
            ptz_configuration_options(),
            ptz_node("tptz:PTZNode"),
            ptz_service_capabilities(),
        ] {
            parse_xml(&envelope(&body)).unwrap();
        }
    }
}
