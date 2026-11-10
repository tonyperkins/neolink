use gstreamer::ClockTime;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, Caps, Element, ElementFactory, FlowError, GhostPad};
use gstreamer_app::{AppLeakyType, AppSrc, AppSrcCallbacks, AppStreamType};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{
        BcMedia, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe, VideoType,
    },
};
use tokio::{sync::mpsc::channel as mpsc, task::JoinHandle};

use super::hub::StreamHub;
use crate::{common::NeoInstance, rtsp::gst::NeoMediaFactory, AnyResult};

#[derive(Clone, Debug)]
pub enum AudioType {
    Aac,
    Adpcm(u32),
}

#[derive(Clone, Debug)]
struct StreamConfig {
    #[allow(dead_code)]
    resolution: [u32; 2],
    bitrate: u32,
    fps: u32,
    bitrate_table: Vec<u32>,
    fps_table: Vec<u32>,
    vid_type: Option<VideoType>,
    aud_type: Option<AudioType>,
}
impl StreamConfig {
    async fn new(instance: &NeoInstance, name: StreamKind) -> AnyResult<Self> {
        let (resolution, bitrate, fps, fps_table, bitrate_table) = instance
            .run_passive_task(|cam| {
                Box::pin(async move {
                    let infos = cam
                        .get_stream_info()
                        .await?
                        .stream_infos
                        .iter()
                        .flat_map(|info| info.encode_tables.clone())
                        .collect::<Vec<_>>();
                    if let Some(encode) =
                        infos.iter().find(|encode| encode.name == name.to_string())
                    {
                        let bitrate_table = encode
                            .bitrate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();
                        let framerate_table = encode
                            .framerate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();

                        Ok((
                            [encode.resolution.width, encode.resolution.height],
                            bitrate_table
                                .get(encode.default_bitrate as usize)
                                .copied()
                                .unwrap_or(encode.default_bitrate)
                                * 1024,
                            framerate_table
                                .get(encode.default_framerate as usize)
                                .copied()
                                .unwrap_or(encode.default_framerate),
                            framerate_table.clone(),
                            bitrate_table.clone(),
                        ))
                    } else {
                        Ok(([0, 0], 0, 0, vec![], vec![]))
                    }
                })
            })
            .await?;

        Ok(StreamConfig {
            resolution,
            bitrate,
            fps,
            fps_table,
            bitrate_table,
            vid_type: None,
            aud_type: None,
        })
    }

    fn update_fps(&mut self, fps: u32) {
        let new_fps = self.fps_table.get(fps as usize).copied().unwrap_or(fps);
        self.fps = new_fps;
    }
    #[allow(dead_code)]
    fn update_bitrate(&mut self, bitrate: u32) {
        let new_bitrate = self
            .bitrate_table
            .get(bitrate as usize)
            .copied()
            .unwrap_or(bitrate);
        self.bitrate = new_bitrate;
    }

    fn update_from_media(&mut self, media: &BcMedia) {
        match media {
            BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
            | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) => self.update_fps(*fps as u32),
            BcMedia::Aac(_) => {
                self.aud_type = Some(AudioType::Aac);
            }
            BcMedia::Adpcm(adpcm) => {
                self.aud_type = Some(AudioType::Adpcm(adpcm.block_size()));
            }
            BcMedia::Iframe(BcMediaIframe { video_type, .. })
            | BcMedia::Pframe(BcMediaPframe { video_type, .. }) => {
                self.vid_type = Some(*video_type);
            }
        }
    }
}

pub(super) async fn make_dummy_factory(
    use_splash: bool,
    pattern: String,
) -> AnyResult<NeoMediaFactory> {
    NeoMediaFactory::new_with_callback(move |element| {
        clear_bin(&element)?;
        if !use_splash {
            Ok(None)
        } else {
            build_unknown(&element, &pattern)?;
            Ok(Some(element))
        }
    })
    .await
}

enum ClientMsg {
    NewClient {
        element: Element,
        reply: std::sync::mpsc::SyncSender<Element>,
    },
}

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn(async move {
        let name = camera.config().await?.borrow().name.clone();
        let hub = StreamHub::new(camera.clone(), stream, name.clone());

        while let Some(msg) = client_rx.recv().await {
            match msg {
                ClientMsg::NewClient { element, reply } => {
                    log::debug!("New client for {name}::{stream}");
                    let camera = camera.clone();
                    let name = name.clone();
                    let hub = hub.clone();
                    tokio::task::spawn(async move {
                        clear_bin(&element)?;
                        log::trace!("{name}::{stream}: Starting camera");

                        // Start the camera
                        let config = camera.config().await?.borrow().clone();
                        let mut media_rx = hub.subscribe();

                        log::trace!("{name}::{stream}: Learning camera stream type");
                        // Learn the camera data type. Bounded by the same timeout
                        // as the factory's wait: past that the RTSP request has
                        // already failed, and while the camera is down every client
                        // retry would otherwise park a task (holding a hub
                        // subscription and a pipeline bin) here until it returns.
                        let learned = tokio::time::timeout(PIPELINE_BUILD_TIMEOUT, async {
                            let mut buffer = vec![];
                            let mut frame_count = 0usize;
                            let mut stream_config = StreamConfig::new(&camera, stream).await?;
                            while let Some(media) = media_rx.recv().await {
                                stream_config.update_from_media(&media);
                                buffer.push(media);
                                if frame_count > 10
                                    || (stream_config.vid_type.is_some()
                                        && stream_config.aud_type.is_some())
                                {
                                    break;
                                }
                                frame_count += 1;
                            }
                            AnyResult::Ok((stream_config, buffer))
                        })
                        .await;
                        let (stream_config, mut buffer) = match learned {
                            Ok(learned) => learned?,
                            Err(_) => {
                                log::debug!(
                                    "{name}::{stream}: Camera stream not ready within {PIPELINE_BUILD_TIMEOUT:?}, dropping client"
                                );
                                return AnyResult::Ok(());
                            }
                        };

                        log::trace!("{name}::{stream}: Building the pipeline");
                        // Build the right video pipeline
                        let vid_src = match stream_config.vid_type.as_ref() {
                            Some(VideoType::H264) => {
                                let src = build_h264(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(VideoType::H265) => {
                                let src = build_h265(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => {
                                log::warn!(
                                    "{name}::{stream}: No video seen while learning the stream format, serving the test pattern"
                                );
                                build_unknown(&element, &config.splash_pattern.to_string())?;
                                AnyResult::Ok(None)
                            }
                        }?;

                        // Build the right audio pipeline
                        let aud_src = match stream_config.aud_type.as_ref() {
                            Some(AudioType::Aac) => {
                                let src = build_aac(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(AudioType::Adpcm(block_size)) => {
                                let src = build_adpcm(&element, *block_size, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => AnyResult::Ok(None),
                        }?;

                        if let Some(app) = vid_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }
                        if let Some(app) = aud_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }

                        log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
                        // Send the pipeline back to the factory so it can start
                        if reply.send(element).is_err() {
                            // The factory gave up waiting; don't start a feeder for
                            // a pipeline nobody will use
                            log::debug!("{name}::{stream}: Factory stopped waiting for pipeline");
                            return AnyResult::Ok(());
                        }
                        let runtime = tokio::runtime::Handle::current();

                        // Run blocking code on a seperate thread
                        // This is not an async thread
                        std::thread::spawn(move || {
                            let mut clock = MediaClock::new(&stream_config);
                            let mut liveness = Liveness::new();

                            log::trace!("{name}::{stream}: Sending buffered frames");
                            for buffered in buffer.drain(..) {
                                send_to_sources(
                                    buffered,
                                    &mut liveness,
                                    &vid_src,
                                    &aud_src,
                                    &mut clock,
                                )?;
                            }

                            log::trace!("{name}::{stream}: Sending new frames");
                            loop {
                                // Wait with a timeout so the pipeline is still checked
                                // while the camera is quiet (paused, reconnecting)
                                // The timer must be created inside the runtime context
                                let r = match runtime.block_on(async {
                                    tokio::time::timeout(Duration::from_secs(5), media_rx.recv())
                                        .await
                                }) {
                                    Ok(Some(data)) => send_to_sources(
                                        data,
                                        &mut liveness,
                                        &vid_src,
                                        &aud_src,
                                        &mut clock,
                                    ),
                                    Ok(None) => break,
                                    Err(_) => match vid_src.as_ref().or(aud_src.as_ref()) {
                                        Some(src) => {
                                            check_live(src).and_then(|_| liveness.check(src))
                                        }
                                        None => Ok(()),
                                    },
                                };
                                if let Err(r) = &r {
                                    log::info!("{name}::{stream}: Stopping client feed: {r:?}");
                                }
                                r?;
                            }
                            log::trace!("All media recieved");
                            AnyResult::Ok(())
                        });
                        AnyResult::Ok(())
                    });
                }
            }
        }
        AnyResult::Ok(())
    });

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(move |element| {
        // This runs on the RTSP server's main loop, so a hang here stalls every
        // client of every camera. Bound the wait.
        let (reply, new_element) = std::sync::mpsc::sync_channel(1);
        client_tx.blocking_send(ClientMsg::NewClient { element, reply })?;

        let element = new_element
            .recv_timeout(PIPELINE_BUILD_TIMEOUT)
            .map_err(|e| anyhow!("Pipeline was not ready in time: {e}"))?;
        Ok(Some(element))
    })
    .await?;
    Ok((factory, thread))
}

/// How long an RTSP request may wait for the camera stream to be ready
const PIPELINE_BUILD_TIMEOUT: Duration = Duration::from_secs(20);

/// How long the pipeline may sit below PAUSED (after having played)
/// before we assume the client has gone and stop feeding it.
///
/// The RTSP server legitimately drops the pipeline to NULL for short periods
/// (e.g. suspend between SETUP and PLAY), so this cannot be instant.
const STOPPED_GRACE: Duration = Duration::from_secs(30);
/// How long we wait for a new pipeline to reach PAUSED/PLAYING at all.
const STARTUP_GRACE: Duration = Duration::from_secs(60);

/// Tracks whether the gstreamer pipeline we are feeding is still in use.
///
/// Without this, the feeder thread (and the camera stream, pipeline and
/// buffers it holds) outlives a disconnected client, because pushing into a
/// stopped appsrc just returns `Flushing`.
struct Liveness {
    created: Instant,
    started: bool,
    stopped_since: Option<Instant>,
}

impl Liveness {
    fn new() -> Self {
        Self {
            created: Instant::now(),
            started: false,
            stopped_since: None,
        }
    }

    fn check(&mut self, appsrc: &AppSrc) -> Result<()> {
        let now = Instant::now();
        if matches!(
            appsrc.current_state(),
            gstreamer::State::Paused | gstreamer::State::Playing
        ) {
            self.started = true;
            self.stopped_since = None;
        } else if self.started {
            let since = *self.stopped_since.get_or_insert(now);
            if now.duration_since(since) > STOPPED_GRACE {
                return Err(anyhow!(
                    "{} has been stopped for over {:?}, client has gone",
                    appsrc.name(),
                    STOPPED_GRACE
                ));
            }
        } else if now.duration_since(self.created) > STARTUP_GRACE {
            return Err(anyhow!(
                "{} never started within {:?}",
                appsrc.name(),
                STARTUP_GRACE
            ));
        }
        Ok(())
    }
}

/// Gap in the camera's own frame clock beyond which we treat it as a
/// discontinuity (camera restart, clock reset) and fall back to nominal fps.
const MAX_CAMERA_FRAME_GAP_US: u64 = 2_000_000;

/// Generates presentation timestamps for the RTSP stream.
///
/// Timestamps are `u64` microseconds. They used to be `u32`, which wraps after
/// ~71.6 minutes and makes PTS/DTS jump backwards for long-running clients
/// (seen downstream as "non-monotonic DTS").
///
/// Video uses the camera's own per-frame microsecond clock when it is
/// available. The previous approach (frame count / reported fps) drifts badly
/// when the reported fps is wrong; e.g. an E1 Pro reports 20 while sending 10,
/// which made the stream play at half speed.
struct MediaClock {
    vid_ts: u64,
    aud_ts: u64,
    last_camera_us: Option<u32>,
    nominal_frame_us: u64,
    frames: u64,
    camera_elapsed_us: u64,
    reported_fps: u32,
    next_drift_report_us: u64,
}

/// How often (in stream time) to log the audio/video clock difference
const DRIFT_REPORT_INTERVAL_US: u64 = 10 * 60 * 1_000_000;

impl MediaClock {
    fn new(stream_config: &StreamConfig) -> Self {
        Self::with_fps(stream_config.fps)
    }

    fn with_fps(reported_fps: u32) -> Self {
        Self {
            vid_ts: 0,
            aud_ts: 0,
            last_camera_us: None,
            nominal_frame_us: 1_000_000 / reported_fps.max(1) as u64,
            frames: 0,
            camera_elapsed_us: 0,
            reported_fps,
            next_drift_report_us: DRIFT_REPORT_INTERVAL_US,
        }
    }

    /// Timestamp for the next video frame, given the camera's frame clock
    fn next_video(&mut self, camera_us: u32) -> Duration {
        if let Some(last) = self.last_camera_us {
            let delta = camera_us.wrapping_sub(last) as u64;
            let step = if delta == 0 || delta > MAX_CAMERA_FRAME_GAP_US {
                log::debug!("Camera frame clock jumped by {delta}us, using nominal frame time");
                self.nominal_frame_us
            } else {
                self.camera_elapsed_us += delta;
                delta
            };
            self.vid_ts += step;
        }
        self.last_camera_us = Some(camera_us);
        self.frames += 1;
        if self.frames == 100 && self.camera_elapsed_us > 0 {
            let observed_fps = 99.0 * 1_000_000.0 / self.camera_elapsed_us as f64;
            log::info!(
                "Camera reported {} fps, observed {:.1} fps from frame timestamps",
                self.reported_fps,
                observed_fps
            );
        }
        if self.vid_ts >= self.next_drift_report_us {
            self.next_drift_report_us += DRIFT_REPORT_INTERVAL_US;
            if self.aud_ts > 0 {
                log::info!(
                    "A/V clocks at {:.1} min: audio-video = {:+} ms",
                    self.vid_ts as f64 / 60e6,
                    (self.aud_ts as i64 - self.vid_ts as i64) / 1000
                );
            }
        }
        Duration::from_micros(self.vid_ts)
    }

    /// Timestamp for the next audio packet, advancing by its duration
    fn next_audio(&mut self, duration_us: Option<u32>) -> Duration {
        let ts = Duration::from_micros(self.aud_ts);
        // If the duration cannot be calculated (malformed header) assume a
        // typical AAC frame rather than panicking the feeder thread
        self.aud_ts += duration_us.unwrap_or(64_000) as u64;
        ts
    }
}

fn send_to_sources(
    data: BcMedia,
    liveness: &mut Liveness,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    clock: &mut MediaClock,
) -> AnyResult<()> {
    match data {
        BcMedia::Aac(aac) => {
            let ts = clock.next_audio(aac.duration());
            if let Some(aud_src) = aud_src.as_ref() {
                log::trace!("Sending AAC: {:?}", ts);
                send_to_appsrc(aud_src, aac.data, ts, liveness)?;
            }
        }
        BcMedia::Adpcm(adpcm) => {
            let ts = clock.next_audio(adpcm.duration());
            if let Some(aud_src) = aud_src.as_ref() {
                log::trace!("Sending ADPCM: {:?}", ts);
                send_to_appsrc(aud_src, adpcm.data, ts, liveness)?;
            }
        }
        BcMedia::Iframe(BcMediaIframe {
            data, microseconds, ..
        })
        | BcMedia::Pframe(BcMediaPframe {
            data, microseconds, ..
        }) => {
            let ts = clock.next_video(microseconds);
            if let Some(vid_src) = vid_src.as_ref() {
                log::trace!("Sending VID: {:?}", ts);
                send_to_appsrc(vid_src, data, ts, liveness)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn send_to_appsrc(
    appsrc: &AppSrc,
    data: Vec<u8>,
    mut ts: Duration,
    liveness: &mut Liveness,
) -> AnyResult<()> {
    check_live(appsrc)?; // Stop if appsrc is unlinked
    liveness.check(appsrc)?; // Stop if the pipeline has been torn down

    // In live mode we follow the advice in
    // https://gstreamer.freedesktop.org/documentation/additional/design/element-source.html?gi-language=c#live-sources
    // Only push buffers when in play state and have a clock
    // we also timestamp at the current time
    if appsrc.is_live() {
        if let Some(time) = appsrc
            .current_clock_time()
            .and_then(|t| appsrc.base_time().map(|bt| t - bt))
        {
            if matches!(appsrc.current_state(), gstreamer::State::Playing) {
                ts = Duration::from_micros(time.useconds());
            } else {
                // Not playing
                return Ok(());
            }
        } else {
            // Clock not up yet
            return Ok(());
        }
    }
    // Wrap the data directly in a gstreamer buffer (no copy).
    //
    // Previously a BufferPool was created for every distinct frame size and
    // kept forever; since video frames vary in size almost every frame this
    // grew without bound.
    let buf = {
        let mut gst_buf = gstreamer::Buffer::from_mut_slice(data);
        let gst_buf_mut = gst_buf
            .get_mut()
            .expect("Newly created buffer should be writable");
        let time = ClockTime::from_useconds(ts.as_micros() as u64);
        gst_buf_mut.set_dts(time);
        gst_buf_mut.set_pts(time);
        gst_buf
    };

    // Push buffer into the appsrc
    match appsrc.push_buffer(buf) {
        Ok(_) => {
            // log::info!(
            //     "Send {}{} on {}",
            //     data.data.len(),
            //     if data.keyframe { " (keyframe)" } else { "" },
            //     appsrc.name()
            // );
            Ok(())
        }
        Err(FlowError::Flushing) => {
            // The appsrc is not running (not yet started, suspended, or torn down).
            // Drop the frame; `Liveness` decides when to give up entirely.
            log::trace!("{} is flushing, dropping frame", appsrc.name());
            Ok(())
        }
        Err(e) => Err(anyhow!("Error in streaming: {e:?}")),
    }?;
    // Note: this used to toggle the appsrc between PAUSED and PLAYING based on
    // its fill level. Changing a source's state waits for its streaming thread,
    // so when a branch is blocked downstream (e.g. a client that only SETUPs
    // video leaves the audio branch blocked) the call never returned and the
    // feeder thread, and with it video, froze after ~20s. The appsrc queue is
    // bounded by its leaky type, so no manual flow control is needed.
    Ok(())
}
fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
}

fn clear_bin(bin: &Element) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    // Clear the autogenerated ones
    for element in bin.iterate_elements().into_iter().flatten() {
        bin.remove(&element)?;
    }

    Ok(())
}

fn build_unknown(bin: &Element, pattern: &str) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Unknown Pipeline");
    let source = make_element("videotestsrc", "testvidsrc")?;
    source.set_property_from_str("pattern", pattern);
    source.set_property("num-buffers", 500i32); // Send buffers then EOS
    let queue = make_queue("queue0", 1024 * 1024 * 4)?;

    let overlay = make_element("textoverlay", "overlay")?;
    overlay.set_property("text", "Stream not Ready");
    overlay.set_property_from_str("valignment", "top");
    overlay.set_property_from_str("halignment", "left");
    overlay.set_property("font-desc", "Sans, 16");
    let encoder = make_element("jpegenc", "encoder")?;
    let payload = make_element("rtpjpegpay", "pay0")?;

    bin.add_many([&source, &queue, &overlay, &encoder, &payload])?;
    source.link_filtered(
        &queue,
        &Caps::builder("video/x-raw")
            .field("format", "YUY2")
            .field("width", 896i32)
            .field("height", 512i32)
            .field("framerate", gstreamer::Fraction::new(25, 1))
            .build(),
    )?;
    Element::link_many([&queue, &overlay, &encoder, &payload])?;

    Ok(())
}

struct Linked {
    appsrc: AppSrc,
    output: Element,
}

fn pipe_h264(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    log::debug!(
        "buffer_size: {buffer_size}, bitrate: {}",
        stream_config.bitrate
    );
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H264 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps.max(1) as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);
    // With block=false, max_bytes is only a signal threshold unless a leaky
    // type is set; without it the internal queue grows without bound when a
    // client consumes slower than the camera produces.
    source.set_leaky_type(AppLeakyType::Downstream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h264parse", "parser")?;
    // let stamper = make_element("h264timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h264(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h264(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph264pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_h265(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H265 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps.max(1) as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);
    // With block=false, max_bytes is only a signal threshold unless a leaky
    // type is set; without it the internal queue grows without bound when a
    // client consumes slower than the camera produces.
    source.set_leaky_type(AppLeakyType::Downstream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h265parse", "parser")?;
    // let stamper = make_element("h265timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h265(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h265(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph265pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_aac(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps.max(1) as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);
    // With block=false, max_bytes is only a signal threshold unless a leaky
    // type is set; without it the internal queue grows without bound when a
    // client consumes slower than the camera produces.
    source.set_leaky_type(AppLeakyType::Downstream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let parser = make_element("aacparse", "audparser")?;
    let decoder = match make_element("faad", "auddecoder_faad") {
        Ok(ele) => Ok(ele),
        Err(_) => make_element("avdec_aac", "auddecoder_avdec_aac"),
    }?;

    // The fallback
    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let fallback_switch = make_element("fallbackswitch", "audfallbackswitch");
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        fallback_switch.set_property("timeout", 3u64 * 1_000_000_000u64);
        fallback_switch.set_property("immediate-fallback", true);
    }

    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &queue, &parser, &decoder, &encoder])?;
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        bin.add_many([&silence, fallback_switch])?;
        Element::link_many([
            &source,
            &queue,
            &parser,
            &decoder,
            fallback_switch,
            &encoder,
        ])?;
        Element::link_many([&silence, fallback_switch])?;
    } else {
        Element::link_many([&source, &queue, &parser, &decoder, &encoder])?;
    }

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

fn build_aac(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_aac(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Adpcm pipeline");
    // Original command line
    // caps=audio/x-adpcm,layout=dvi,block_align={},channels=1,rate=8000
    // ! queue silent=true max-size-bytes=10485760 min-threshold-bytes=1024
    // ! adpcmdec
    // ! audioconvert
    // ! rtpL16pay name=pay1

    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps.max(1) as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);
    // With block=false, max_bytes is only a signal threshold unless a leaky
    // type is set; without it the internal queue grows without bound when a
    // client consumes slower than the camera produces.
    source.set_leaky_type(AppLeakyType::Downstream);

    source.set_caps(Some(
        &Caps::builder("audio/x-adpcm")
            .field("layout", "div")
            .field("block_align", block_size as i32)
            .field("channels", 1i32)
            .field("rate", 8000i32)
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let decoder = make_element("decodebin", "auddecoder")?;
    let encoder = make_element("audioconvert", "audencoder")?;
    let encoder_out = encoder.clone();

    bin.add_many([&source, &queue, &decoder, &encoder])?;
    Element::link_many([&source, &queue, &decoder])?;
    decoder.connect_pad_added(move |_element, pad| {
        let sink_pad = encoder
            .static_pad("sink")
            .expect("Encoder is missing its pad");
        pad.link(&sink_pad)
            .expect("Failed to link ADPCM decoder to encoder");
    });

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder_out,
    })
}

fn build_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_adpcm(bin, block_size, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

#[allow(dead_code)]
fn pipe_silence(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Silence pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps.max(1) as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);
    // With block=false, max_bytes is only a signal threshold unless a leaky
    // type is set; without it the internal queue grows without bound when a
    // client consumes slower than the camera produces.
    source.set_leaky_type(AppLeakyType::Downstream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let sink_queue = make_queue("audsinkqueue", buffer_size)?;
    let sink = make_element("fakesink", "silence_sink")?;

    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let src_queue = make_queue("audsinkqueue", buffer_size)?;
    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &sink_queue, &sink, &silence, &src_queue, &encoder])?;

    Element::link_many([&source, &sink_queue, &sink])?;

    Element::link_many([&silence, &src_queue, &encoder])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

#[allow(dead_code)]
struct AppSrcPair {
    vid: AppSrc,
    aud: Option<AppSrc>,
}

// #[allow(dead_code)]
// /// Experimental build a stream of MPEGTS
// fn build_mpegts(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrcPair> {
//     let buffer_size = buffer_size(stream_config.bitrate);
//     log::debug!(
//         "buffer_size: {buffer_size}, bitrate: {}",
//         stream_config.bitrate
//     );

//     // VID
//     let vid_link = match stream_config.vid_format {
//         VidFormat::H264 => pipe_h264(bin, stream_config)?,
//         VidFormat::H265 => pipe_h265(bin, stream_config)?,
//         VidFormat::None => unreachable!(),
//     };

//     // AUD
//     let aud_link = match stream_config.aud_format {
//         AudFormat::Aac => pipe_aac(bin, stream_config)?,
//         AudFormat::Adpcm(block) => pipe_adpcm(bin, block, stream_config)?,
//         AudFormat::None => pipe_silence(bin, stream_config)?,
//     };

//     let bin = bin
//         .clone()
//         .dynamic_cast::<Bin>()
//         .map_err(|_| anyhow!("Media source's element should be a bin"))?;

//     // MUX
//     let muxer = make_element("mpegtsmux", "mpeg_muxer")?;
//     let rtp = make_element("rtpmp2tpay", "pay0")?;

//     bin.add_many([&muxer, &rtp])?;
//     Element::link_many([&vid_link.output, &muxer, &rtp])?;
//     Element::link_many([&aud_link.output, &muxer])?;

//     Ok(AppSrcPair {
//         vid: vid_link.appsrc,
//         aud: Some(aud_link.appsrc),
//     })
// }

// Convenice funcion to make an element or provide a message
// about what plugin is missing
fn make_element(kind: &str, name: &str) -> AnyResult<Element> {
    ElementFactory::make_with_name(kind, Some(name)).with_context(|| {
        let plugin = match kind {
            "appsrc" => "app (gst-plugins-base)",
            "audioconvert" => "audioconvert (gst-plugins-base)",
            "adpcmdec" => "Required for audio",
            "h264parse" => "videoparsersbad (gst-plugins-bad)",
            "h265parse" => "videoparsersbad (gst-plugins-bad)",
            "rtph264pay" => "rtp (gst-plugins-good)",
            "rtph265pay" => "rtp (gst-plugins-good)",
            "rtpjitterbuffer" => "rtp (gst-plugins-good)",
            "aacparse" => "audioparsers (gst-plugins-good)",
            "rtpL16pay" => "rtp (gst-plugins-good)",
            "x264enc" => "x264 (gst-plugins-ugly)",
            "x265enc" => "x265 (gst-plugins-bad)",
            "avdec_h264" => "libav (gst-libav)",
            "avdec_h265" => "libav (gst-libav)",
            "videotestsrc" => "videotestsrc (gst-plugins-base)",
            "imagefreeze" => "imagefreeze (gst-plugins-good)",
            "audiotestsrc" => "audiotestsrc (gst-plugins-base)",
            "decodebin" => "playback (gst-plugins-good)",
            _ => "Unknown",
        };
        format!(
            "Missing required gstreamer plugin `{}` for `{}` element",
            plugin, kind
        )
    })
}

#[allow(dead_code)]
fn make_dbl_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    // queue.set_property(
    //     "max-size-time",
    //     std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
    //         .unwrap_or(0),
    // );

    let queue2 = make_element("queue2", &format!("queue2_{}", name))?;
    queue2.set_property("max-size-bytes", buffer_size * 2u32 / 3u32);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue2.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    queue2.set_property("use-buffering", false);

    let bin = gstreamer::Bin::builder().name(name).build();
    bin.add_many([&queue, &queue2])?;
    Element::link_many([&queue, &queue2])?;

    let pad = queue
        .static_pad("sink")
        .expect("Failed to get a static pad from queue.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let pad = queue2
        .static_pad("src")
        .expect("Failed to get a static pad from queue2.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let bin = bin
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot convert bin"))?;
    Ok(bin)
}

fn make_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    Ok(queue)
}

fn buffer_size(bitrate: u32) -> u32 {
    // 0.1 seconds (according to bitrate) or 4kb what ever is larger
    std::cmp::max(bitrate * 2 / 8u32, 4u32 * 1024u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn us(d: Duration) -> u128 {
        d.as_micros()
    }

    #[test]
    fn video_uses_camera_frame_clock() {
        // Camera claims 20fps but sends 10fps: timestamps follow the camera
        let mut clock = MediaClock::with_fps(20);
        assert_eq!(us(clock.next_video(1_000_000)), 0);
        assert_eq!(us(clock.next_video(1_100_000)), 100_000);
        assert_eq!(us(clock.next_video(1_200_000)), 200_000);
    }

    #[test]
    fn video_survives_u32_wrap() {
        let mut clock = MediaClock::with_fps(10);
        let start = u32::MAX - 50_000;
        assert_eq!(us(clock.next_video(start)), 0);
        // Wraps past u32::MAX (~71.6 min of camera time)
        assert_eq!(us(clock.next_video(start.wrapping_add(100_000))), 100_000);
        assert_eq!(us(clock.next_video(start.wrapping_add(200_000))), 200_000);
    }

    #[test]
    fn video_jumps_use_nominal_frame_time() {
        let mut clock = MediaClock::with_fps(10);
        clock.next_video(5_000_000);
        // Repeated timestamp
        assert_eq!(us(clock.next_video(5_000_000)), 100_000);
        // Large forward jump (e.g. after a reconnect)
        assert_eq!(us(clock.next_video(60_000_000)), 200_000);
        // Backwards jump (wrapping_sub gives a huge delta)
        assert_eq!(us(clock.next_video(1_000_000)), 300_000);
        // Then normal again
        assert_eq!(us(clock.next_video(1_100_000)), 400_000);
    }

    #[test]
    fn video_timestamps_exceed_u32_micros() {
        // PTS must keep increasing past 71.6 minutes of stream time
        let mut clock = MediaClock::with_fps(10);
        let mut cam: u32 = 0;
        let mut last = Duration::ZERO;
        for _ in 0..50_000 {
            last = clock.next_video(cam);
            cam = cam.wrapping_add(100_000);
        }
        assert_eq!(us(last), 49_999 * 100_000);
        assert!(us(last) > u32::MAX as u128);
    }

    #[test]
    fn audio_advances_by_duration_with_fallback() {
        let mut clock = MediaClock::with_fps(10);
        assert_eq!(us(clock.next_audio(Some(64_000))), 0);
        assert_eq!(us(clock.next_audio(None)), 64_000);
        assert_eq!(us(clock.next_audio(Some(32_000))), 128_000);
        assert_eq!(us(clock.next_audio(None)), 160_000);
    }

    #[test]
    fn zero_fps_does_not_panic() {
        let mut clock = MediaClock::with_fps(0);
        clock.next_video(0);
        assert_eq!(us(clock.next_video(0)), 1_000_000);
    }
}
