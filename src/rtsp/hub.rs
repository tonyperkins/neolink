//! Shares a single camera stream between all RTSP clients of one stream kind
//!
//! Previously every RTSP client started its own camera stream. Cameras such as
//! the E1 Pro only support one live preview per stream per connection: starting
//! a second one silently freezes the first. That happens routinely when an RTSP
//! client (e.g. go2rtc) reconnects while its old session is still waiting to
//! time out, and the result is a stall/reconnect loop.
//!
//! The hub runs one camera stream while at least one client is subscribed and
//! fans the media out over a broadcast channel. A new client starts at the
//! next keyframe.
//!
//! Note: replaying a cached GOP to new clients was tried and removed. The burst
//! of frames arriving at once breaks consumers that timestamp by wall clock
//! (e.g. go2rtc with `-use_wallclock_as_timestamps 1`): they then report a
//! 90000 fps stream and downstream CFR transcodes duplicate frames endlessly.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use neolink_core::{bc_protocol::StreamKind, bcmedia::model::BcMedia};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::time::{interval, sleep, Duration, Instant};

use crate::common::NeoInstance;

/// If the camera sends nothing for this long while clients are watching (and
/// pausing is not enabled) the camera stream is restarted
const MEDIA_STALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Keep the camera stream running this long after the last client leaves, so a
/// reconnecting client (e.g. go2rtc) reuses it instead of restarting it
const LINGER: Duration = Duration::from_secs(10);
/// Time allowed for a dropped camera stream to finish stopping before a new one
/// is started. The stop is sent asynchronously when the stream is dropped and
/// waits up to 2s for the camera's reply; starting a new stream (same handle)
/// before that completes let the late stop kill the new stream.
const STOP_SETTLE: Duration = Duration::from_secs(3);

/// Frames buffered per client before it is considered lagging (~10s at 10fps + audio)
const CHANNEL_CAPACITY: usize = 256;
#[derive(Default)]
struct HubInner {
    tx: Option<broadcast::Sender<BcMedia>>,
    info: Option<BcMedia>,
}

impl HubInner {
    fn record(&mut self, media: &BcMedia) {
        if matches!(media, BcMedia::InfoV1(_) | BcMedia::InfoV2(_)) {
            self.info = Some(media.clone());
        }
    }

    fn reset(&mut self) {
        self.tx = None;
        self.info = None;
    }
}

#[derive(Clone)]
pub(super) struct StreamHub {
    camera: NeoInstance,
    stream: StreamKind,
    name: String,
    inner: Arc<Mutex<HubInner>>,
    /// Held by the producer for its lifetime (including STOP_SETTLE after its
    /// last camera stream) so camera streams never overlap
    producer_lock: Arc<tokio::sync::Mutex<()>>,
}

impl StreamHub {
    pub(super) fn new(camera: NeoInstance, stream: StreamKind, name: String) -> Self {
        Self {
            camera,
            stream,
            name,
            inner: Default::default(),
            producer_lock: Default::default(),
        }
    }

    /// Subscribe to the shared stream, starting the camera stream if needed.
    ///
    /// Must be called from within a tokio runtime.
    pub(super) fn subscribe(&self) -> HubFeed {
        let mut inner = self.inner.lock().expect("Hub lock poisoned");
        let tx = match inner.tx.as_ref() {
            Some(tx) => tx.clone(),
            None => {
                let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
                inner.tx = Some(tx.clone());
                let hub = self.clone();
                let producer_tx = tx.clone();
                tokio::task::spawn(async move { hub.produce(producer_tx).await });
                tx
            }
        };
        let rx = tx.subscribe();
        let mut initial = VecDeque::new();
        if let Some(info) = inner.info.as_ref() {
            initial.push_back(info.clone());
        }
        log::debug!(
            "{}::{}: Client subscribed ({} clients)",
            self.name,
            self.stream,
            tx.receiver_count(),
        );
        HubFeed {
            initial,
            rx,
            need_keyframe: true,
        }
    }

    /// Atomically decide whether the producer should stop. Done under the lock
    /// so that a concurrent `subscribe` either sees the running producer (and
    /// keeps it alive by subscribing) or sees it gone and starts a new one.
    fn stop_if_unused(&self, tx: &broadcast::Sender<BcMedia>) -> bool {
        let mut inner = self.inner.lock().expect("Hub lock poisoned");
        if tx.receiver_count() == 0 {
            inner.reset();
            true
        } else {
            false
        }
    }

    async fn produce(self, tx: broadcast::Sender<BcMedia>) {
        let (name, stream) = (self.name.clone(), self.stream);
        let producer_lock = self.producer_lock.clone();
        let _producer_guard = producer_lock.lock().await;
        log::info!("{name}::{stream}: Starting shared camera stream");
        let mut idle_since: Option<Instant> = None;
        'outer: loop {
            // Pausing legitimately stops media, so only watch for stalls without it
            let stall_watch = match self.camera.config().await {
                Ok(config) => !config.borrow().pause.on_motion,
                Err(_) => false,
            };
            match self.camera.stream_while_live(stream).await {
                Ok(mut media_rx) => {
                    // Wake periodically so we notice when all clients have left
                    // even if the camera is not sending (e.g. paused)
                    let mut idle = interval(Duration::from_secs(5));
                    let mut last_media = Instant::now();
                    loop {
                        tokio::select! {
                            media = media_rx.recv() => match media {
                                Some(media) => {
                                    last_media = Instant::now();
                                    let mut inner = self.inner.lock().expect("Hub lock poisoned");
                                    inner.record(&media);
                                    let _ = tx.send(media);
                                }
                                None => {
                                    log::info!("{name}::{stream}: Camera stream ended");
                                    break;
                                }
                            },
                            _ = idle.tick() => {
                                if stall_watch && last_media.elapsed() > MEDIA_STALL_TIMEOUT {
                                    log::warn!(
                                        "{name}::{stream}: No media from camera for {:?}, restarting camera stream",
                                        MEDIA_STALL_TIMEOUT
                                    );
                                    break;
                                }
                            }
                        }
                        if tx.receiver_count() == 0 {
                            let since = *idle_since.get_or_insert_with(Instant::now);
                            if since.elapsed() >= LINGER && self.stop_if_unused(&tx) {
                                break 'outer;
                            }
                        } else {
                            idle_since = None;
                        }
                    }
                }
                Err(e) => {
                    log::warn!("{name}::{stream}: Could not start camera stream: {e:?}");
                }
            }
            if self.stop_if_unused(&tx) {
                break;
            }
            sleep(STOP_SETTLE).await;
            if self.stop_if_unused(&tx) {
                break;
            }
            log::info!("{name}::{stream}: Restarting shared camera stream");
        }
        log::info!("{name}::{stream}: Stopped shared camera stream (no clients)");
        // Keep holding the producer lock while the camera stream finishes stopping
        sleep(STOP_SETTLE).await;
    }
}

/// One client's view of the shared stream
pub(super) struct HubFeed {
    initial: VecDeque<BcMedia>,
    rx: broadcast::Receiver<BcMedia>,
    /// After a gap (start or lag) skip P-frames until the next keyframe so the
    /// decoder is never fed frames that reference missing data
    need_keyframe: bool,
}

impl HubFeed {
    /// Returns true if the frame should be delivered
    fn accept(&mut self, media: &BcMedia) -> bool {
        match media {
            BcMedia::Iframe(_) => {
                self.need_keyframe = false;
                true
            }
            BcMedia::InfoV1(_) | BcMedia::InfoV2(_) => true,
            // Hold back audio as well as P-frames until the keyframe. The
            // factory learns the stream format from the first ~10 frames; if
            // those were all audio it concluded there was no video and served
            // the 20s "Stream not Ready" test pattern instead of the camera.
            _ => !self.need_keyframe,
        }
    }

    fn lagged(&mut self, n: u64) {
        log::warn!("RTSP client fell behind the camera stream, skipped {n} frames");
        self.need_keyframe = true;
    }

    pub(super) async fn recv(&mut self) -> Option<BcMedia> {
        loop {
            let media = match self.initial.pop_front() {
                Some(media) => media,
                None => match self.rx.recv().await {
                    Ok(media) => media,
                    Err(RecvError::Lagged(n)) => {
                        self.lagged(n);
                        continue;
                    }
                    Err(RecvError::Closed) => return None,
                },
            };
            if self.accept(&media) {
                return Some(media);
            }
        }
    }
}
