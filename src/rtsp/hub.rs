//! Shares a single camera stream between all RTSP clients of one stream kind
//!
//! Previously every RTSP client started its own camera stream. Cameras such as
//! the E1 Pro only support one live preview per stream per connection: starting
//! a second one silently freezes the first. That happens routinely when an RTSP
//! client (e.g. go2rtc) reconnects while its old session is still waiting to
//! time out, and the result is a stall/reconnect loop.
//!
//! The hub runs one camera stream while at least one client is subscribed and
//! fans the media out over a broadcast channel. It also caches the current GOP
//! (everything since the last keyframe) so a new client can start decoding
//! immediately.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use neolink_core::{bc_protocol::StreamKind, bcmedia::model::BcMedia};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::time::{interval, sleep, Duration};

use crate::common::NeoInstance;

/// Frames buffered per client before it is considered lagging (~10s at 10fps + audio)
const CHANNEL_CAPACITY: usize = 256;
/// Bounds on the cached GOP. If the camera goes this long without a keyframe the
/// cache is dropped rather than allowed to grow.
const MAX_GOP_FRAMES: usize = 600;
const MAX_GOP_BYTES: usize = 16 * 1024 * 1024;

fn media_len(media: &BcMedia) -> usize {
    match media {
        BcMedia::Iframe(f) => f.data.len(),
        BcMedia::Pframe(f) => f.data.len(),
        BcMedia::Aac(a) => a.data.len(),
        BcMedia::Adpcm(a) => a.data.len(),
        BcMedia::InfoV1(_) | BcMedia::InfoV2(_) => 0,
    }
}

#[derive(Default)]
struct HubInner {
    tx: Option<broadcast::Sender<BcMedia>>,
    info: Option<BcMedia>,
    gop: Vec<BcMedia>,
    gop_bytes: usize,
}

impl HubInner {
    fn record(&mut self, media: &BcMedia) {
        match media {
            BcMedia::InfoV1(_) | BcMedia::InfoV2(_) => self.info = Some(media.clone()),
            BcMedia::Iframe(_) => {
                self.clear_gop();
                self.push(media);
            }
            _ if !self.gop.is_empty() => self.push(media),
            _ => {}
        }
    }

    fn push(&mut self, media: &BcMedia) {
        let len = media_len(media);
        if self.gop.len() >= MAX_GOP_FRAMES || self.gop_bytes + len > MAX_GOP_BYTES {
            log::debug!("GOP cache limit reached, dropping cache until next keyframe");
            self.clear_gop();
            return;
        }
        self.gop_bytes += len;
        self.gop.push(media.clone());
    }

    fn clear_gop(&mut self) {
        self.gop.clear();
        self.gop_bytes = 0;
    }

    fn reset(&mut self) {
        self.tx = None;
        self.info = None;
        self.clear_gop();
    }
}

#[derive(Clone)]
pub(super) struct StreamHub {
    camera: NeoInstance,
    stream: StreamKind,
    name: String,
    inner: Arc<Mutex<HubInner>>,
}

impl StreamHub {
    pub(super) fn new(camera: NeoInstance, stream: StreamKind, name: String) -> Self {
        Self {
            camera,
            stream,
            name,
            inner: Default::default(),
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
        initial.extend(inner.gop.iter().cloned());
        let has_keyframe = initial.iter().any(|m| matches!(m, BcMedia::Iframe(_)));
        log::debug!(
            "{}::{}: Client subscribed ({} clients, {} cached frames)",
            self.name,
            self.stream,
            tx.receiver_count(),
            initial.len()
        );
        HubFeed {
            initial,
            rx,
            need_keyframe: !has_keyframe,
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
        log::info!("{name}::{stream}: Starting shared camera stream");
        'outer: loop {
            match self.camera.stream_while_live(stream).await {
                Ok(mut media_rx) => {
                    // Wake periodically so we notice when all clients have left
                    // even if the camera is not sending (e.g. paused)
                    let mut idle = interval(Duration::from_secs(5));
                    loop {
                        tokio::select! {
                            media = media_rx.recv() => match media {
                                Some(media) => {
                                    let mut inner = self.inner.lock().expect("Hub lock poisoned");
                                    inner.record(&media);
                                    let _ = tx.send(media);
                                }
                                None => {
                                    log::info!("{name}::{stream}: Camera stream ended");
                                    break;
                                }
                            },
                            _ = idle.tick() => {}
                        }
                        if self.stop_if_unused(&tx) {
                            break 'outer;
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
            // Clients remain: the new camera stream starts a fresh GOP
            self.inner.lock().expect("Hub lock poisoned").clear_gop();
            sleep(Duration::from_secs(1)).await;
            if self.stop_if_unused(&tx) {
                break;
            }
            log::info!("{name}::{stream}: Restarting shared camera stream");
        }
        log::info!("{name}::{stream}: Stopped shared camera stream (no clients)");
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
            BcMedia::Pframe(_) => !self.need_keyframe,
            _ => true,
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
