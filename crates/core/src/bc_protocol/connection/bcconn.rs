use super::BcSubscription;
use crate::{bc::model::*, Error, Result};
use futures::future::BoxFuture;
use futures::sink::{Sink, SinkExt};
use futures::stream::{Stream, StreamExt};
use log::*;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, error::TrySendError, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use tokio::{sync::RwLock, task::JoinSet};

type MsgHandler = dyn 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>;

#[derive(Default)]
struct Subscriber {
    /// Subscribers based on their ID and their num
    /// First filtered by ID then number
    /// If num is None it will be upgraded to a Some based on the number the
    /// camera assigns
    num: BTreeMap<u32, BTreeMap<Option<u16>, Sender<Result<Bc>>>>,
    /// Subscribers based on their ID
    id: BTreeMap<u32, Arc<MsgHandler>>,
}

pub(crate) type BcConnSink = Box<dyn Sink<Bc, Error = Error> + Send + Sync + Unpin>;
pub(crate) type BcConnSource = Box<dyn Stream<Item = Result<Bc>> + Send + Sync + Unpin>;

/// A shareable connection to a camera.  Handles serialization of messages.  To send/receive, call
/// .[subscribe()] with a message number.  You can use the BcSubscription to send or receive only
/// messages with that number; each incoming message is routed to its appropriate subscriber.
///
/// There can be only one subscriber per kind of message at a time.
pub struct BcConnection {
    sink: Sender<Result<Bc>>,
    poll_commander: Sender<PollCommand>,
    rx_thread: RwLock<JoinSet<Result<()>>>,
    cancel: CancellationToken,
}

impl BcConnection {
    pub async fn new(mut sink: BcConnSink, mut source: BcConnSource) -> Result<BcConnection> {
        let (sinker, sinker_rx) = channel::<Result<Bc>>(100);
        let cancel = CancellationToken::new();

        let (poll_commander, poll_commanded) = channel(200);
        let mut poller = Poller {
            subscribers: Default::default(),
            sink: sinker.clone(),
            reciever: ReceiverStream::new(poll_commanded),
        };

        let mut rx_thread = JoinSet::<Result<()>>::new();
        let thread_poll_commander = poll_commander.clone();
        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => {
                    Result::Ok(())
                },
                v = async {
                    let sender = thread_poll_commander;
                    while let Some(bc) = source.next().await {
                        sender.send(PollCommand::Bc(Box::new(bc))).await?;
                    }
                    Result::Ok(())
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    let mut stream = ReceiverStream::new(sinker_rx);
                    while let Some(packet) = stream.next().await {
                        sink.send(packet?).await?;
                    }
                    Ok(())
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                // `run` only returns once polling is over: with an error, or
                // with `Ok` once every command sender is gone. Calling it again
                // after `Ok` returned immediately, forever, pinning a core.
                // (Same fix as vertoforce/neolink@e4cb31d and
                // victorpicaud/neolink@1e6ba5b.)
                v = poller.run() => {
                    trace!("Polling has ended");
                    v
                }
            }
        });

        Ok(BcConnection {
            sink: sinker,
            poll_commander,
            rx_thread: RwLock::new(rx_thread),
            cancel,
        })
    }

    pub(super) async fn send(&self, bc: Bc) -> crate::Result<()> {
        self.sink.send(Ok(bc)).await?;
        Ok(())
    }

    pub async fn subscribe(&self, msg_id: u32, msg_num: u16) -> Result<BcSubscription> {
        let (tx, rx) = channel(100);
        self.poll_commander
            .send(PollCommand::AddSubscriber(msg_id, Some(msg_num), tx))
            .await?;
        Ok(BcSubscription::new(rx, Some(msg_num as u32), self))
    }

    /// Some messages are initiated by the camera. This creates a handler for them
    /// It requires a closure that will be used to handle the message
    /// and return either None or Some(Bc) reply
    pub async fn handle_msg<T>(&self, msg_id: u32, handler: T) -> Result<()>
    where
        T: 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>,
    {
        self.poll_commander
            .send(PollCommand::AddHandler(msg_id, Arc::new(handler)))
            .await?;
        Ok(())
    }

    /// Stop a message handler created using [`handle_msg`]
    #[allow(dead_code)] // Currently unused but added for future use
    pub async fn unhandle_msg(&self, msg_id: u32) -> Result<()> {
        self.poll_commander
            .send(PollCommand::RemoveHandler(msg_id))
            .await?;
        Ok(())
    }

    /// Some times we want to wait for a reply on a new message ID
    /// to do this we wait for the next packet with a certain ID
    /// grab it's message ID and then subscribe to that ID
    ///
    /// The command Snap that grabs a jpeg payload is an example of this
    ///
    /// This function creates a temporary handle to grab this single message
    pub async fn subscribe_to_id(&self, msg_id: u32) -> Result<BcSubscription> {
        let (tx, rx) = channel(100);
        self.poll_commander
            .send(PollCommand::AddSubscriber(msg_id, None, tx))
            .await?;
        Ok(BcSubscription::new(rx, None, self))
    }

    pub(crate) async fn join(&self) -> Result<()> {
        let mut locked_threads = self.rx_thread.write().await;
        while let Some(res) = locked_threads.join_next().await {
            match res {
                Err(e) => {
                    locked_threads.abort_all();
                    return Err(e.into());
                }
                Ok(Err(e)) => {
                    locked_threads.abort_all();
                    return Err(e);
                }
                Ok(Ok(())) => {}
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.poll_commander.send(PollCommand::Disconnect).await;
        self.cancel.cancel();
        let mut locked_threads = self.rx_thread.write().await;
        while locked_threads.join_next().await.is_some() {}
        Ok(())
    }
}

impl Drop for BcConnection {
    fn drop(&mut self) {
        log::trace!("Drop BcConnection");
        self.cancel.cancel();

        let poll_commander = self.poll_commander.clone();
        let _gt = tokio::runtime::Handle::current().enter();
        let mut threads = std::mem::take(&mut self.rx_thread);
        tokio::task::spawn(async move {
            let _ = poll_commander.send(PollCommand::Disconnect).await;
            let locked_threads = threads.get_mut();
            while locked_threads.join_next().await.is_some() {}
            log::trace!("Dropped BcConnection");
        });
    }
}

enum PollCommand {
    Bc(Box<Result<Bc>>),
    AddHandler(u32, Arc<MsgHandler>),
    RemoveHandler(u32),
    AddSubscriber(u32, Option<u16>, Sender<Result<Bc>>),
    Disconnect,
}

impl std::fmt::Debug for PollCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollCommand::Bc(_) => f.write_str("PollCommand::Bc"),
            PollCommand::AddHandler(_, _) => f.write_str("PollCommand::AddHandler"),
            PollCommand::RemoveHandler(_) => f.write_str("PollCommand::RemoveHandler"),
            PollCommand::AddSubscriber(_, _, _) => f.write_str("PollCommand::AddSubscriber"),
            PollCommand::Disconnect => f.write_str("PollCommand::Disconnect"),
        }
    }
}

struct Poller {
    subscribers: Subscriber,
    sink: Sender<Result<Bc>>,
    reciever: ReceiverStream<PollCommand>,
}

impl Poller {
    async fn run(&mut self) -> Result<()> {
        let cancel = CancellationToken::new();
        let _dropguard = cancel.clone().drop_guard();
        while let Some(command) = self.reciever.next().await {
            // Clean Up subscribers
            self.subscribers
                .num
                .iter_mut()
                .for_each(|(_, channels)| channels.retain(|_, channel| !channel.is_closed()));
            self.subscribers
                .num
                .retain(|_, channels| !channels.is_empty());
            // Handle the command
            match command {
                PollCommand::Bc(boxed_response) => {
                    match *boxed_response {
                        Ok(response) => {
                            let msg_id = response.meta.msg_id;
                            let msg_num = response.meta.msg_num;
                            log::trace!(
                                "Looking for ID: {} with num: {}, in {:?} and {:?}",
                                msg_id,
                                msg_num,
                                self.subscribers.id.keys().to_owned(),
                                self.subscribers
                                    .num
                                    .iter()
                                    .map(|(k, v)| (k, v.keys()))
                                    .collect::<Vec<_>>(),
                            );
                            match (
                                self.subscribers.id.get(&msg_id),
                                self.subscribers.num.get_mut(&msg_id), // Both filter first on ID
                            ) {
                                (Some(occ), _) => {
                                    log::trace!("Calling ID callback");
                                    let occ = occ.clone();
                                    let sink = self.sink.clone();
                                    // Move this on another thread coz I have NO idea
                                    // how long the callback will run for
                                    // and we must NOT hang
                                    let cancel = cancel.clone();
                                    tokio::task::spawn(async move {
                                        tokio::select! {
                                            _ = cancel.cancelled() => Result::Ok(()),
                                            v = occ(&response) => {
                                                if let Some(reply) = v {
                                                    assert!(reply.meta.msg_num == response.meta.msg_num);
                                                    sink.send(Ok(reply)).await?;
                                                }
                                                Result::Ok(())
                                            }
                                        }
                                    });
                                    log::trace!("Called ID callback");
                                }
                                (None, Some(occ)) => {
                                    let sender = if let Some(sender) =
                                        occ.get(&Some(msg_num)).filter(|a| !a.is_closed()).cloned()
                                    {
                                        // Connection with id exists and is not closed
                                        Some(sender)
                                    } else if let Some(sender) = occ.get(&None).cloned() {
                                        // Upgrade a None to a known MsgID
                                        occ.remove(&None);
                                        occ.insert(Some(msg_num), sender.clone());
                                        Some(sender)
                                    } else if occ
                                        .get(&Some(msg_num))
                                        .map(|a| a.is_closed())
                                        .unwrap_or(false)
                                    {
                                        // Connection is closed and there is no None to replace it
                                        // Remove it for cleanup and report no sender
                                        occ.remove(&Some(msg_num));
                                        None
                                    } else {
                                        None
                                    };
                                    if let Some(sender) = sender {
                                        // Never wait on a subscriber here. This loop routes
                                        // every message on the connection, ping replies
                                        // included, so one full queue (e.g. a video stream
                                        // whose consumer stalled) used to stop them all
                                        // until the camera dropped the connection. A
                                        // subscriber that falls this far behind has its
                                        // subscription ended instead: it receives what is
                                        // queued and then an error, so a video stream
                                        // restarts cleanly rather than continuing with a
                                        // gap. (The same problem was fixed in upstream
                                        // PR #399 and in several forks.)
                                        match sender.try_send(Ok(response)) {
                                            Ok(()) => {}
                                            Err(TrySendError::Full(_)) => {
                                                warn!(
                                                    "Subscriber to message {} (num {}) fell {} messages behind, ending its subscription",
                                                    msg_id,
                                                    msg_num,
                                                    sender.max_capacity()
                                                );
                                                occ.remove(&Some(msg_num));
                                            }
                                            Err(TrySendError::Closed(_)) => {
                                                occ.remove(&Some(msg_num));
                                            }
                                        }
                                    } else {
                                        trace!(
                                            "Ignoring uninteresting message id {} (number: {})",
                                            msg_id,
                                            msg_num
                                        );
                                        trace!("Contents: {:?}", response);
                                    }
                                }
                                (None, None) => {
                                    trace!(
                                        "Ignoring uninteresting message id {} (number: {})",
                                        msg_id,
                                        msg_num
                                    );
                                    trace!("Contents: {:?}", response);
                                }
                            }
                        }
                        Err(e) => {
                            // Best effort and never waiting: a subscriber whose queue
                            // is full still sees its channel close when it is cleared
                            for sub in self.subscribers.num.values() {
                                for sender in sub.values() {
                                    let _ = sender.try_send(Err(e.clone()));
                                }
                            }
                            self.subscribers.num.clear();
                            self.subscribers.id.clear();
                            return Err(e);
                        }
                    }
                }
                PollCommand::AddHandler(msg_id, handler) => {
                    match self.subscribers.id.entry(msg_id) {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(handler);
                        }
                        Entry::Occupied(_) => {
                            return Err(Error::SimultaneousSubscriptionId { msg_id });
                        }
                    };
                }
                PollCommand::RemoveHandler(msg_id) => {
                    self.subscribers.id.remove(&msg_id);
                }
                PollCommand::AddSubscriber(msg_id, msg_num, tx) => {
                    match self
                        .subscribers
                        .num
                        .entry(msg_id)
                        .or_default()
                        .entry(msg_num)
                    {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(tx);
                        }
                        Entry::Occupied(mut occ_entry) => {
                            if occ_entry.get().is_closed() {
                                occ_entry.insert(tx);
                            } else {
                                // log::error!("Failed to subscribe in bcconn to {:?} for {:?}", msg_num, msg_id);
                                let _ = tx
                                    .send(Err(Error::SimultaneousSubscription { msg_num }))
                                    .await;
                            }
                        }
                    };
                }
                PollCommand::Disconnect => {
                    return Err(Error::ConnectionShutdown);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn msg(msg_id: u32, msg_num: u16) -> Bc {
        Bc::new_from_meta(BcMeta {
            msg_id,
            channel_id: 0,
            stream_type: 0,
            response_code: 200,
            msg_num,
            class: 0x0000,
        })
    }

    fn poller() -> (Poller, Sender<PollCommand>) {
        let (sink, _) = channel::<Result<Bc>>(1);
        let (commander, commanded) = channel::<PollCommand>(100);
        (
            Poller {
                subscribers: Default::default(),
                sink,
                reciever: ReceiverStream::new(commanded),
            },
            commander,
        )
    }

    /// `Poller::run` returns `Ok` as soon as its commands close, so it must be
    /// called once and not in a loop
    #[tokio::test]
    async fn poller_run_returns_once_commands_close() {
        let (mut poller, commander) = poller();
        drop(commander);
        let result = tokio::time::timeout(Duration::from_secs(1), poller.run())
            .await
            .expect("Poller::run did not return after its commands closed");
        assert!(result.is_ok());
    }

    /// A subscriber that stops reading must not hold up other subscribers
    /// (e.g. ping replies) and has its own subscription ended instead
    #[tokio::test]
    async fn stalled_subscriber_does_not_block_others() {
        let (mut poller, commander) = poller();
        let (stalled_tx, mut stalled_rx) = channel(2);
        let (other_tx, mut other_rx) = channel(10);
        commander
            .send(PollCommand::AddSubscriber(3, Some(1), stalled_tx))
            .await
            .unwrap();
        commander
            .send(PollCommand::AddSubscriber(93, Some(2), other_tx))
            .await
            .unwrap();
        // Overflow the stalled subscriber, then message the other one
        for _ in 0..5 {
            commander
                .send(PollCommand::Bc(Box::new(Ok(msg(3, 1)))))
                .await
                .unwrap();
        }
        commander
            .send(PollCommand::Bc(Box::new(Ok(msg(93, 2)))))
            .await
            .unwrap();
        drop(commander);

        tokio::time::timeout(Duration::from_secs(1), poller.run())
            .await
            .expect("Poller blocked on a stalled subscriber")
            .unwrap();

        let delivered = other_rx.recv().await.unwrap().unwrap();
        assert_eq!(delivered.meta.msg_id, 93);
        // The stalled subscriber gets what fit in its queue, then its channel ends
        assert!(stalled_rx.recv().await.unwrap().is_ok());
        assert!(stalled_rx.recv().await.unwrap().is_ok());
        assert!(stalled_rx.recv().await.is_none());
    }
}
