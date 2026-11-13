//! Subscriber concurrency limiting for the streaming services.
//!
//! Each gRPC stream is capped by a [`Semaphore`]: [`SubscriberLimit::acquire`]
//! takes a permit when a client subscribes and hands back a [`SubscriberGuard`]
//! that holds it for the lifetime of the response stream, releasing it when the
//! stream is dropped — i.e. when the client disconnects. Mirrors
//! nordpool-stream-server's `subscriber` module. Each feed carries a stable
//! `feed` name so the `powerbot.grpc.subscribers` gauge (live count) and the
//! `powerbot.grpc.subscribers_dropped` counter are tagged by it (docs/design.md).

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

/// Holds one live subscriber's concurrency permit for its stream's lifetime.
/// Move it onto the response stream so the permit is released (and the slot
/// freed for another client) exactly when tonic drops the stream on disconnect.
/// Dropping it also decrements the live-subscriber gauge for its feed.
pub(crate) struct SubscriberGuard {
    _permit: OwnedSemaphorePermit,
    feed: &'static str,
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        crate::metrics::subscriber_removed(self.feed);
    }
}

/// Bounds concurrent subscribers on one streaming feed with a [`Semaphore`] plus
/// the message returned when the cap is reached. A feed holds one and calls
/// [`acquire`](Self::acquire) per subscribe. `feed` is the metric tag.
pub(crate) struct SubscriberLimit {
    semaphore: Arc<Semaphore>,
    feed: &'static str,
    limit_message: &'static str,
}

impl SubscriberLimit {
    pub(crate) fn new(
        max_subscribers: usize,
        feed: &'static str,
        limit_message: &'static str,
    ) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_subscribers)),
            feed,
            limit_message,
        }
    }

    /// Take a subscriber slot for the lifetime of a stream, or reject the
    /// subscribe with `RESOURCE_EXHAUSTED` when the cap is reached. The returned
    /// [`SubscriberGuard`] must be moved onto the response stream so the permit
    /// is held until the client disconnects. Bumps the live-subscriber gauge on
    /// success; counts a `limit` drop on rejection.
    // tonic's `Status` is large by design, so the `Err` variant is large here.
    #[allow(clippy::result_large_err)]
    pub(crate) fn acquire(&self) -> Result<SubscriberGuard, Status> {
        let permit = Arc::clone(&self.semaphore)
            .try_acquire_owned()
            .map_err(|_| {
                crate::metrics::subscriber_dropped(self.feed, "limit");
                Status::resource_exhausted(self.limit_message)
            })?;
        crate::metrics::subscriber_added(self.feed);
        Ok(SubscriberGuard {
            _permit: permit,
            feed: self.feed,
        })
    }
}
