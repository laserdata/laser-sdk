use crate::cursor::Cursor;
use crate::error::LaserError;
use crate::laser::Laser;
use laser_wire::change::ChangeRecord;
use laser_wire::framing::decode_named;

impl Laser {
    /// The change-feed accessor: [`ChangeRecord`]s the plane publishes after
    /// each committed projector batch for a binding that opted into `notify`.
    /// "Query after my data landed" becomes await-then-query instead of
    /// sleep-and-retry, and it composes with read-your-writes rather than
    /// replacing it. Gated on the `WATCH` capability: refused typed when the
    /// deployment does not publish the feed, so a consumer can never wait on
    /// a topic nothing writes to.
    pub fn watch(&self) -> Watch<'_> {
        Watch {
            laser: self,
            index: None,
        }
    }
}

/// A fluent change-feed read. Narrow with [`index`](Self::index), then drain
/// with [`records`](Self::records). Build it with [`Laser::watch`].
pub struct Watch<'a> {
    laser: &'a Laser,
    index: Option<String>,
}

impl<'a> Watch<'a> {
    /// Keep only advancements of this materialized index.
    #[must_use]
    pub fn index(mut self, index: impl Into<String>) -> Self {
        self.index = Some(index.into());
        self
    }

    /// Open the feed reader: the existing [`Cursor`] over the changes topic on
    /// the ops stream, decoding each record and applying the index filter
    /// client-side. No new consumption machinery, the feed is ordinary records
    /// consumed by offset.
    pub fn records(self) -> Result<WatchReader, LaserError> {
        if !self.laser.capabilities.watch {
            return Err(LaserError::unsupported_feature(
                "watch",
                "watch",
                "the change feed is not published by this deployment",
            ));
        }
        let cursor = self
            .laser
            .reader_on(self.laser.ops_stream(), self.laser.changes_topic())?;
        Ok(WatchReader {
            cursor,
            index: self.index,
        })
    }
}

/// A resumable change-feed reader. Each [`poll`](Self::poll) drains the
/// records that landed since the last one. Persist [`offsets`](Self::offsets)
/// and seed a fresh reader to resume across restarts, exactly like any
/// [`Cursor`].
pub struct WatchReader {
    cursor: Cursor,
    index: Option<String>,
}

impl WatchReader {
    /// Resume from a previous reader's offsets.
    #[must_use]
    pub fn from_offsets(mut self, offsets: Vec<u64>) -> Self {
        self.cursor = self.cursor.from_offsets(offsets);
        self
    }

    /// Drain the change records that landed since the last poll, filtered to
    /// the watched index when one was set. A record that does not decode as a
    /// [`ChangeRecord`] is skipped: the changes topic defaults to
    /// `laser_wire::topics::CHANGES_TOPIC` but is overridable
    /// ([`Laser::with_changes_topic`]), and a misbehaving deployment publishing
    /// other traffic on it must not wedge the reader either way.
    pub async fn poll(&mut self) -> Result<Vec<ChangeRecord>, LaserError> {
        let messages = self.cursor.poll().await?;
        Ok(messages
            .into_iter()
            .filter_map(|message| decode_named::<ChangeRecord>(&message.payload).ok())
            .filter(|record| {
                self.index
                    .as_deref()
                    .is_none_or(|index| record.index == index)
            })
            .collect())
    }

    /// The per-partition offsets consumed so far, the resume state.
    pub fn offsets(&self) -> &[u64] {
        self.cursor.offsets()
    }

    /// This reader as a [`Stream`](futures::Stream) of change records, one at a
    /// time, draining what landed since the last read and ending once caught up
    /// (the same shape as the async reader in the Python binding). Drive it with
    /// `futures::StreamExt`.
    pub fn stream(self) -> impl futures::Stream<Item = Result<ChangeRecord, LaserError>> {
        futures::stream::unfold(
            (self, std::collections::VecDeque::new(), false),
            |(mut reader, mut buffered, stop)| async move {
                if stop {
                    return None;
                }
                loop {
                    if let Some(record) = buffered.pop_front() {
                        return Some((Ok(record), (reader, buffered, false)));
                    }
                    match reader.poll().await {
                        Ok(batch) if batch.is_empty() => return None,
                        Ok(batch) => buffered.extend(batch),
                        Err(error) => return Some((Err(error), (reader, buffered, true))),
                    }
                }
            },
        )
    }
}
