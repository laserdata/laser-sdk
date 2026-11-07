use crate::error::LaserError;
use crate::filters::reader::{FilteredReader, MatchedPage, MatchedRecord};
use futures::Stream;
use futures::future::BoxFuture;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A [`FilteredReader`] as a [`Stream`] of matching records, one at a time.
/// Build it with [`FilteredReader::stream`]. It acknowledges through the
/// same handle, so a loop over `records.next().await` acknowledges each
/// record before it asks for the next one. The stream never ends on its own:
/// it waits while nothing is new, so wrap a read in a timeout to bound it.
pub struct FilteredRecords {
    reader: Option<FilteredReader>,
    reading: Option<BoxFuture<'static, (FilteredReader, Result<MatchedRecord, LaserError>)>>,
    // A record a read finished with while an acknowledgment waited for it.
    finished: VecDeque<Result<MatchedRecord, LaserError>>,
}

impl FilteredReader {
    /// This reader as a [`Stream`] of matching records that still acknowledges
    /// them, the async iteration Python and TypeScript readers offer.
    pub fn stream(self) -> FilteredRecords {
        FilteredRecords {
            reader: Some(self),
            reading: None,
            finished: VecDeque::new(),
        }
    }
}

impl FilteredRecords {
    /// Mark one record handled, and store the progress this completes.
    pub async fn ack(&mut self, record: &MatchedRecord) -> Result<(), LaserError> {
        self.reader().await?.ack(record).await
    }

    /// Mark every record up to and including `record` on its partition
    /// handled, and store the progress this completes.
    pub async fn ack_through(&mut self, record: &MatchedRecord) -> Result<(), LaserError> {
        self.reader().await?.ack_through(record).await
    }

    /// Mark every record of `page` handled, and store the progress this completes.
    pub async fn ack_page(&mut self, page: &MatchedPage) -> Result<(), LaserError> {
        self.reader().await?.ack_page(page).await
    }

    /// Store completed progress and leave the group, like
    /// [`FilteredReader::close`], once the read in flight finished. Records
    /// that were not acknowledged are read again by the next reader.
    pub async fn close(mut self) -> Result<(), LaserError> {
        self.reader().await?;
        self.reader.take().ok_or_else(stream_failed)?.close().await
    }

    // The reader once no read is in flight. A read that was started keeps
    // its record for the next poll of the stream, and stays in flight when
    // this call is cancelled.
    async fn reader(&mut self) -> Result<&mut FilteredReader, LaserError> {
        if let Some(reading) = self.reading.as_mut() {
            let (reader, record) = reading.await;
            self.reading = None;
            self.reader = Some(reader);
            self.finished.push_back(record);
        }
        self.reader.as_mut().ok_or_else(stream_failed)
    }
}

impl Stream for FilteredRecords {
    type Item = Result<MatchedRecord, LaserError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(record) = self.finished.pop_front() {
            return Poll::Ready(Some(record));
        }
        if self.reading.is_none() {
            let Some(mut reader) = self.reader.take() else {
                return Poll::Ready(Some(Err(stream_failed())));
            };
            self.reading = Some(Box::pin(async move {
                let record = reader.next_record().await;
                (reader, record)
            }));
        }
        let Some(reading) = self.reading.as_mut() else {
            return Poll::Ready(Some(Err(stream_failed())));
        };
        match reading.as_mut().poll(context) {
            Poll::Ready((reader, record)) => {
                self.reading = None;
                self.reader = Some(reader);
                Poll::Ready(Some(record))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// Only a read that panicked leaves the stream without its reader.
fn stream_failed() -> LaserError {
    LaserError::Invalid("the filtered record stream lost its reader to a failed read".to_owned())
}
