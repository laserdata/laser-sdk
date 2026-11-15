use crate::error::LaserError;
use crate::laser::Laser;
use iggy::prelude::{StreamClient, StreamCreateOptions};
use std::sync::atomic::{AtomicBool, Ordering};

impl Laser {
    /// Create a uniquely named stream for one program run, `{prefix}-` and
    /// eight lowercase hex characters, and return it with a handle whose
    /// default stream it is. A stream of that name that already exists is
    /// refused, never reused. Call [`ScratchStream::delete`] when the run
    /// ends. Dropping the handle deletes it too, best effort.
    pub async fn scratch_stream(
        &self,
        prefix: impl AsRef<str>,
    ) -> Result<ScratchStream, LaserError> {
        let random = ulid::Ulid::generate().random() & u128::from(u32::MAX);
        let name = format!("{}-{random:08x}", prefix.as_ref());
        self.client()
            .create_stream(&name, &StreamCreateOptions::default())
            .await?;
        Ok(ScratchStream {
            laser: self.with_default_stream(name.clone()),
            name,
            deleted: AtomicBool::new(false),
        })
    }
}

/// A stream that lives for one program run, built with
/// [`Laser::scratch_stream`]. [`laser`](Self::laser) addresses it as the
/// default stream and [`delete`](Self::delete) removes it with every topic and
/// message in it.
pub struct ScratchStream {
    laser: Laser,
    name: String,
    deleted: AtomicBool,
}

impl ScratchStream {
    /// The stream's unique name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A handle on the same connection whose default stream is this one.
    pub fn laser(&self) -> &Laser {
        &self.laser
    }

    /// Delete the stream. Idempotent: a stream already gone is fine.
    pub async fn delete(&self) -> Result<(), LaserError> {
        crate::laser::delete_stream(&self.laser, &self.name).await?;
        self.deleted.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for ScratchStream {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::Acquire) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let laser = self.laser.clone();
        let name = std::mem::take(&mut self.name);
        runtime.spawn(async move {
            if let Err(error) = crate::laser::delete_stream(&laser, &name).await {
                tracing::warn!(stream = %name, %error, "Deleting scratch stream {name} on drop failed");
            }
        });
    }
}
