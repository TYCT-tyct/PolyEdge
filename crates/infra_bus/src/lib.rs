use anyhow::{anyhow, Result};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct RingBus<T: Clone + Send + Sync + 'static> {
    tx: broadcast::Sender<T>,
}

impl<T: Clone + Send + Sync + 'static> RingBus<T> {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(16));
        Self { tx }
    }

    pub fn publish(&self, event: T) -> Result<()> {
        match self.tx.send(event) {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!("bus publish failed: {err}")),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<T> {
        self.tx.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_subscribe_roundtrip() {
        let bus = RingBus::new(32);
        let mut rx = bus.subscribe();
        bus.publish(7u64).expect("publish");
        let v = rx.recv().await.expect("recv");
        assert_eq!(v, 7);
    }
}
