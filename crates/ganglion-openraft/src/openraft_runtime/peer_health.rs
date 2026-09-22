//! Local transport observations for application failure-detection policy.
//! These are suspicion signals only; they grant no Raft or application authority.
use std::{collections::BTreeMap, io, time::Instant};
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct PeerTransportFailure {
    pub since: Instant,
    pub latest: Instant,
    pub attempts: u64,
    pub kind: io::ErrorKind,
}

#[derive(Debug, Clone)]
pub(crate) struct PeerHealth(watch::Sender<BTreeMap<u64, PeerTransportFailure>>);

impl Default for PeerHealth {
    fn default() -> Self {
        Self(watch::channel(BTreeMap::new()).0)
    }
}

impl PeerHealth {
    pub fn subscribe(&self) -> watch::Receiver<BTreeMap<u64, PeerTransportFailure>> {
        self.0.subscribe()
    }

    pub fn observe<T>(&self, peer: u64, outcome: &io::Result<T>) {
        let kind = match outcome {
            Ok(_) => None,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::UnexpectedEof
                ) =>
            {
                Some(error.kind())
            }
            // Timeouts, protocol/TLS failures and cancelled RPCs do not supply
            // explicit process-loss evidence. Heartbeat expiry remains available.
            Err(_) => return,
        };
        self.record(peer, kind, Instant::now());
    }

    fn record(&self, peer: u64, kind: Option<io::ErrorKind>, now: Instant) {
        self.0.send_if_modified(|failures| {
            match kind {
                None => failures.remove(&peer).is_some(),
                Some(kind) => {
                    // Bound observations even across repeated membership changes.
                    if !failures.contains_key(&peer) && failures.len() >= 256 {
                        return false;
                    }
                    let failure = failures.entry(peer).or_insert(PeerTransportFailure {
                        since: now,
                        latest: now,
                        attempts: 0,
                        kind,
                    });
                    failure.latest = now;
                    failure.attempts = failure.attempts.saturating_add(1);
                    failure.kind = kind;
                    true
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reconnect_success_clears_suspicion_and_restarts_the_grace() {
        let health = PeerHealth::default();
        let view = health.subscribe();
        let first = Instant::now();
        health.record(2, Some(io::ErrorKind::ConnectionReset), first);
        health.record(
            2,
            Some(io::ErrorKind::ConnectionRefused),
            first + std::time::Duration::from_secs(1),
        );
        assert_eq!(view.borrow()[&2].attempts, 2);
        assert_eq!(view.borrow()[&2].since, first);
        health.record(2, None, first);
        assert!(view.borrow().is_empty());
        let later = first + std::time::Duration::from_secs(2);
        health.record(2, Some(io::ErrorKind::ConnectionRefused), later);
        assert_eq!(view.borrow()[&2].since, later);
        assert_eq!(view.borrow()[&2].attempts, 1);
    }
    #[test]
    fn timeout_and_protocol_errors_do_not_start_suspicion() {
        let health = PeerHealth::default();
        for kind in [
            io::ErrorKind::TimedOut,
            io::ErrorKind::InvalidData,
            io::ErrorKind::PermissionDenied,
        ] {
            health.observe::<()>(2, &Err(io::Error::from(kind)));
        }
        assert!(health.subscribe().borrow().is_empty());
    }
}
