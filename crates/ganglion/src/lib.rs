//! Ganglion umbrella crate.
//!
//! Re-exports the stable Ganglion surface (the consensus runtime: nodes, the TCP
//! server/network, log store, config, and the embedded `openraft` types) so that
//! depending crates bind to `ganglion` rather than reaching into the backend crate
//! `ganglion-openraft` directly. The consensus backend stays an internal detail.
pub use ganglion_openraft::*;
