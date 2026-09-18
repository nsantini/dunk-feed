//! Jetstream v2 client, TECH-DESIGN section 5.1. Connects to a Jetstream v2
//! host, fetches and caches the zstd dictionary, decodes compressed frames,
//! and hands a caller typed events through `async fn next(&mut self) ->
//! Result<Event, JetstreamError>`. Hand-written against the v2
//! `subscribeEvents` endpoint, because section 1 found the Rust Jetstream
//! crates too thin or too stale to trust. This story lands the wire types
//! in `event`; `client` gains its connect, decode and reconnect loop over
//! the slices that follow. The ingest task of story 06 is the first caller.

#![allow(dead_code)] // ingest (story 06) now calls this module; dictionary_id and time_micros still await a caller: story 07 or 08.

pub mod client;
pub mod event;

pub use client::{JetstreamClient, JetstreamError};
pub use event::Event;
