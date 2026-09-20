//! HTTP serving for the AT Protocol feed generator, story 08. This slice
//! (1.0) declares only `cursor`, the pure pagination helper; the router,
//! `AppState`, and the routes themselves land in slices 2.0 and 3.0.

pub mod cursor;
