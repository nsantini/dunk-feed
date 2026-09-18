//! Test-only helpers shared by every `store` submodule's tests (round 1
//! finding 9). Every module under `src/store/` used to define its own copy
//! of `migrated_conn`; this is the one copy.

use rusqlite::Connection;

use crate::store::schema;

/// An in-memory connection migrated to `schema::CURRENT_VERSION`, with
/// `foreign_keys` on so a test that relies on the `feed` -> `pairs` foreign
/// key sees it enforced.
pub(crate) fn migrated_conn() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
    schema::migrate(&conn).unwrap();
    conn
}
