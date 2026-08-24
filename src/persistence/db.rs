//! Placeholder for SQLite persistence of download tasks and chunk progress.
//! Will be implemented to enable full resume across application restarts.

use anyhow::Result;

pub fn init_db(_path: &std::path::Path) -> Result<()> {
    // TODO: create tables for tasks, chunks, settings overrides
    Ok(())
}
