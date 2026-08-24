use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkState {
    Pending,
    Downloading,
    Completed,
    Failed,
    Verifying,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub id: Uuid,
    pub index: usize,
    pub start: u64,
    pub end: u64, // inclusive
    pub state: ChunkState,
    pub downloaded: u64,
    pub retries: u32,
    pub last_error: Option<String>,
    /// Current speed for this chunk (bytes/sec) - not persisted
    #[serde(skip)]
    pub current_speed: f64,
}

impl Chunk {
    pub fn new(index: usize, start: u64, end: u64) -> Self {
        Self {
            id: Uuid::new_v4(),
            index,
            start,
            end,
            state: ChunkState::Pending,
            downloaded: 0,
            retries: 0,
            last_error: None,
            current_speed: 0.0,
        }
    }

    pub fn size(&self) -> u64 {
        self.end.saturating_sub(self.start) + 1
    }

    pub fn remaining(&self) -> u64 {
        self.size().saturating_sub(self.downloaded)
    }

    pub fn progress(&self) -> f32 {
        if self.size() == 0 {
            1.0
        } else {
            self.downloaded as f32 / self.size() as f32
        }
    }

    pub fn is_done(&self) -> bool {
        matches!(self.state, ChunkState::Completed)
    }
}
