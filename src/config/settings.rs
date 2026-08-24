use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSettings {
    pub max_concurrent_tasks: usize,
    pub max_connections_per_task: usize,
    pub global_rate_limit_bps: u64,
    pub default_task_rate_limit_bps: u64,
    pub initial_chunk_size: u64,
    pub min_chunk_size: u64,
    pub max_chunk_size: u64,
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub user_agent: String,
    pub download_dir: PathBuf,
    pub enable_notifications: bool,
    pub on_complete_cmd: Option<String>,
    pub proxy: Option<String>,
    pub adaptive_chunking: bool,
    pub clipboard_poll_ms: u64,
    pub log_level: String,
    pub secure_temp_files: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        let download_dir = directories::UserDirs::new()
            .and_then(|u| u.download_dir().map(|p| p.to_path_buf()))
            .unwrap_or_else(|| PathBuf::from("./downloads"));
        Self {
            max_concurrent_tasks: 3,
            max_connections_per_task: 8,
            global_rate_limit_bps: 0,
            default_task_rate_limit_bps: 0,
            initial_chunk_size: 4 * 1024 * 1024,
            min_chunk_size: 512 * 1024,
            max_chunk_size: 32 * 1024 * 1024,
            max_retries: 5,
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
            user_agent: "SplitDownloader/1.0 (Rust)".to_string(),
            download_dir,
            enable_notifications: true,
            on_complete_cmd: None,
            proxy: None,
            adaptive_chunking: true,
            clipboard_poll_ms: 1500,
            log_level: "info".to_string(),
            secure_temp_files: true,
        }
    }
}

impl AppSettings {
    pub fn config_path() -> PathBuf {
        let proj = directories::ProjectDirs::from("com", "splitdownloader", "SplitDownloader")
            .expect("Failed to determine project dirs");
        let dir = proj.config_dir();
        std::fs::create_dir_all(dir).ok();
        dir.join("settings.toml")
    }

    pub fn load() -> Self {
        let path = Self::config_path();
        if path.exists() {
            match std::fs::read_to_string(&path) {
                Ok(content) => match toml::from_str(&content) {
                    Ok(s) => return s,
                    Err(e) => tracing::warn!("Failed to parse settings: {}", e),
                },
                Err(e) => tracing::warn!("Failed to read settings: {}", e),
            }
        }
        let defaults = Self::default();
        let _ = defaults.save();
        defaults
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let path = Self::config_path();
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}
