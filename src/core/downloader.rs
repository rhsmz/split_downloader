use crate::config::AppSettings;
use crate::core::chunk::{Chunk, ChunkState};
use crate::core::rate_limiter::BandwidthLimiter;
use crate::core::retry::RetryPolicy;
use crate::util::hash::{HashAlgo, verify_file};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::{
    header::{HeaderMap, COOKIE, RANGE, USER_AGENT},
    Client,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Critical = 0,
    High = 1,
    Normal = 2,
}

impl Default for TaskPriority {
    fn default() -> Self {
        TaskPriority::Normal
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    Queued,
    Preparing,
    Downloading,
    Paused,
    Verifying,
    Completed,
    Failed(String),
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct TaskProgress {
    pub downloaded: u64,
    pub total: u64,
    pub speed_bps: f64,
    pub eta_secs: Option<f64>,
    pub chunks_done: usize,
    pub chunks_total: usize,
}

#[derive(Debug)]
pub struct DownloadTask {
    pub id: Uuid,
    pub urls: Vec<String>,
    pub filename: String,
    pub save_path: PathBuf,
    pub part_path: PathBuf,
    pub total_size: Option<u64>,
    pub priority: TaskPriority,
    pub state: TaskState,
    pub chunks: Vec<Chunk>,
    pub expected_hash: Option<(HashAlgo, String)>,
    pub custom_headers: HeaderMap,
    pub cookies: Option<String>,
    pub rate_limit_bps: u64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub progress: TaskProgress,
    pub current_chunk_size: u64,
}

impl DownloadTask {
    pub fn new(
        urls: Vec<String>,
        filename: String,
        download_dir: &Path,
        priority: TaskPriority,
        rate_limit: u64,
    ) -> Self {
        let id = Uuid::new_v4();
        let save_path = download_dir.join(&filename);
        let part_path = download_dir.join(format!("{}.part", filename));
        Self {
            id,
            urls,
            filename,
            save_path,
            part_path,
            total_size: None,
            priority,
            state: TaskState::Queued,
            chunks: Vec::new(),
            expected_hash: None,
            custom_headers: HeaderMap::new(),
            cookies: None,
            rate_limit_bps: rate_limit,
            created_at: chrono::Utc::now(),
            progress: TaskProgress {
                downloaded: 0,
                total: 0,
                speed_bps: 0.0,
                eta_secs: None,
                chunks_done: 0,
                chunks_total: 0,
            },
            current_chunk_size: 4 * 1024 * 1024,
        }
    }
}

pub struct TaskDownloader {
    client: Client,
    settings: Arc<AppSettings>,
    global_limiter: Arc<BandwidthLimiter>,
    task: Arc<RwLock<DownloadTask>>,
    cancel_tx: mpsc::Sender<()>,
}

impl TaskDownloader {
    pub fn new(
        client: Client,
        settings: Arc<AppSettings>,
        global_limiter: Arc<BandwidthLimiter>,
        task: Arc<RwLock<DownloadTask>>,
    ) -> (Self, mpsc::Receiver<()>) {
        let (cancel_tx, cancel_rx) = mpsc::channel(1);
        (
            Self {
                client,
                settings,
                global_limiter,
                task,
                cancel_tx,
            },
            cancel_rx,
        )
    }

    pub fn cancel_handle(&self) -> mpsc::Sender<()> {
        self.cancel_tx.clone()
    }

    pub async fn run(self, mut cancel_rx: mpsc::Receiver<()>) -> Result<()> {
        {
            let mut t = self.task.write().await;
            t.state = TaskState::Preparing;
        }

        let (total_size, supports_range) = self.probe().await?;
        {
            let mut t = self.task.write().await;
            t.total_size = Some(total_size);
            t.progress.total = total_size;
        }

        self.prepare_file(total_size).await?;
        self.build_chunks(total_size, supports_range).await?;

        {
            let mut t = self.task.write().await;
            t.state = TaskState::Downloading;
        }

        let max_conn = self.settings.max_connections_per_task;
        let task_limiter = BandwidthLimiter::new({
            let t = self.task.read().await;
            t.rate_limit_bps
        });

        let retry_policy = RetryPolicy::new(
            self.settings.max_retries,
            self.settings.initial_backoff_ms,
            self.settings.max_backoff_ms,
        );

        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_conn));
        let mut handles = Vec::new();

        loop {
            if cancel_rx.try_recv().is_ok() {
                let mut t = self.task.write().await;
                t.state = TaskState::Cancelled;
                return Ok(());
            }

            let pending: Vec<usize> = {
                let t = self.task.read().await;
                t.chunks
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| matches!(c.state, ChunkState::Pending | ChunkState::Failed))
                    .map(|(i, _)| i)
                    .collect()
            };

            if pending.is_empty() {
                let all_done = {
                    let t = self.task.read().await;
                    t.chunks.iter().all(|c| c.is_done())
                };
                if all_done {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }

            for idx in pending {
                let permit = semaphore.clone().acquire_owned().await?;
                let client = self.client.clone();
                let task = self.task.clone();
                let settings = self.settings.clone();
                let global_lim = self.global_limiter.clone();
                let task_lim = task_limiter.clone();
                let retry = retry_policy.clone();
                let urls = {
                    let t = task.read().await;
                    t.urls.clone()
                };

                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) = download_chunk(
                        client, task, idx, urls, settings, global_lim, task_lim, retry,
                    )
                    .await
                    {
                        error!("Chunk {} failed: {}", idx, e);
                    }
                });
                handles.push(handle);
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            self.update_progress().await;
        }

        for h in handles {
            let _ = h.await;
        }

        {
            let mut t = self.task.write().await;
            t.state = TaskState::Verifying;
        }

        let (path, expected) = {
            let t = self.task.read().await;
            (t.part_path.clone(), t.expected_hash.clone())
        };

        if let Some((algo, hex)) = expected {
            match verify_file(&path, algo, &hex).await {
                Ok(true) => info!("Hash verification passed"),
                Ok(false) => {
                    let mut t = self.task.write().await;
                    t.state = TaskState::Failed("Hash mismatch".into());
                    return Err(anyhow!("Hash verification failed"));
                }
                Err(e) => warn!("Hash verification error: {}", e),
            }
        }

        let (part, final_path) = {
            let t = self.task.read().await;
            (t.part_path.clone(), t.save_path.clone())
        };
        if part.exists() {
            tokio::fs::rename(&part, &final_path)
                .await
                .context("Failed to rename .part to final")?;
        }

        {
            let mut t = self.task.write().await;
            t.state = TaskState::Completed;
            t.progress.downloaded = t.progress.total;
        }

        Ok(())
    }

    async fn probe(&self) -> Result<(u64, bool)> {
        let t = self.task.read().await;
        let url = t.urls.first().ok_or_else(|| anyhow!("No URLs"))?;
        let mut req = self.client.head(url);
        req = req.header(USER_AGENT, &self.settings.user_agent);
        if let Some(ref cookies) = t.cookies {
            req = req.header(COOKIE, cookies);
        }
        for (k, v) in t.custom_headers.iter() {
            req = req.header(k, v);
        }

        let resp = req.send().await.context("HEAD request failed")?;
        if !resp.status().is_success() && resp.status().as_u16() != 405 {
            return self.probe_with_range(url).await;
        }

        let len = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| anyhow!("No Content-Length"))?;

        let accepts = resp
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_lowercase().contains("bytes"))
            .unwrap_or(false);

        Ok((len, accepts))
    }

    async fn probe_with_range(&self, url: &str) -> Result<(u64, bool)> {
        let resp = self
            .client
            .get(url)
            .header(RANGE, "bytes=0-0")
            .header(USER_AGENT, &self.settings.user_agent)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            if let Some(cr) = resp.headers().get(reqwest::header::CONTENT_RANGE) {
                if let Ok(s) = cr.to_str() {
                    if let Some(total) = s.split('/').nth(1) {
                        if let Ok(len) = total.parse::<u64>() {
                            return Ok((len, true));
                        }
                    }
                }
            }
        }
        let resp = self.client.get(url).send().await?;
        let len = resp
            .content_length()
            .ok_or_else(|| anyhow!("Cannot determine size"))?;
        Ok((len, false))
    }

    async fn prepare_file(&self, total: u64) -> Result<()> {
        let t = self.task.read().await;
        let path = &t.part_path;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await?;

        file.set_len(total).await?;

        #[cfg(unix)]
        if self.settings.secure_temp_files {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(path)?.permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(path, perms)?;
        }

        Ok(())
    }

    async fn build_chunks(&self, total: u64, supports_range: bool) -> Result<()> {
        let mut t = self.task.write().await;
        if !t.chunks.is_empty() {
            return Ok(());
        }

        if !supports_range || total < self.settings.min_chunk_size * 2 {
            t.chunks.push(Chunk::new(0, 0, total.saturating_sub(1)));
            t.progress.chunks_total = 1;
            return Ok(());
        }

        let chunk_size = if self.settings.adaptive_chunking {
            t.current_chunk_size
        } else {
            self.settings.initial_chunk_size
        };

        let mut start = 0u64;
        let mut idx = 0;
        while start < total {
            let end = (start + chunk_size - 1).min(total - 1);
            t.chunks.push(Chunk::new(idx, start, end));
            start = end + 1;
            idx += 1;
        }
        t.progress.chunks_total = t.chunks.len();
        Ok(())
    }

    async fn update_progress(&self) {
        let mut t = self.task.write().await;
        let downloaded: u64 = t.chunks.iter().map(|c| c.downloaded).sum();
        let done = t.chunks.iter().filter(|c| c.is_done()).count();
        t.progress.downloaded = downloaded;
        t.progress.chunks_done = done;
        let speed: f64 = t.chunks.iter().map(|c| c.current_speed).sum();
        t.progress.speed_bps = speed;
        if speed > 0.0 {
            let remaining = t.progress.total.saturating_sub(downloaded);
            t.progress.eta_secs = Some(remaining as f64 / speed);
        }
    }
}

async fn download_chunk(
    client: Client,
    task: Arc<RwLock<DownloadTask>>,
    chunk_idx: usize,
    urls: Vec<String>,
    settings: Arc<AppSettings>,
    global_lim: Arc<BandwidthLimiter>,
    task_lim: BandwidthLimiter,
    retry_policy: RetryPolicy,
) -> Result<()> {
    {
        let mut t = task.write().await;
        if let Some(c) = t.chunks.get_mut(chunk_idx) {
            c.state = ChunkState::Downloading;
        }
    }

    let (start, end, already) = {
        let t = task.read().await;
        let c = &t.chunks[chunk_idx];
        (c.start, c.end, c.downloaded)
    };

    let mut attempt = 0u32;
    let mut last_err = None;

    while attempt <= settings.max_retries {
        if attempt > 0 {
            retry_policy.wait(attempt).await;
        }

        for (mirror_idx, url) in urls.iter().enumerate() {
            match try_download_range(
                &client,
                url,
                start + already,
                end,
                &task,
                chunk_idx,
                &settings,
                &global_lim,
                &task_lim,
            )
            .await
            {
                Ok(()) => {
                    let mut t = task.write().await;
                    if let Some(c) = t.chunks.get_mut(chunk_idx) {
                        c.state = ChunkState::Completed;
                        c.downloaded = c.size();
                        c.last_error = None;
                    }
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "Chunk {} mirror {} attempt {} failed: {}",
                        chunk_idx, mirror_idx, attempt, e
                    );
                    last_err = Some(e);
                }
            }
        }
        attempt += 1;
        {
            let mut t = task.write().await;
            if let Some(c) = t.chunks.get_mut(chunk_idx) {
                c.retries = attempt;
            }
        }
    }

    let mut t = task.write().await;
    if let Some(c) = t.chunks.get_mut(chunk_idx) {
        c.state = ChunkState::Failed;
        c.last_error = last_err.map(|e| e.to_string());
    }
    Err(anyhow!("Chunk {} exhausted retries", chunk_idx))
}

async fn try_download_range(
    client: &Client,
    url: &str,
    from: u64,
    to: u64,
    task: &Arc<RwLock<DownloadTask>>,
    chunk_idx: usize,
    settings: &AppSettings,
    global_lim: &BandwidthLimiter,
    task_lim: &BandwidthLimiter,
) -> Result<()> {
    let range_val = format!("bytes={}-{}", from, to);
    let mut req = client.get(url).header(RANGE, range_val);
    req = req.header(USER_AGENT, &settings.user_agent);

    {
        let t = task.read().await;
        if let Some(ref cookies) = t.cookies {
            req = req.header(COOKIE, cookies);
        }
        for (k, v) in t.custom_headers.iter() {
            req = req.header(k.clone(), v.clone());
        }
    }

    let resp = req.send().await.context("Range request failed")?;
    if resp.status() != reqwest::StatusCode::PARTIAL_CONTENT
        && resp.status() != reqwest::StatusCode::OK
    {
        return Err(anyhow!("Unexpected status: {}", resp.status()));
    }

    let part_path = {
        let t = task.read().await;
        t.part_path.clone()
    };

    let mut file = OpenOptions::new().write(true).open(&part_path).await?;
    file.seek(SeekFrom::Start(from)).await?;

    let mut stream = resp.bytes_stream();
    let mut written = 0u64;
    let start_time = Instant::now();

    while let Some(item) = stream.next().await {
        let chunk_bytes: Bytes = item.context("Stream error")?;
        let len = chunk_bytes.len() as u64;

        global_lim.consume(len).await;
        task_lim.consume(len).await;

        file.write_all(&chunk_bytes).await?;
        written += len;

        {
            let mut t = task.write().await;
            if let Some(c) = t.chunks.get_mut(chunk_idx) {
                c.downloaded = (from - c.start) + written;
                let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
                c.current_speed = written as f64 / elapsed;
            }
        }
    }

    file.flush().await?;
    Ok(())
}

impl Clone for RetryPolicy {
    fn clone(&self) -> Self {
        Self {
            max_retries: self.max_retries,
            initial_backoff: self.initial_backoff,
            max_backoff: self.max_backoff,
        }
    }
}
