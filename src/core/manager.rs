use crate::config::AppSettings;
use crate::core::downloader::{DownloadTask, TaskDownloader, TaskPriority, TaskState};
use crate::core::rate_limiter::BandwidthLimiter;
use crate::util::notify::send_notification;
use anyhow::Result;
use dashmap::DashMap;
use reqwest::{Client, Proxy};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{error, info};
use uuid::Uuid;

struct PriorityTask {
    priority: TaskPriority,
    created: chrono::DateTime<chrono::Utc>,
    id: Uuid,
}

impl PartialEq for PriorityTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for PriorityTask {}
impl PartialOrd for PriorityTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PriorityTask {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.created.cmp(&self.created))
    }
}

pub struct DownloadManager {
    settings: Arc<RwLock<AppSettings>>,
    client: Client,
    global_limiter: Arc<BandwidthLimiter>,
    tasks: DashMap<Uuid, Arc<RwLock<DownloadTask>>>,
    queue: Arc<RwLock<BinaryHeap<PriorityTask>>>,
    active_semaphore: Arc<Semaphore>,
    bandwidth_history: Arc<RwLock<Vec<(f64, f64)>>>,
    cancel_handles: DashMap<Uuid, mpsc::Sender<()>>,
}

impl DownloadManager {
    pub async fn new(settings: AppSettings) -> Result<Self> {
        let mut builder = Client::builder()
            .user_agent(&settings.user_agent)
            .pool_max_idle_per_host(16)
            .timeout(std::time::Duration::from_secs(60));

        if let Some(ref proxy_url) = settings.proxy {
            let proxy = Proxy::all(proxy_url)?;
            builder = builder.proxy(proxy);
        }

        let client = builder.build()?;
        let global_limiter = Arc::new(BandwidthLimiter::new(settings.global_rate_limit_bps));
        let max_tasks = settings.max_concurrent_tasks;

        Ok(Self {
            settings: Arc::new(RwLock::new(settings)),
            client,
            global_limiter,
            tasks: DashMap::new(),
            queue: Arc::new(RwLock::new(BinaryHeap::new())),
            active_semaphore: Arc::new(Semaphore::new(max_tasks)),
            bandwidth_history: Arc::new(RwLock::new(Vec::new())),
            cancel_handles: DashMap::new(),
        })
    }

    pub async fn add_task(
        &self,
        urls: Vec<String>,
        filename: Option<String>,
        priority: TaskPriority,
        rate_limit: Option<u64>,
        hash: Option<(crate::util::hash::HashAlgo, String)>,
        headers: Option<reqwest::header::HeaderMap>,
        cookies: Option<String>,
    ) -> Result<Uuid> {
        let settings = self.settings.read().await;
        let name = filename.unwrap_or_else(|| {
            urls.first()
                .and_then(|u| {
                    url::Url::parse(u).ok().and_then(|uu| {
                        uu.path_segments()
                            .and_then(|s| s.last().map(|s| s.to_string()))
                    })
                })
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("download_{}", Uuid::new_v4()))
        });

        let rate = rate_limit.unwrap_or(settings.default_task_rate_limit_bps);
        let mut task = DownloadTask::new(urls, name, &settings.download_dir, priority, rate);
        if let Some(h) = hash {
            task.expected_hash = Some(h);
        }
        if let Some(h) = headers {
            task.custom_headers = h;
        }
        task.cookies = cookies;

        let id = task.id;
        let arc_task = Arc::new(RwLock::new(task));
        self.tasks.insert(id, arc_task.clone());

        {
            let mut q = self.queue.write().await;
            q.push(PriorityTask {
                priority,
                created: chrono::Utc::now(),
                id,
            });
        }

        self.try_start_next().await;
        Ok(id)
    }

    async fn try_start_next(&self) {
        let permit = match self.active_semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => return,
        };

        let next_id = {
            let mut q = self.queue.write().await;
            q.pop().map(|p| p.id)
        };

        let Some(id) = next_id else {
            drop(permit);
            return;
        };

        let Some(task_arc) = self.tasks.get(&id).map(|t| t.clone()) else {
            drop(permit);
            return;
        };

        {
            let t = task_arc.read().await;
            if !matches!(t.state, TaskState::Queued) {
                drop(permit);
                return;
            }
        }

        let client = self.client.clone();
        let settings = {
            let s = self.settings.read().await;
            Arc::new(s.clone())
        };
        let global_lim = self.global_limiter.clone();
        let (downloader, cancel_rx) =
            TaskDownloader::new(client, settings.clone(), global_lim, task_arc.clone());
        self.cancel_handles
            .insert(id, downloader.cancel_handle());

        let settings_for_notify = settings.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let result = downloader.run(cancel_rx).await;
            match result {
                Ok(()) => {
                    let t = task_arc.read().await;
                    if matches!(t.state, TaskState::Completed) {
                        info!("Task {} completed: {}", id, t.filename);
                        if settings_for_notify.enable_notifications {
                            let _ = send_notification(
                                "Download Complete",
                                &format!("{} finished", t.filename),
                            );
                        }
                        if let Some(ref cmd) = settings_for_notify.on_complete_cmd {
                            let cmd = cmd.replace("{path}", &t.save_path.to_string_lossy());
                            let _ = std::process::Command::new("sh").arg("-c").arg(&cmd).spawn();
                        }
                    }
                }
                Err(e) => {
                    error!("Task {} failed: {}", id, e);
                    let mut t = task_arc.write().await;
                    t.state = TaskState::Failed(e.to_string());
                }
            }
        });
    }

    pub async fn pause(&self, id: Uuid) {
        if let Some(tx) = self.cancel_handles.get(&id) {
            let _ = tx.send(()).await;
        }
        if let Some(t) = self.tasks.get(&id) {
            let mut task = t.write().await;
            if matches!(task.state, TaskState::Downloading) {
                task.state = TaskState::Paused;
            }
        }
    }

    pub async fn resume(&self, id: Uuid) {
        if let Some(t) = self.tasks.get(&id) {
            let mut task = t.write().await;
            if matches!(task.state, TaskState::Paused | TaskState::Failed(_)) {
                task.state = TaskState::Queued;
                let priority = task.priority;
                drop(task);
                let mut q = self.queue.write().await;
                q.push(PriorityTask {
                    priority,
                    created: chrono::Utc::now(),
                    id,
                });
            }
        }
        self.try_start_next().await;
    }

    pub async fn cancel(&self, id: Uuid) {
        if let Some(tx) = self.cancel_handles.get(&id) {
            let _ = tx.send(()).await;
        }
        if let Some(t) = self.tasks.get(&id) {
            let mut task = t.write().await;
            task.state = TaskState::Cancelled;
            let part = task.part_path.clone();
            drop(task);
            let _ = tokio::fs::remove_file(part).await;
        }
        self.cancel_handles.remove(&id);
    }

    pub fn list_tasks(&self) -> Vec<Arc<RwLock<DownloadTask>>> {
        self.tasks.iter().map(|e| e.value().clone()).collect()
    }

    pub async fn get_bandwidth_history(&self) -> Vec<(f64, f64)> {
        self.bandwidth_history.read().await.clone()
    }

    pub async fn update_settings(&self, new: AppSettings) -> Result<()> {
        new.save()?;
        let mut s = self.settings.write().await;
        *s = new;
        Ok(())
    }

    pub async fn export_diagnostics(&self, path: PathBuf) -> Result<()> {
        let mut report = String::new();
        report.push_str("# SplitDownloader Diagnostics\n\n");
        report.push_str(&format!("Time: {}\n\n", chrono::Utc::now()));
        for entry in self.tasks.iter() {
            let t = entry.value().read().await;
            report.push_str(&format!(
                "Task {} | {} | {:?} | {}/{} bytes | speed {:.1} KB/s\n",
                t.id,
                t.filename,
                t.state,
                t.progress.downloaded,
                t.progress.total,
                t.progress.speed_bps / 1024.0
            ));
            for c in &t.chunks {
                report.push_str(&format!(
                    "  Chunk {} [{:-}-{:-}] {:?} downloaded={} retries={}\n",
                    c.index, c.start, c.end, c.state, c.downloaded, c.retries
                ));
            }
        }
        tokio::fs::write(path, report).await?;
        Ok(())
    }
}
