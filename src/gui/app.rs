use crate::config::AppSettings;
use crate::core::{DownloadManager, TaskPriority};
use crate::util::hash::HashAlgo;
use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints};
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Handle;
use uuid::Uuid;

pub struct SplitDownloaderApp {
    manager: Arc<DownloadManager>,
    rt: Handle,
    settings: AppSettings,
    new_url: String,
    new_mirrors: String,
    new_filename: String,
    new_hash_algo: String,
    new_hash_value: String,
    new_priority: TaskPriority,
    show_settings: bool,
    show_add_dialog: bool,
    selected_task: Option<Uuid>,
    last_clipboard: String,
    last_clip_check: Instant,
    bandwidth_points: Vec<[f64; 2]>,
    status_msg: String,
}

impl SplitDownloaderApp {
    pub fn new(manager: Arc<DownloadManager>, rt: Handle, settings: AppSettings) -> Self {
        Self {
            manager,
            rt,
            settings,
            new_url: String::new(),
            new_mirrors: String::new(),
            new_filename: String::new(),
            new_hash_algo: "sha256".into(),
            new_hash_value: String::new(),
            new_priority: TaskPriority::Normal,
            show_settings: false,
            show_add_dialog: false,
            selected_task: None,
            last_clipboard: String::new(),
            last_clip_check: Instant::now(),
            bandwidth_points: Vec::new(),
            status_msg: "Ready".into(),
        }
    }
}

impl eframe::App for SplitDownloaderApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.settings.clipboard_poll_ms > 0
            && self.last_clip_check.elapsed().as_millis() as u64 > self.settings.clipboard_poll_ms
        {
            self.last_clip_check = Instant::now();
            if let Ok(mut cb) = arboard::Clipboard::new() {
                if let Ok(text) = cb.get_text() {
                    if text != self.last_clipboard
                        && (text.starts_with("http://") || text.starts_with("https://"))
                    {
                        self.last_clipboard = text.clone();
                        self.new_url = text;
                        self.show_add_dialog = true;
                        self.status_msg = "URL detected from clipboard".into();
                    }
                }
            }
        }

        egui::TopBottomPanel::top("menu").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui.button("Add Download...").clicked() {
                        self.show_add_dialog = true;
                        ui.close_menu();
                    }
                    if ui.button("Export Diagnostics").clicked() {
                        let path = self.settings.download_dir.join("diagnostics.txt");
                        let mgr = self.manager.clone();
                        let path2 = path.clone();
                        self.rt.spawn(async move {
                            let _ = mgr.export_diagnostics(path2).await;
                        });
                        self.status_msg = format!("Diagnostics exported to {:?}", path);
                        ui.close_menu();
                    }
                    if ui.button("Quit").clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
                ui.menu_button("View", |ui| {
                    if ui.button("Settings").clicked() {
                        self.show_settings = true;
                        ui.close_menu();
                    }
                });
                ui.label(&self.status_msg);
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Downloads");
            ui.separator();

            let tasks = self.manager.list_tasks();
            egui::ScrollArea::vertical().show(ui, |ui| {
                for task_arc in tasks {
                    let task = self.rt.block_on(task_arc.read());
                    let id = task.id;
                    let selected = self.selected_task == Some(id);

                    ui.group(|ui| {
                        ui.horizontal(|ui| {
                            if ui.selectable_label(selected, &task.filename).clicked() {
                                self.selected_task = Some(id);
                            }
                            ui.label(format!("{:?}", task.state));
                            let pct = if task.progress.total > 0 {
                                100.0 * task.progress.downloaded as f32
                                    / task.progress.total as f32
                            } else {
                                0.0
                            };
                            ui.add(
                                egui::ProgressBar::new(pct / 100.0).text(format!("{:.1}%", pct)),
                            );
                            ui.label(format!("{:.1} KB/s", task.progress.speed_bps / 1024.0));
                        });

                        ui.horizontal(|ui| {
                            if ui.button("Pause").clicked() {
                                let mgr = self.manager.clone();
                                self.rt.spawn(async move {
                                    mgr.pause(id).await;
                                });
                            }
                            if ui.button("Resume").clicked() {
                                let mgr = self.manager.clone();
                                self.rt.spawn(async move {
                                    mgr.resume(id).await;
                                });
                            }
                            if ui.button("Cancel").clicked() {
                                let mgr = self.manager.clone();
                                self.rt.spawn(async move {
                                    mgr.cancel(id).await;
                                });
                            }
                        });

                        if selected {
                            ui.collapsing("Chunks", |ui| {
                                for c in &task.chunks {
                                    let color = match c.state {
                                        crate::core::ChunkState::Completed => egui::Color32::GREEN,
                                        crate::core::ChunkState::Downloading => {
                                            egui::Color32::YELLOW
                                        }
                                        crate::core::ChunkState::Failed => egui::Color32::RED,
                                        _ => egui::Color32::GRAY,
                                    };
                                    ui.colored_label(
                                        color,
                                        format!(
                                            "#{} [{:-6}-{:-6}] {:?} {:.0}%",
                                            c.index,
                                            c.start,
                                            c.end,
                                            c.state,
                                            c.progress() * 100.0
                                        ),
                                    );
                                }
                            });
                        }
                    });
                }
            });

            ui.separator();
            ui.heading("Bandwidth");
            if self.bandwidth_points.len() > 120 {
                self.bandwidth_points
                    .drain(0..self.bandwidth_points.len() - 120);
            }
            let points: PlotPoints = self.bandwidth_points.iter().copied().collect();
            let line = Line::new(points);
            Plot::new("bw")
                .height(120.0)
                .show(ui, |plot_ui| {
                    plot_ui.line(line);
                });
        });

        if self.show_add_dialog {
            egui::Window::new("Add Download")
                .collapsible(false)
                .resizable(true)
                .show(ctx, |ui| {
                    ui.label("URL (primary):");
                    ui.text_edit_singleline(&mut self.new_url);
                    ui.label("Mirrors (one per line, optional):");
                    ui.text_edit_multiline(&mut self.new_mirrors);
                    ui.label("Filename (optional):");
                    ui.text_edit_singleline(&mut self.new_filename);
                    ui.horizontal(|ui| {
                        ui.label("Hash algo:");
                        ui.text_edit_singleline(&mut self.new_hash_algo);
                        ui.label("Value:");
                        ui.text_edit_singleline(&mut self.new_hash_value);
                    });
                    ui.horizontal(|ui| {
                        ui.label("Priority:");
                        ui.radio_value(&mut self.new_priority, TaskPriority::Critical, "Critical");
                        ui.radio_value(&mut self.new_priority, TaskPriority::High, "High");
                        ui.radio_value(&mut self.new_priority, TaskPriority::Normal, "Normal");
                    });

                    ui.horizontal(|ui| {
                        if ui.button("Start").clicked() {
                            let mut urls = vec![self.new_url.clone()];
                            for line in self.new_mirrors.lines() {
                                let u = line.trim();
                                if !u.is_empty() {
                                    urls.push(u.to_string());
                                }
                            }
                            let filename = if self.new_filename.is_empty() {
                                None
                            } else {
                                Some(self.new_filename.clone())
                            };
                            let hash = if !self.new_hash_value.is_empty() {
                                HashAlgo::from_str(&self.new_hash_algo)
                                    .map(|a| (a, self.new_hash_value.clone()))
                            } else {
                                None
                            };
                            let mgr = self.manager.clone();
                            let priority = self.new_priority;
                            self.rt.spawn(async move {
                                let _ = mgr
                                    .add_task(urls, filename, priority, None, hash, None, None)
                                    .await;
                            });
                            self.show_add_dialog = false;
                            self.new_url.clear();
                            self.new_mirrors.clear();
                            self.new_filename.clear();
                            self.new_hash_value.clear();
                            self.status_msg = "Task added".into();
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_add_dialog = false;
                        }
                    });
                });
        }

        if self.show_settings {
            egui::Window::new("Settings").show(ctx, |ui| {
                ui.label("Max concurrent tasks:");
                ui.add(egui::Slider::new(
                    &mut self.settings.max_concurrent_tasks,
                    1..=16,
                ));
                ui.label("Max connections per task:");
                ui.add(egui::Slider::new(
                    &mut self.settings.max_connections_per_task,
                    1..=32,
                ));
                ui.checkbox(
                    &mut self.settings.enable_notifications,
                    "Enable notifications",
                );
                ui.checkbox(&mut self.settings.adaptive_chunking, "Adaptive chunking");
                if ui.button("Save").clicked() {
                    let s = self.settings.clone();
                    let mgr = self.manager.clone();
                    self.rt.spawn(async move {
                        let _ = mgr.update_settings(s).await;
                    });
                    self.show_settings = false;
                }
                if ui.button("Close").clicked() {
                    self.show_settings = false;
                }
            });
        }

        ctx.request_repaint_after(std::time::Duration::from_millis(200));
    }
}
