mod config;
mod core;
mod gui;
mod util;

use crate::config::AppSettings;
use crate::core::DownloadManager;
use crate::gui::SplitDownloaderApp;
use eframe::egui;
use std::sync::Arc;
use tracing_subscriber::{fmt, EnvFilter};

fn main() -> eframe::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let settings = AppSettings::load();
    std::fs::create_dir_all(&settings.download_dir).ok();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");
    let handle = rt.handle().clone();

    let manager = rt
        .block_on(DownloadManager::new(settings.clone()))
        .expect("Failed to create DownloadManager");
    let manager = Arc::new(manager);

    let app = SplitDownloaderApp::new(manager, handle, settings);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1000.0, 700.0])
            .with_title("SplitDownloader"),
        ..Default::default()
    };

    // Keep runtime alive
    let _rt_guard = rt;

    eframe::run_native(
        "SplitDownloader",
        options,
        Box::new(|_cc| Ok(Box::new(app))),
    )
}
