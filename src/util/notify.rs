use anyhow::Result;
use notify_rust::Notification;

pub fn send_notification(title: &str, body: &str) -> Result<()> {
    Notification::new()
        .summary(title)
        .body(body)
        .appname("SplitDownloader")
        .timeout(5000)
        .show()?;
    Ok(())
}
