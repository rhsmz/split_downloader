use anyhow::{Context, Result};
use md5::{Digest as Md5Digest, Md5};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use xxhash_rust::xxh64::Xxh64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgo {
    Sha256,
    Md5,
    XxHash64,
}

impl HashAlgo {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "sha256" | "sha-256" => Some(Self::Sha256),
            "md5" => Some(Self::Md5),
            "xxh64" | "xxhash" | "xxhash64" => Some(Self::XxHash64),
            _ => None,
        }
    }
}

pub async fn verify_file(path: &Path, algo: HashAlgo, expected_hex: &str) -> Result<bool> {
    let computed = compute_hash(path, algo).await?;
    Ok(computed.eq_ignore_ascii_case(expected_hex.trim()))
}

pub async fn compute_hash(path: &Path, algo: HashAlgo) -> Result<String> {
    let mut file = File::open(path).await.context("Open for hash")?;
    let mut buf = vec![0u8; 128 * 1024];

    match algo {
        HashAlgo::Sha256 => {
            let mut hasher = Sha256::new();
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(hex::encode(hasher.finalize()))
        }
        HashAlgo::Md5 => {
            let mut hasher = Md5::new();
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(hex::encode(hasher.finalize()))
        }
        HashAlgo::XxHash64 => {
            let mut hasher = Xxh64::new(0);
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(format!("{:016x}", hasher.digest()))
        }
    }
}
