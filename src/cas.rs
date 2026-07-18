//! BLAKE3 content-addressed blob storage.
//!
//! Small blobs are returned inline so the authoritative store can keep them in
//! SQLite. Larger blobs are written atomically below `root`, using their digest
//! as the file name. A [`BlobRef`] therefore contains everything required to
//! read and verify content without exposing storage details to callers.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{MemoryError, Result};

/// Avoid creating a filesystem inode for the small text artifacts that make up
/// most agent memory.
pub const DEFAULT_INLINE_THRESHOLD: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub hash: String,
    pub size_bytes: u64,
    /// Present when bytes are stored by the caller (normally in SQLite).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline_bytes: Option<Vec<u8>>,
}

impl BlobRef {
    pub fn is_inline(&self) -> bool {
        self.inline_bytes.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct Cas {
    root: PathBuf,
    inline_threshold: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CasVerifyReport {
    pub checked_files: usize,
    pub issues: Vec<String>,
}

impl CasVerifyReport {
    pub fn is_ok(&self) -> bool {
        self.issues.is_empty()
    }
}

impl Cas {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        Self::with_inline_threshold(root, DEFAULT_INLINE_THRESHOLD)
    }

    pub fn with_inline_threshold(root: impl AsRef<Path>, inline_threshold: usize) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        fs::create_dir_all(root.join(".tmp"))?;
        Ok(Self {
            root,
            inline_threshold,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn inline_threshold(&self) -> usize {
        self.inline_threshold
    }

    /// Hash and persist bytes. Writes are atomic and naturally idempotent.
    pub fn put(&self, bytes: &[u8]) -> Result<BlobRef> {
        let hash = blake3::hash(bytes).to_hex().to_string();
        let size_bytes = u64::try_from(bytes.len()).map_err(|_| MemoryError::ContentTooLarge)?;

        if bytes.len() <= self.inline_threshold {
            return Ok(BlobRef {
                hash,
                size_bytes,
                inline_bytes: Some(bytes.to_vec()),
            });
        }

        let destination = self.path_for_hash(&hash)?;
        if destination.exists() {
            self.verify_external(&hash, size_bytes)?;
            return Ok(BlobRef {
                hash,
                size_bytes,
                inline_bytes: None,
            });
        }

        let parent = destination
            .parent()
            .ok_or_else(|| MemoryError::Integrity(format!("invalid CAS path for digest {hash}")))?;
        fs::create_dir_all(parent)?;

        let mut temporary = tempfile::NamedTempFile::new_in(self.root.join(".tmp"))?;
        temporary.write_all(bytes)?;
        temporary.as_file_mut().sync_all()?;

        match temporary.persist_noclobber(&destination) {
            Ok(_) => {
                // Best effort directory durability. Opening directories is
                // supported on Unix (the primary target); failure does not
                // invalidate the atomically persisted object.
                if let Ok(directory) = File::open(parent) {
                    let _ = directory.sync_all();
                }
            }
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                // Another writer won the race. The digest still has to match;
                // never trust a file solely because its name looks correct.
                self.verify_external(&hash, size_bytes)?;
            }
            Err(error) => return Err(MemoryError::Io(error.error)),
        }

        Ok(BlobRef {
            hash,
            size_bytes,
            inline_bytes: None,
        })
    }

    /// Read bytes and verify both their length and digest.
    pub fn get(&self, blob: &BlobRef) -> Result<Vec<u8>> {
        let bytes = match &blob.inline_bytes {
            Some(bytes) => bytes.clone(),
            None => fs::read(self.path_for_hash(&blob.hash)?)?,
        };
        self.verify_bytes(&blob.hash, blob.size_bytes, &bytes)?;
        Ok(bytes)
    }

    pub fn verify(&self, blob: &BlobRef) -> Result<()> {
        match &blob.inline_bytes {
            Some(bytes) => self.verify_bytes(&blob.hash, blob.size_bytes, bytes),
            None => self.verify_external(&blob.hash, blob.size_bytes),
        }
    }

    pub fn external_exists(&self, hash: &str) -> Result<bool> {
        Ok(self.path_for_hash(hash)?.is_file())
    }

    /// Verify every external object, including objects no longer referenced by
    /// the database. Unreferenced objects are harmless and can be collected by
    /// a later maintenance pass.
    pub fn verify_all_external(&self) -> Result<CasVerifyReport> {
        let mut report = CasVerifyReport::default();
        if !self.root.exists() {
            report
                .issues
                .push(format!("CAS root does not exist: {}", self.root.display()));
            return Ok(report);
        }

        for first in fs::read_dir(&self.root)? {
            let first = first?;
            if !first.file_type()?.is_dir() || first.file_name() == ".tmp" {
                continue;
            }
            for second in fs::read_dir(first.path())? {
                let second = second?;
                if !second.file_type()?.is_dir() {
                    continue;
                }
                for entry in fs::read_dir(second.path())? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    report.checked_files += 1;
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if validate_hash(&name).is_err() {
                        report.issues.push(format!(
                            "unexpected CAS file name: {}",
                            entry.path().display()
                        ));
                        continue;
                    }
                    let mut file = File::open(entry.path())?;
                    let mut hasher = blake3::Hasher::new();
                    let mut buffer = [0_u8; 64 * 1024];
                    loop {
                        let count = file.read(&mut buffer)?;
                        if count == 0 {
                            break;
                        }
                        hasher.update(&buffer[..count]);
                    }
                    let actual = hasher.finalize().to_hex().to_string();
                    if actual != name {
                        report.issues.push(format!(
                            "CAS digest mismatch for {}: got {actual}",
                            entry.path().display()
                        ));
                    }
                }
            }
        }
        Ok(report)
    }

    /// Copy external objects into another CAS root, preserving digest paths.
    ///
    /// Both the source and any pre-existing destination object are verified.
    /// A target is never trusted merely because its digest-shaped path exists.
    /// New targets are written durably and atomically. The caller is
    /// responsible for serializing this with writes when a point-in-time copy
    /// is required.
    pub fn copy_external_to(&self, destination_root: impl AsRef<Path>) -> Result<usize> {
        let destination = Cas::with_inline_threshold(destination_root, self.inline_threshold)?;
        let mut copied = 0;
        for first in fs::read_dir(&self.root)? {
            let first = first?;
            if !first.file_type()?.is_dir() || first.file_name() == ".tmp" {
                continue;
            }
            for second in fs::read_dir(first.path())? {
                let second = second?;
                if !second.file_type()?.is_dir() {
                    continue;
                }
                for entry in fs::read_dir(second.path())? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    let hash = entry.file_name().to_string_lossy().into_owned();
                    validate_hash(&hash)?;
                    let bytes = fs::read(entry.path())?;
                    let actual = blake3::hash(&bytes).to_hex().to_string();
                    if actual != hash {
                        return Err(MemoryError::Integrity(format!(
                            "cannot back up corrupt CAS object {hash}"
                        )));
                    }
                    let size_bytes =
                        u64::try_from(bytes.len()).map_err(|_| MemoryError::ContentTooLarge)?;
                    let target = destination.path_for_hash(&hash)?;
                    if target.exists() {
                        destination.verify_external(&hash, size_bytes)?;
                        continue;
                    }

                    let parent = target.parent().ok_or_else(|| {
                        MemoryError::Integrity(format!("invalid CAS path for {hash}"))
                    })?;
                    fs::create_dir_all(parent)?;
                    let mut temporary =
                        tempfile::NamedTempFile::new_in(destination.root.join(".tmp"))?;
                    temporary.write_all(&bytes)?;
                    temporary.as_file_mut().sync_all()?;
                    match temporary.persist_noclobber(&target) {
                        Ok(_) => {
                            copied += 1;
                            if let Ok(directory) = File::open(parent) {
                                let _ = directory.sync_all();
                            }
                        }
                        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                            // A concurrent copier won the race. It is usable
                            // only if it contains exactly the expected object.
                            destination.verify_external(&hash, size_bytes)?;
                        }
                        Err(error) => return Err(MemoryError::Io(error.error)),
                    }
                }
            }
        }
        Ok(copied)
    }

    fn verify_external(&self, hash: &str, expected_size: u64) -> Result<()> {
        let path = self.path_for_hash(hash)?;
        let mut file = File::open(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                MemoryError::Integrity(format!("missing CAS object {hash} at {}", path.display()))
            } else {
                MemoryError::Io(error)
            }
        })?;
        let actual_size = file.metadata()?.len();
        if actual_size != expected_size {
            return Err(MemoryError::Integrity(format!(
                "blob {hash} has size {actual_size}, expected {expected_size}"
            )));
        }
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        let actual_hash = hasher.finalize().to_hex().to_string();
        if actual_hash != hash {
            return Err(MemoryError::Integrity(format!(
                "blob digest mismatch: expected {hash}, got {actual_hash}"
            )));
        }
        Ok(())
    }

    fn verify_bytes(&self, hash: &str, expected_size: u64, bytes: &[u8]) -> Result<()> {
        validate_hash(hash)?;
        let actual_size = u64::try_from(bytes.len()).map_err(|_| MemoryError::ContentTooLarge)?;
        if actual_size != expected_size {
            return Err(MemoryError::Integrity(format!(
                "blob {hash} has size {actual_size}, expected {expected_size}"
            )));
        }
        let actual_hash = blake3::hash(bytes).to_hex().to_string();
        if actual_hash != hash {
            return Err(MemoryError::Integrity(format!(
                "blob digest mismatch: expected {hash}, got {actual_hash}"
            )));
        }
        Ok(())
    }

    fn path_for_hash(&self, hash: &str) -> Result<PathBuf> {
        validate_hash(hash)?;
        Ok(self.root.join(&hash[..2]).join(&hash[2..4]).join(hash))
    }
}

fn validate_hash(hash: &str) -> Result<()> {
    if hash.len() == 64
        && hash
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        Ok(())
    } else {
        Err(MemoryError::Integrity(format!(
            "invalid BLAKE3 digest {hash:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_and_external_round_trip() {
        let temporary = tempfile::tempdir().unwrap();
        let cas = Cas::with_inline_threshold(temporary.path(), 8).unwrap();

        let inline = cas.put(b"short").unwrap();
        assert!(inline.is_inline());
        assert_eq!(cas.get(&inline).unwrap(), b"short");

        let external = cas.put(b"this is longer").unwrap();
        assert!(!external.is_inline());
        assert_eq!(cas.get(&external).unwrap(), b"this is longer");
        assert!(cas.verify_all_external().unwrap().is_ok());
    }

    #[test]
    fn detects_corrupt_inline_content() {
        let temporary = tempfile::tempdir().unwrap();
        let cas = Cas::new(temporary.path()).unwrap();
        let mut blob = cas.put(b"correct").unwrap();
        blob.inline_bytes = Some(b"wrong!!".to_vec());
        assert!(matches!(cas.get(&blob), Err(MemoryError::Integrity(_))));
    }

    #[test]
    fn copy_rejects_a_corrupt_preexisting_target() {
        let temporary = tempfile::tempdir().unwrap();
        let source = Cas::with_inline_threshold(temporary.path().join("source"), 1).unwrap();
        let destination =
            Cas::with_inline_threshold(temporary.path().join("destination"), 1).unwrap();
        let blob = source.put(b"external object").unwrap();
        let target = destination.path_for_hash(&blob.hash).unwrap();
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, b"corrupt object!").unwrap();

        let error = source
            .copy_external_to(destination.root())
            .expect_err("an existing target must be verified");
        assert!(matches!(error, MemoryError::Integrity(_)));
        assert_eq!(fs::read(target).unwrap(), b"corrupt object!");
    }

    #[test]
    fn copy_accepts_a_verified_preexisting_target_without_recopying() {
        let temporary = tempfile::tempdir().unwrap();
        let source = Cas::with_inline_threshold(temporary.path().join("source"), 1).unwrap();
        let destination =
            Cas::with_inline_threshold(temporary.path().join("destination"), 1).unwrap();
        let blob = source.put(b"external object").unwrap();
        assert_eq!(source.copy_external_to(destination.root()).unwrap(), 1);
        assert_eq!(source.copy_external_to(destination.root()).unwrap(), 0);
        destination.verify(&blob).unwrap();
    }
}
