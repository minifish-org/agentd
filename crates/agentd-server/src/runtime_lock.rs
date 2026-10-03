use anyhow::{Context, Result};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

/// The sidecar must remain in place after unlock: unlinking a lock file would
/// let another process lock a different inode for the same database.
pub(crate) struct DatabaseOwnership {
    pub(crate) database_path: PathBuf,
    _lock: File,
}

pub(crate) struct SandboxOwnership {
    pub(crate) state_dir: PathBuf,
    _lock: File,
}

pub(crate) fn acquire_sandbox_lock(state_dir: &Path) -> Result<SandboxOwnership> {
    if !state_dir.is_absolute() {
        anyhow::bail!("sandbox state directory must be absolute");
    }
    std::fs::create_dir_all(state_dir)?;
    let state_dir = state_dir.canonicalize()?;
    let lock = acquire_lock_file(&state_dir.join(".agentd-runtime-lock"))?;
    Ok(SandboxOwnership {
        state_dir,
        _lock: lock,
    })
}

fn acquire_lock_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    file.try_lock().map_err(|error| {
        anyhow::anyhow!("runtime is already owned or cannot be locked: {error}")
    })?;
    Ok(file)
}

pub(crate) fn acquire_database_lock(database: &Path) -> Result<DatabaseOwnership> {
    if !database.is_absolute() {
        anyhow::bail!("runtime database path must be absolute");
    }
    let parent = database.parent().context("database path has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create database directory {}", parent.display()))?;
    let identity = if database.is_symlink() || database.exists() {
        database
            .canonicalize()
            .context("database path or symlink target could not be resolved")?
    } else {
        parent.canonicalize()?.join(
            database
                .file_name()
                .context("database path has no file name")?,
        )
    };
    let mut lock_path = identity.as_os_str().to_os_string();
    lock_path.push(".runtime-lock");
    let file = acquire_lock_file(Path::new(&lock_path))?;
    Ok(DatabaseOwnership {
        database_path: identity,
        _lock: file,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_runtime_has_one_owner_and_releases_on_drop() {
        let dir = tempfile::TempDir::new().unwrap();
        let database = dir.path().join("agentd.db");
        let first = acquire_database_lock(&database).unwrap();
        assert!(acquire_database_lock(&database).is_err());
        drop(first);
        assert!(acquire_database_lock(&database).is_ok());
    }

    #[test]
    fn memory_and_relative_database_locations_are_rejected() {
        assert!(acquire_database_lock(Path::new(":memory:")).is_err());
        assert!(acquire_database_lock(Path::new("agentd.db")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn sandbox_directory_aliases_have_one_runtime_owner() {
        let dir = tempfile::TempDir::new().unwrap();
        let state_dir = dir.path().join("sandbox");
        let owner = acquire_sandbox_lock(&state_dir).unwrap();
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(&state_dir, &alias).unwrap();
        assert!(acquire_sandbox_lock(&alias).is_err());
        drop(owner);
        assert!(acquire_sandbox_lock(&alias).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn dangling_database_alias_cannot_create_a_second_lock_identity() {
        let dir = tempfile::TempDir::new().unwrap();
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(dir.path().join("missing.db"), &alias).unwrap();
        assert!(acquire_database_lock(&alias).is_err());
        assert!(!dir.path().join("alias.db.runtime-lock").exists());
    }

    #[cfg(unix)]
    #[test]
    fn database_aliases_share_the_runtime_lock() {
        let dir = tempfile::TempDir::new().unwrap();
        let database = dir.path().join("agentd.db");
        std::fs::write(&database, b"existing database").unwrap();
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&database, &alias).unwrap();
        let _owner = acquire_database_lock(&database).unwrap();
        assert!(acquire_database_lock(&alias).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn resetting_through_an_alias_preserves_database_identity_and_ownership() {
        let dir = tempfile::TempDir::new().unwrap();
        let database = dir.path().join("agentd.db");
        std::fs::write(&database, b"old database").unwrap();
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&database, &alias).unwrap();
        let owner = acquire_database_lock(&alias).unwrap();
        assert_eq!(owner.database_path, database.canonicalize().unwrap());
        std::fs::remove_file(&owner.database_path).unwrap();
        std::fs::write(&owner.database_path, b"new database").unwrap();
        assert!(alias.is_symlink());
        assert!(acquire_database_lock(&alias).is_err());
        assert!(acquire_database_lock(&database).is_err());
    }
}
