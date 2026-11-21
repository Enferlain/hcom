use std::fs::File;
use std::io::ErrorKind;

use anyhow::{Context, Result};

use super::store::Store;

/// Held for the controller's lifetime. The kernel releases ownership on exit.
/// The lock file is never unlinked, so contenders always lock the same file.
/// Removing it externally while a controller may be alive is unsupported.
pub struct Owner<'store, 'db> {
    _lock: File,
    store: &'store Store<'db>,
    epoch: i64,
}

impl<'store, 'db> Owner<'store, 'db> {
    pub fn try_acquire(store: &'store Store<'db>) -> Result<Option<Self>> {
        let directory = store.path().with_file_name(".sandbox-controller");
        crate::paths::ensure_private_directory(&directory)?;
        let path = directory.join("owner.lock");
        let lock = match crate::sys::fs::create_private_new(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                File::options().read(true).write(true).open(path)?
            }
            Err(error) => return Err(error.into()),
        };
        if !crate::sys::fs::try_lock_exclusive(&lock).context("lock sandbox controller")? {
            return Ok(None);
        }
        let epoch = store.allocate_epoch()?;
        Ok(Some(Self {
            _lock: lock,
            store,
            epoch,
        }))
    }

    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Each replacement connection receives a fresh epoch. The bridge must
    /// bind it to its route before admitting any traffic from that connection.
    pub fn allocate_connection_epoch(&self) -> Result<i64> {
        self.store.allocate_epoch()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::HcomDb;

    #[test]
    fn only_one_owner_and_epoch_survives_takeover() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("hcom.db");
        let first_db = HcomDb::open_at(&path).unwrap();
        let second_db = HcomDb::open_at(&path).unwrap();
        let first = Store::initialize(&first_db).unwrap();
        let second = Store::initialize(&second_db).unwrap();
        let owner = Owner::try_acquire(&first).unwrap().unwrap();
        assert!(Owner::try_acquire(&second).unwrap().is_none());
        let epoch = owner.allocate_connection_epoch().unwrap();
        assert!(epoch > owner.epoch());
        drop(owner);
        let successor = Owner::try_acquire(&second).unwrap().unwrap();
        assert!(successor.epoch() > epoch);
        assert!(Owner::try_acquire(&first).unwrap().is_none());
    }
}
