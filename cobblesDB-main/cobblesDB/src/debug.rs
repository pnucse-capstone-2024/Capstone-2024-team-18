use crate::lsm_storage::{LsmStorageInner, CobblesDB};

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        if !snapshot.l0_sstables.is_empty() {
            println!(
                "L0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            );
        }
        for (level, files) in &snapshot.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
    }
}

impl CobblesDB {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
