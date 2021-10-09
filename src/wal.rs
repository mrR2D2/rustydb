use std::fs::{File, OpenOptions, remove_file};
use std::io::{BufWriter, Write, Result as IoResult};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::memtable;
use crate::utils;
use crate::wal_iterator::WalIterator;

/* WAL entry has the following format:
+----------------+-----------------+-------------+-----+-------------+-------+
| tombstone (1B) | timestamp (16B) | k_size (8B) | key | v_size (8B) | value |
+----------------+-----------------+-------------+-----+-------------+-------+

    tombstone = If this record was deleted and has a value.
    timestamp = Timestamp of the operation in microseconds.
    k_size = Length of the Key data.
    key = Key data.
    v_size = Length of the Value data.
    value = Value data.
 */

#[derive(Debug)]
pub struct WalEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

pub struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
}

impl IntoIterator for Wal {
    type Item = WalEntry;
    type IntoIter = WalIterator;

    fn into_iter(self) -> Self::IntoIter {
        WalIterator::new(self.path).unwrap()
    }
}

impl Wal {

    pub fn new(dir: &Path) -> IoResult<Wal> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(Wal { path, file })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8],
               timestamp: u128) -> IoResult<()> {

        self.file.write_all(&(false as u8).to_le_bytes())?; // tombstone
        self.file.write_all(&timestamp.to_le_bytes())?;     // timestamp
        self.file.write_all(&key.len().to_le_bytes())?;     // k_size
        self.file.write_all(key)?;                          // key
        self.file.write_all(&value.len().to_le_bytes())?;   // v_size
        self.file.write_all(value)?;                        // value

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> IoResult<()> {
        self.file.write_all(&(true as u8).to_le_bytes())?;  // tombstone
        self.file.write_all(&timestamp.to_le_bytes())?;     // timestamp
        self.file.write_all(&key.len().to_le_bytes())?;     // k_size
        self.file.write_all(key)?;                          // key

        Ok(())
    }

    pub fn flush(&mut self) -> IoResult<()> {
        self.file.flush()
    }

    pub fn from_path(path: &Path) -> IoResult<Wal> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(Wal {
            path: path.to_owned(),
            file,
        })
    }

    pub fn load_from_dir(dir: &Path) -> IoResult<(Wal, memtable::MemTable)> {
        let mut wal_files = utils::get_files_by_ext(dir, "wal");
        wal_files.sort();

        let mut new_mem_table = memtable::MemTable::new();
        let mut new_wal = Self::new(dir)?;
        for wal_file in wal_files.iter() {
            if let Ok(wal) = Self::from_path(wal_file) {
                for entry in wal.into_iter() {
                    if entry.deleted {
                        new_mem_table.delete(entry.key.as_slice(), entry.timestamp);
                        new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
                    } else {
                        new_mem_table.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().unwrap().as_slice(),
                            entry.timestamp,
                        );
                        new_wal.set(
                            entry.key.as_slice(),
                            entry.value.unwrap().as_slice(),
                            entry.timestamp,
                        )?;
                    }
                }
            }
        }

        new_wal.flush().unwrap();
        wal_files.into_iter().for_each(|f| remove_file(f).unwrap());

        Ok((new_wal, new_mem_table))
    }

}