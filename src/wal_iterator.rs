use crate::wal;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, Result as IoResult};
use std::path::{PathBuf};

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

pub struct WalIterator {
    reader: BufReader<File>,
}

impl WalIterator {

    pub fn new(path: PathBuf) -> IoResult<WalIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WalIterator { reader })
    }

    fn read_size(&mut self) -> Option<usize> {
        let mut buff = [0; 8];
        match self.reader.read_exact(&mut buff) {
            Ok(()) => { Some(usize::from_le_bytes(buff)) },
            Err(_) => { None },
        }
    }

    fn read_vec(&mut self, size: usize) -> Option<Vec<u8>> {
        let mut result = vec![0; size];
        match self.reader.read_exact(&mut result) {
            Ok(()) => { Some(result) },
            Err(_) => { None },
        }
    }

    fn read_bool(&mut self) -> Option<bool> {
        let mut buff = [0; 1];
        match self.reader.read_exact(&mut buff) {
            Ok(()) => { Some(buff[0] != 0) },
            Err(_) => { None },
        }
    }

    fn read_timestamp(&mut self) -> Option<u128> {
        let mut buff = [0; 16];
        match self.reader.read_exact(&mut buff) {
            Ok(()) => { Some(u128::from_le_bytes(buff)) },
            Err(_) => { None },
        }
    }
}

impl Iterator for WalIterator {
    type Item = wal::WalEntry;
    
    fn next(&mut self) -> Option<Self::Item> {
        let deleted = self.read_bool()?;
        let timestamp = self.read_timestamp()?;
        let key_size = self.read_size()?;
        let key = self.read_vec(key_size)?;
        let mut value = None;
        if !deleted {
            let val_size = self.read_size()?;
            value = Option::from(self.read_vec(val_size)?);
        }
        Some(wal::WalEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}