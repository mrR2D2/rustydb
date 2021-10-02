
pub struct MemTable {
    entries: Vec<MemTableEntry>,
    size: usize,
}

pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

impl MemTable {
    fn new() -> MemTable {
        MemTable {
            entries: Vec::new(),
            size: 0,
        }
    }

    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key, |e| e.key.as_slice())
    }

    fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted: false,
        };
        match self.get_index(key) {
            Ok(idx) => {
                self.entries[idx] = entry;
                if let Some(v) = self.entries[idx].value.as_ref() {
                    if value.len() < v.len() {
                        self.size -= v.len() - value.len();
                    } else {
                        self.size += value.len() - v.len();
                    }
                }
            }
            Err(idx) => {
                self.entries.insert(idx, entry);
                self.size += key.len() + value.len() + 16 + 1;
            }
        }
    }

    fn delete(&mut self, key: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: None,
            timestamp,
            deleted: true,
        };
        match self.get_index(key) {
            Ok(idx) => {
                self.entries[idx] = entry;
                if let Some(v) = self.entries[idx].value.as_ref() {
                    self.size -= v.len();
                }
            }
            Err(idx) => {
                self.entries.insert(idx, entry);
                self.size += key.len() + 16 + 1;
            }
        }
    }

    fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.entries[idx]);
        }
        None
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::memtable::MemTable;

    #[test]
    fn test_mem_table_get_not_exists() {
        let mut mem_table = MemTable::new();
        mem_table.set(b"key 1", b"value", 0);

        let res = mem_table.get(b"key 2");

        assert_eq!(res.is_some(), false);
    }

    #[test]
    fn test_mem_table_get_exists() {
        let mut mem_table = MemTable::new();
        mem_table.set(b"key 1", b"some value", 0);
        mem_table.set(b"key 2", b"some value", 1);

        let res = mem_table.get(b"key 2");

        assert_eq!(res.is_some(), true);
        let res = res.unwrap();
        assert_eq!(res.key, b"key 2");
        assert_eq!(res.value.as_ref().unwrap(), b"some value");
        assert_eq!(res.timestamp, 1);
        assert_eq!(res.deleted, false);
    }

    #[test]
    fn test_mem_table_delete_not_exists() {
        let mut mem_table = MemTable::new();
        mem_table.set(b"key 1", b"value", 0);

        mem_table.delete(b"key 2", 1);

        let entry = mem_table.get(b"key 2").unwrap();
        assert_eq!(entry.key, b"key 2");
        assert_eq!(entry.value, None);
        assert_eq!(entry.timestamp, 1);
        assert_eq!(entry.deleted, true);

        assert_eq!(mem_table.len(), 2);
        assert_eq!(mem_table.entries[1].key, b"key 2");
        assert_eq!(mem_table.entries[1].value, None);
        assert_eq!(mem_table.entries[1].timestamp, 1);
        assert_eq!(mem_table.entries[1].deleted, true);
    }

    #[test]
    fn test_mem_table_delete_exists() {
        let mut mem_table = MemTable::new();
        mem_table.set(b"key 1", b"value", 0);

        mem_table.delete(b"key 1", 1);

        let entry = mem_table.get(b"key 1").unwrap();
        assert_eq!(entry.key, b"key 1");
        assert_eq!(entry.value, None);
        assert_eq!(entry.timestamp, 1);
        assert_eq!(entry.deleted, true);

        assert_eq!(mem_table.len(), 1);
        assert_eq!(mem_table.entries[0].key, b"key 1");
        assert_eq!(mem_table.entries[0].value, None);
        assert_eq!(mem_table.entries[0].timestamp, 1);
        assert_eq!(mem_table.entries[0].deleted, true);
    }

    #[test]
    fn test_mem_table_set() {
        let mut mem_table = MemTable::new();

        mem_table.set(b"key 1", b"some value", 0);

        assert_eq!(mem_table.len(), 1);
        assert_eq!(mem_table.entries[0].key, b"key 1");
        assert_eq!(mem_table.entries[0].value.as_ref().unwrap(), b"some value");
        assert_eq!(mem_table.entries[0].timestamp, 0);
        assert_eq!(mem_table.entries[0].deleted, false);
    }

    #[test]
    fn test_mem_table_set_override() {
        let mut mem_table = MemTable::new();
        mem_table.set(b"key 1", b"some value", 0);

        mem_table.set(b"key 1", b"some value upd", 1);

        assert_eq!(mem_table.len(), 1);
        assert_eq!(mem_table.entries[0].key, b"key 1");
        assert_eq!(mem_table.entries[0].value.as_ref().unwrap(),
                   b"some value upd");
        assert_eq!(mem_table.entries[0].timestamp, 1);
        assert_eq!(mem_table.entries[0].deleted, false);
    }
}