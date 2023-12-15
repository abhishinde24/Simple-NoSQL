# implementation

- interface for interacting with database
- Memtable for storing data into RAM
- SSTable to flush data to disk (file) when memtable is full
- LSM tree to search for key into disk efficiently 
