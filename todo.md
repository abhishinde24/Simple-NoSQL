# implementation

- interface for interacting with database
- Memtable for storing data into RAM
- SSTable to flush data to disk (file) when memtable is full
- LSM tree to search for key into disk efficiently 
- adding tombstone to delete operations
- adding sstable compaction process after certain threshold
- db.close() method to close db(save data in persistent format)

## to-do

- adding bloom filter to check is key exist or not before start checking
- optimising search - find key inside presistent sstable table
- 