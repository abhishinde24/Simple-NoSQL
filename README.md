# Simple-NoSQL
Implementing NoSQL database from Scratch with Minimum Complexity while Covering Fundamental characteristics for database.

## Working
- Memtable for storing data for quick access before flushing into SStable
- SStable for storing flushed memtable data into persistent file
- LSMTable is array of SSTable table ,Compaction of SStables into smaller one happen after certain threshold
