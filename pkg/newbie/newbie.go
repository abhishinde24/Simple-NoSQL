package newbie

import (
	"fmt"
)

// uuid for tombstone
const (
	DATAPATH = "../data/"
	TOMBSTONE = "f75bbbae-7c66-53f9-9c49-cc15e10d35db"
)

type DB struct {
	mem_table MemTable
	lsm_tree LSMTree
}

func Init(max_inmemory_size uint32) (*DB, error) {
	// Initializing DB class
	
	memtable, err := InitMemTable(max_inmemory_size)

	if err != nil{
		return nil, fmt.Errorf("error while initializing memtable")
	}
	lsmTree := initLSMTree()

	db := &DB{
		mem_table: *memtable,
		lsm_tree: *lsmTree,
	}

	return db, nil
}

func (db *DB) Set(key string,value []byte)( error){
	db.mem_table.Set(key,value)
	
	if db.mem_table.CapacityReached(){
		// need to flush data from memtable to SStable
		ssTable := make_persistent_sstable(DATAPATH)
		iterator := db.mem_table.entries.Iterator()
		for iterator.Next(){
			ssTable.addEntry(&SSTableEntry{iterator.Key().(string),iterator.Value().([]byte)})
		}
		db.lsm_tree.SSTables = append(db.lsm_tree.SSTables, ssTable)
		//clearing memtable
		db.mem_table.Clear()
	}
	return nil
}

func (db *DB) Get(key string)([]byte, error){
	value := db.mem_table.Get(key) 
	if string(value) == TOMBSTONE{
		return nil,nil		
	}
	if value != nil {
		return value , nil
	}
	// searching in LSM tree
	// iterating in reverse order to check for lastest value
	for i := len(db.lsm_tree.SSTables) - 1; i >=0 ; i-- {
		entry := db.lsm_tree.SSTables[i].Search(key,0)
		if entry != nil {
			value := entry.Value
			if string(value) == TOMBSTONE{
				return nil,nil				
			}			
			return value,nil
		}
	}
	return nil, nil
}

func (db *DB)Delete(key string)(error){
	err := db.Set(key,[]byte(TOMBSTONE))
	if err != nil{
		panic("Error while deleting an entry")
	}
	return nil
}
