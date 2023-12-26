package newbie

import "fmt"

const (
	DATAPATH = "./data/"
)

type DB struct {
	mem_table MemTable
}

func Init(max_inmemory_size uint32) (*DB, error) {
	// Initializing DB class
	
	memtable, err := InitMemTable(max_inmemory_size)

	if err != nil{
		return nil,fmt.Errorf("error while initializing memtable")
	}

	db := &DB{
		mem_table: *memtable,
	}

	return db, nil
}

func (db *DB) Set(key string,value []byte)( error){
	db.mem_table.Set(key,value)
	return nil
}

func (db *DB) Get(key string)( 	[]byte, error){
	return db.mem_table.Get(key), nil
}