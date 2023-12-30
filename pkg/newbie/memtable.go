package newbie

import (
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

// A red-black tree is a self-balancing binary search tree, and it provides efficient insertion, deletion, and search operations with a guaranteed logarithmic time complexity.
type MemTable struct {
	entries *treemap.Map 
	maxSize uint32
}

func InitMemTable(max_size uint32) (*MemTable, error) {
	memtable := &MemTable{
		entries: treemap.NewWith(utils.StringComparator),
		maxSize: max_size,
	}
	return memtable, nil
}

func (mt *MemTable) Len() int {
	return mt.entries.Size()
}

func (mt *MemTable) Set(key string,value []byte){
	mt.entries.Put(key,value)
}

func (mt *MemTable) Get(key string) ([]byte){
	value, found := mt.entries.Get(key)
	if found{
		return value.([]byte)
	}
	return nil
}

func (mt *MemTable) Clear(){
	mt.entries.Clear()
}

func (mt *MemTable) CapacityReached() bool {
	return mt.entries.Size() >= int(mt.maxSize)
}