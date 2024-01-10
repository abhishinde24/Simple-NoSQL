package newbie

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

const(
	sstable_max_size = 1001	
)

type SSTable struct {
	fd *os.File
	path string
	previousEntryKey string
	size uint64
}

type SSTableEntry struct {
	Key   string
	Value []byte
}

func (s SSTableEntry) IsEmpty() bool{
	return reflect.DeepEqual(s,SSTableEntry{})
}

func extractMapEntry(entryMap map[string][]byte) (string, []byte) {
	for key, value := range entryMap {
		return key, value
	}
	return "", nil
}

func NewSegmentEntry(entryMap map[string][]byte) *SSTableEntry {
	key, value := extractMapEntry(entryMap)
	return &SSTableEntry{Key: key, Value: value}
}

func NewSSTable(path string, fd *os.File) *SSTable {
	// creating new file
	fd , err := os.Create(path)
	if err != nil {
				panic(fmt.Errorf("Error while creating file"))
	}
	s := &SSTable{
		path: path,
		fd:   fd,
	}
	return s
}

func FlushMemTableToSSTable(path string,memTable *MemTable)(*SSTable){
	ssTable := make_persistent_sstable(DATAPATH)
	iterator := memTable.entries.Iterator()
	for iterator.Next(){
		ssTable.addEntry(&SSTableEntry{iterator.Key().(string),iterator.Value().([]byte)})
	}
	return ssTable
}
func make_persistent_sstable(base_path string)(*SSTable){
	dir, err := os.Getwd()
	if err != nil {
		panic(fmt.Errorf("Error while creating file"))
	}
	base_path = filepath.Join(dir,base_path)	
	currentTime := time.Now().UnixNano()
	if _, err := os.Stat(base_path); os.IsNotExist(err) {
    	err := os.Mkdir(base_path, 0755)
		if err != nil{
			panic(fmt.Errorf("Error while creating file path"))
		}
	}

	fileExist := true 
	sstable_filepath := ""
	// sstable_filepath := filepath.Join(base_path,"sstable_" + fmt.Sprintf("%d",currentTime) + ".newbie")
	for fileExist {
		sstable_filepath = filepath.Join(base_path,"sstable_" + fmt.Sprintf("%d",currentTime) + ".newbie")
		fileExist,err = Exists(sstable_filepath)
		if err != nil{
			fmt.Println("error while checking ",err)
			panic(fmt.Sprintf("Error while checking file exist or not , File path %v",sstable_filepath))
		}
		currentTime = time.Now().UnixNano()
	}

	return NewSSTable(sstable_filepath,nil)
}

func (s *SSTable) reachedEOF() bool {
	curPos, _ := s.fd.Seek(0, io.SeekCurrent)
	// Create a bufio.Scanner to read lines from the file.
    scanner := bufio.NewScanner(s.fd)
	hasNextLine := scanner.Scan()
	s.fd.Seek(curPos, io.SeekStart) // moving cursor to original position
	
	return !hasNextLine 
}

func (s *SSTable) readEntry(line string) *SSTableEntry {
    
	var entryMap map[string][]byte
	
	if line == ""{
		return NewSegmentEntry(entryMap)
	}

	err := json.Unmarshal([]byte(line), &entryMap)
	if err != nil {
		panic(err)
	}

	return NewSegmentEntry(entryMap)
}

func (s *SSTable) addEntry(entry *SSTableEntry) int64 {

	key := entry.Key
	value := entry.Value

	if s.previousEntryKey != "" && s.previousEntryKey > key {
		panic(fmt.Errorf(fmt.Sprintf("Tried to insert %v, but previous entry %v is bigger", key, s.previousEntryKey)))
	}

	jsonStr, err := json.Marshal(map[string][]byte{key: value})
	if err != nil {
		panic(err)
	}

	s.previousEntryKey = key
	pos, err := s.fd.Seek(0, os.SEEK_CUR)
	if err != nil {
		panic(err)
	}
	_, err = s.fd.WriteString(string(jsonStr) + "\n")
	if err != nil {
		panic(err)
	}
	s.size++
	return pos
}

func (s* SSTable) Search(query string, offset int64) *SSTableEntry {
	s.fd.Seek(offset, io.SeekStart) // moving cursor to offset position
	fileScanner := bufio.NewScanner(s.fd)
    fileScanner.Split(bufio.ScanLines)	
	
	for fileScanner.Scan() {
		nextLine := fileScanner.Text()
		entry := s.readEntry(nextLine)
		if entry.Key == query{
			return entry
		}	
		if entry.Key > query{
			break
		}
    }

	return nil
}
func (s* SSTable)GetSliceOfSSTableEntry()([]*SSTableEntry){
	s.fd.Seek(0, io.SeekStart) // moving cursor to offset position
	var ssTableEntrys []*SSTableEntry

	fileScanner := bufio.NewScanner(s.fd)
    fileScanner.Split(bufio.ScanLines)	
	
	for fileScanner.Scan() {
				nextLine := fileScanner.Text()
		entry := s.readEntry(nextLine)
		ssTableEntrys = append(ssTableEntrys,entry)
	}

	return ssTableEntrys
}

func (s* SSTable) DeleteSSTable()(error){
	err := os.Remove(s.path)
	if err != nil {
		// If there is an error, print it
		fmt.Println("Error:", err)
		return err
	}
	s = nil
	return nil
}
func MergeSSTable(s1 SSTable,s2 SSTable)(*SSTable,error){
	memtable ,err := InitMemTable(sstable_max_size)	
	if err != nil{
		return nil,err
	}
	// merging using merge two sorted array method
	s1_slice := s1.GetSliceOfSSTableEntry()
	s2_slice := s2.GetSliceOfSSTableEntry()
	n := len(s1_slice)
	m := len(s2_slice)
	i , j  := 0,0
	for  i < n && j < m  {
		if(s1_slice[i].Key == s2_slice[j].Key){
			if string(s2_slice[j].Value) != (TOMBSTONE){
				memtable.Set(s2_slice[j].Key,s2_slice[j].Value)
			}
			i++
			j++
		}else if (s1_slice[i].Key < s2_slice[j].Key){
			if string(s1_slice[i].Value) != (TOMBSTONE){
				memtable.Set(s1_slice[i].Key,s1_slice[i].Value)
			}
			i++
		}else{
			if string(s2_slice[j].Value) != (TOMBSTONE){
				memtable.Set(s2_slice[j].Key,s2_slice[j].Value)
			}
			j++
		}
	}
	// make empty s1 slice
	for i < n {
			if string(s1_slice[i].Value) != (TOMBSTONE){
				memtable.Set(s1_slice[i].Key,s1_slice[i].Value)
			}
			i++
	}

	// make empty s2 slice
	for j < m {
			if string(s2_slice[j].Value) != (TOMBSTONE){
				memtable.Set(s2_slice[j].Key,s2_slice[j].Value)
			}
			j++
	}
	return FlushMemTableToSSTable(DATAPATH,memtable),nil
	
}



