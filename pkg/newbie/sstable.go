package newbie

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
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

func make_persistent_sstable(base_path string)(*SSTable){
	dir, err := os.Getwd()
	if err != nil {
		panic(fmt.Errorf("Error while creating file"))
	}
	base_path = filepath.Join(dir,base_path)	
	currentTime := time.Now()
	if _, err := os.Stat(base_path); os.IsNotExist(err) {
    	err := os.Mkdir(base_path, 0755)
		if err != nil{
			panic(fmt.Errorf("Error while creating file path"))
		}
	}
	sstable_filepath := filepath.Join(base_path,"sstable_" + fmt.Sprintf("%d",currentTime.Unix()) + ".newbie")
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

func (s *SSTable) readEntry() *SSTableEntry {
    
	scanner := bufio.NewScanner(s.fd)
	scanner.Scan()
	line := scanner.Text()

	var entryMap map[string][]byte

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
	for !s.reachedEOF(){
		entry := s.readEntry()
		if entry.Key == query{
			return entry
		}	
		if entry.Key > query{
			break
		}
	}
	return nil

}



