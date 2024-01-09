package newbie

import (
	"fmt"
	"os"
	"sort"
)
type LSMTree struct {
	SSTables []*SSTable
}

func (lsm* LSMTree) length()(int){
	return	len(lsm.SSTables)	 
}

func initLSMTree() *LSMTree {
	activeDataFiles := find(DATAPATH,".newbie")
		// sorting file paths
	sort.Strings(activeDataFiles)

	lsmTree := &LSMTree{
		SSTables: []*SSTable{},
	}
	for _, s := range(activeDataFiles){
		file, err := os.OpenFile(s, os.O_RDWR, 0644)
		if err != nil {
			panic(fmt.Errorf("Error opening file %s: err - %s",s,err))
		}
		ssTable := &SSTable{path: s,fd: file}
		lsmTree.SSTables = append(lsmTree.SSTables, ssTable)
	}
	return lsmTree
}

func (lsm* LSMTree)compactLSMTree(){
	var n = lsm.length()

	if n < 2{
		return
	}

	// mergering consecutive SStable
	var sstables []*SSTable
	for i := 1; i < n ; i+=2 {

		sstable, err := MergeSSTable(*lsm.SSTables[i-1],*lsm.SSTables[i])
		if err != nil {
			panic(fmt.Errorf("Error - %s \n",err))
		}
		sstables = append(sstables,sstable)
		lsm.SSTables[i].fd.Close()
		lsm.SSTables[i - 1].fd.Close()
		lsm.SSTables[i].DeleteSSTable()
		lsm.SSTables[i - 1].DeleteSSTable()

	}
	if n % 2 != 0{
		sstables = append(sstables,lsm.SSTables[n-1])
	}

	lsm.SSTables = sstables
}
