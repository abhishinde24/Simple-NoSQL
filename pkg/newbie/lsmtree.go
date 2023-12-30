package newbie

import (
	"fmt"
	"os"
	"sort"
)
type LSMTree struct {
	SSTables []*SSTable
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

