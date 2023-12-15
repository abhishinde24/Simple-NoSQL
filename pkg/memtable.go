package newbie

type MemTable struct {
	entries  map[string][]byte
	max_size uint32
}
