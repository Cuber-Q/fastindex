package fastindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"syscall"
)

// Read the data file, fetch <key,value_size,value_position> of each k-v pair, and
// write it into a small index file. There should be a lot of small index file,
// and each one of them just contains a shard of key, such as shard the key by key%1000.
// When data file has been fetched into small index files, we will sort index files concurrently.
// When index files have been handled successfully, map all of them to memory using os.mmap()
// meanwhile the data file also be mapped into memory using os.mmap(), so that we don't have to
// care about loading disk page into memory and discarding old memory page when memory is not enough.
//

const fixIndexItemSize = 24

type FastIndex struct {
	dir      string
	shardNum int
	shards   []*IndexShard
}

type IndexShard struct {
	dir      string
	fileName string
	shard    int

	buf          *bytes.Buffer
	writeBufSize int
	totalSize    int64

	file     *os.File
	fileSize int64
	items    items

	// mmap indexFile's data
	dataRef []byte
}

func NewFastIndex(dir string, shardNum int) *FastIndex {
	fidx := &FastIndex{
		dir:      dir,
		shardNum: shardNum,
	}

	fidx.shards = make([]*IndexShard, shardNum)
	for i := 0; i < shardNum; i++ {
		fidx.shards[i] = NewIndexShard(dir, i)
	}

	return fidx
}

func NewIndexShard(dir string, shard int) *IndexShard {
	idx := &IndexShard{
		dir:          dir,
		shard:        shard,
		writeBufSize: int(16 * KB),
	}

	idx.fileName = dir + "/index_" + strconv.Itoa(shard) + ".idx"
	createDirIfNotExist(dir)
	file, e := os.Create(idx.fileName)
	if e != nil {
		panic(e)
	}
	idx.file = file

	idx.buf = bytes.NewBuffer([]byte{})

	return idx
}

func OpenFastIndex(idxDir string, shardNum int) *FastIndex {
	fidx := &FastIndex{
		dir:      idxDir,
		shardNum: shardNum,
	}

	fidx.shards = make([]*IndexShard, shardNum)
	for i := 0; i < shardNum; i++ {
		fidx.shards[i] = OpenIndexShard(idxDir, i)
	}

	return fidx
}

func OpenIndexShard(idxDir string, shard int) *IndexShard {
	idx := &IndexShard{
		dir:          idxDir,
		shard:        shard,
		writeBufSize: int(16 * KB),
	}

	idx.fileName = idxDir + "/index_" + strconv.Itoa(shard) + ".idx"
	createDirIfNotExist(idxDir)
	file, e := os.Open(idx.fileName)
	if e != nil {
		panic(e)
	}

	dfInfo, e := file.Stat()
	if e != nil {
		panic("OpenIndexShard : open indexShardFile error")
	}
	size := dfInfo.Size()

	idx.file = file
	idx.fileSize = size

	return idx
}

// Build builds a FastIndex from existed data file
func (fidx *FastIndex) Build(dataPath string, readBufSize int) {
	dfile, e := os.Open(dataPath)
	defer dfile.Close()

	if e != nil {
		panic("Build index : open dataFile error")
	}
	dfInfo, e := dfile.Stat()
	if e != nil {
		panic("Build index : open dataFile error")
	}
	size := dfInfo.Size()

	var fReadOff int64 = 0
	var kvReadOff int64 = 0
	var valuePos int64 = 0
	buf := make([]byte, readBufSize)
	valuePosByte := make([]byte, 8)
	for fReadOff < size {
		// reset kv read offset
		kvReadOff = 0
		n, e := dfile.ReadAt(buf, fReadOff)
		if e != nil {
			//panic("Build index: read dataFile error")
		}

		len := int64(n)
		for kvReadOff < len {
			// next read can't read whole kv pair's size columns, reload buf
			if len-kvReadOff-24 < 0 {
				break
			}

			// write into indexShard
			keyByte, key, valueSizeByte, valueSize := fidx.readKV(buf, kvReadOff)

			// current read can'r read a whole k-v pair, reload buf
			if len-kvReadOff-24-valueSize < 0 {
				break
			}

			// valuePos is the absolute position of current value in the dataFile
			valuePos = fReadOff + kvReadOff + 24

			// shard by key and write indexShard
			shard := key % int64(fidx.shardNum)
			binary.BigEndian.PutUint64(valuePosByte, uint64(valuePos))
			fidx.shards[shard].Write(keyByte, valueSizeByte, valuePosByte)

			// keep going kvRead
			kvReadOff += 24 + valueSize
		}

		fReadOff += kvReadOff
	}

	// write every indexShard's remain data
	sortBuf := make([]byte, int(2*size/int64(fidx.shardNum)))
	for _, idxShard := range fidx.shards {
		idxShard.writeCompletely()
		idxShard.sort(&sortBuf)
		idxShard.file.Close()
	}
}

func (fidx *FastIndex) readKV(buf []byte, readOff int64) ([]byte, int64, []byte, int64) {
	keySizeByte := buf[readOff : readOff+8]
	keySize := int64(binary.BigEndian.Uint64(keySizeByte))
	readOff += 8

	keyByte := buf[readOff : readOff+keySize]
	key := int64(binary.BigEndian.Uint64(keyByte))
	readOff += keySize

	valueSizeByte := buf[readOff : readOff+8]
	valueSize := int64(binary.BigEndian.Uint64(valueSizeByte))
	readOff += 8 + valueSize

	return keyByte, key, valueSizeByte, valueSize
}

// FindLoop query indexShard and returns the valuePos of the key, or empty []byte if key not exists
func (fidx *FastIndex) Find(key int64) (int64, int64) {
	shard := key % int64(fidx.shardNum)
	return fidx.shards[shard].Find(key)
}

func (idx *IndexShard) Write(key []byte, valueSize []byte, valuePos []byte) {
	idx.buf.Write(key)
	idx.buf.Write(valueSize)
	idx.buf.Write(valuePos)

	len := idx.buf.Len()
	if len >= idx.writeBufSize {
		if _, e := idx.buf.WriteTo(idx.file); e != nil {
			fmt.Errorf("write to index_%d error", idx.shard)
		}
		idx.totalSize += int64(len)
	}
}

func (idx *IndexShard) writeCompletely() {
	len := idx.buf.Len()
	if len > 0 {
		if _, e := idx.buf.WriteTo(idx.file); e != nil {
			fmt.Errorf("write to index_%d error", idx.shard)
		}
		idx.totalSize += int64(len)
	}
}

// sort sorts small indexShard file, with O(N*logN)
func (idx *IndexShard) sort(buf *[]byte) {
	//buf = buf[:]
	fInfo, e := idx.file.Stat()
	if e != nil {
		fmt.Errorf("get file info error")
	}

	size := fInfo.Size()

	buffer := bytes.NewBuffer([]byte{})
	buffer.ReadFrom(idx.file)
	cnt, e := idx.file.ReadAt(*buf, 0)
	if e != nil {
		fmt.Println("error when read index file, cnt:", cnt, ", e:", e)
		return
	}

	// sort indexShard
	itemNum := size / fixIndexItemSize
	idx.items = make(items, itemNum)
	var i int64 = 0
	var offset int64 = 0
	for ; i < itemNum; i++ {
		offset = fixIndexItemSize * i
		idx.items[i] = convertByteToItem(buf[offset : offset+fixIndexItemSize])
	}
	sort.Sort(idx.items)

	// write back sorted indexShard
	idx.writeBack(buf)
}

type items []*indexItem
type indexItem struct {
	key  int64
	vsz  int64
	vpos int64
}

// Len is the number of elements in the collection.
func (items items) Len() int {
	return len(items)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (items items) Less(i, j int) bool {
	return items[i].key <= items[j].key
}

// Swap swaps the elements with indexes i and j.
func (items items) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
}

func convertByteToItem(buf []byte) *indexItem {
	if len(buf) != 24 {
		fmt.Errorf("invalid data size:%d, expected 24", len(buf))
		return nil
	}

	item := &indexItem{}
	off := 0

	item.key = int64(binary.BigEndian.Uint64(buf[off : off+8]))
	off += 8

	item.vsz = int64(binary.BigEndian.Uint64(buf[off : off+8]))
	off += 8

	item.vpos = int64(binary.BigEndian.Uint64(buf[off : off+8]))
	return item
}

func (idx *IndexShard) writeBack(buf *[]byte) {
	var offset int64 = 0
	_buf := make([]byte, 8)
	for _, item := range idx.items {
		binary.BigEndian.PutUint64(_buf, uint64(item.key))
		copy(*buf[offset:], _buf)

		offset += 8
		binary.BigEndian.PutUint64(_buf, uint64(item.vsz))
		copy(*buf[offset:], _buf)

		offset += 8
		binary.BigEndian.PutUint64(_buf, uint64(item.vpos))
		copy(*buf[offset:], _buf)

		offset += 8
	}

	if n, e := idx.file.WriteAt(*buf, 0); e != nil {
		fmt.Errorf("writeBack indexShard error:%s, writed n:%d", e, n)
	}
}

// Find using mmap to reduce concern of memory's alloc and free
func (idx *IndexShard) Find(key int64) (int64, int64) {
	// init mmap
	if len(idx.dataRef) == 0 {
		b, err := syscall.Mmap(int(idx.file.Fd()), 0, int(idx.fileSize), syscall.PROT_READ, syscall.MAP_SHARED)

		if err != nil {
			// todo
		}

		idx.dataRef = b

		// Advise the kernel that the mmap is accessed randomly.
		if err := madvise(b, syscall.MADV_RANDOM); err != nil {
			fmt.Errorf("madvise: %s", err)
		}
	}

	_buf := make([]byte, 8)
	binary.BigEndian.PutUint64(_buf, uint64(key))

	// binary search with time complex as O(logN)
	index := sort.Search(len(idx.dataRef)/24, func(i int) bool {
		result := bytes.Compare(idx.dataRef[i*24:i*24+8], _buf)
		return result != -1
	})
	if index <= 0 {
		return -1, 0
	}

	// convert vsize, vpos from []byte to int64
	offset := index * 24
	vsize := int64(binary.BigEndian.Uint64(idx.dataRef[offset+8 : offset+16]))
	vpos := int64(binary.BigEndian.Uint64(idx.dataRef[offset+16 : offset+24]))
	return vsize, vpos
}
