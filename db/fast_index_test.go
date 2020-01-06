package db

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
)

const indexDir = "/Users/Cuber_Q/goproj/fastindex/index"
const dataFilePath = "/Users/Cuber_Q/goproj/fastindex/data/data.d"

func Test_index_shard_create(t *testing.T) {
	testBuf := make([]byte, 8)
	for i := 0; i < 1000; i++ {
		idx := NewIndexShard(indexDir, i)
		for j := 0; j < 1024*1024; j++ {
			idx.Write(testBuf, testBuf, testBuf)
		}
	}
}

func Test_fast_index_create(t *testing.T) {
	shardNum := 1000
	fidx := NewFastIndex(indexDir, shardNum)
	wg := sync.WaitGroup{}
	wg.Add(shardNum)
	for i := 0; i < shardNum; i++ {
		go func(shard int) {
			testBuf := make([]byte, 8)
			for j := 0; j < 1024*1024; j++ {
				fidx.shards[shard].Write(testBuf, testBuf, testBuf)
			}
			fidx.shards[shard].writeCompletely()
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println("fastIndex created success!")
}

func Test_index_shard_sort(t *testing.T) {
	//shardNum := 1000

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 20; i++ {
		// create a indexShard
		go func(shard int) {
			var key int64 = 0
			for i := 0; i < 50; i++ {
				oneShard(shard*50+i, key)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	// print sorted items
	//for _,item := range idx.items {
	//	fmt.Println(item.key)
	//}
}

func oneShard(shard int, key int64) {
	_buf := make([]byte, 8)
	idx := NewIndexShard(indexDir, shard)
	for j := 0; j < 1024*1024; j++ {
		key = rand.Int63n(1 << 31)
		binary.BigEndian.PutUint64(_buf, uint64(key))
		idx.Write(_buf, _buf, _buf)
	}
	idx.writeCompletely()
	// sort indexShard
	idx.sort()
}

func Test_fast_index_build(t *testing.T) {
	fidx := NewFastIndex(indexDir, 10)
	fidx.Build(dataFilePath, 0)
}

func Test_fast_index_random_find(t *testing.T) {
	fidx := OpenFastIndex(indexDir, 10)
	failCnt := 0
	for i := 0; i < 100; i++ {
		key := rand.Int63n(1 << 30)
		vsize, _ := fidx.Find(key)
		if vsize < 0 {
			failCnt++
		}
	}

	fmt.Println("failCnt:", failCnt)
}

func Test_fast_index_point_find(t *testing.T) {
	fidx := OpenFastIndex(indexDir, 1024)
	keys := []int64{134020434, 164029137, 957743134, 958990240}
	dataF, _ := os.Open(dataFilePath)

	failCnt := 0
	value := make([]byte, KB)
	for _, k := range keys {
		vsize, vpos := fidx.Find(k)
		if vsize < 0 {
			failCnt++
			continue
		}

		//fmt.Println("vpos:", vpos)
		n, _ := dataF.ReadAt(value, vpos)
		if n != 0 {
			v := string(value[:vsize])
			fmt.Println("key:", k, ", v:", v)
		} else {
			fmt.Println("err at key:", k)
		}
		value = value[:]
	}
}

func Test_print_index_shard(t *testing.T) {
	fidx := OpenFastIndex(indexDir, 10)
	// init dataref
	fidx.shards[0].Find(1210)

	buf := fidx.shards[0].dataRef
	_buf := make([]byte, 24)
	var offset int64 = 0

	for i := 0; i < len(buf)/24; i++ {
		fmt.Print("index:", i, ", ")
		offset = int64(i * 24)
		_buf = buf[offset : offset+8]
		offset += 8
		fmt.Print("key:", int64(binary.BigEndian.Uint64(_buf)), ", ")

		_buf = buf[offset : offset+8]
		offset += 8
		fmt.Print("valueSize:", int64(binary.BigEndian.Uint64(_buf)), ", ")

		_buf = buf[offset : offset+8]
		offset += 8
		fmt.Print("valuePos:", int64(binary.BigEndian.Uint64(_buf)), ", ")
		fmt.Println()
	}
}
