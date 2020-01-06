package db

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

type DB struct {
	baseDir string

	dataFileDir  string
	indexFileDir string

	dataFilePath string

	indexShardNum int

	maxDataSize    int64
	maxKey         int64
	maxValueLength int64

	// buffer size
	writeBufSize int
	readBufSize  int

	dataFile *os.File
	fidx     *FastIndex
}

func OpenDB(baseDir string) *DB {
	db := &DB{baseDir: baseDir}
	db.dataFileDir = baseDir + "/data/"
	db.dataFilePath = db.dataFileDir + "data.d"
	db.indexFileDir = baseDir + "/index/"
	db.maxDataSize = 16 * GB
	db.indexShardNum = 1000
	db.maxKey = 1 << 30
	db.maxValueLength = KB

	// using for dataGen
	db.writeBufSize = int(MB)

	// using for read dataFile when building index
	db.readBufSize = int(64 * MB)
	return db
}

// create dataFile and indexFiles
func (db *DB) CreateData(size int64) {
	// create dataFile
	db.maxDataSize = size
	dataGen := &DataFileGen{
		maxKey:         db.maxKey,
		maxValueLength: db.maxValueLength,
		maxSize:        db.maxDataSize,
		writeBufSize:   db.writeBufSize,
		path:           db.dataFilePath,
	}

	if e := dataGen.generate(); e != nil {
		fmt.Errorf("dataGen.generate error: %s", e)
	}

}

func (db *DB) CreateIndex() {
	// create indexFiles
	fidx := NewFastIndex(db.indexFileDir, db.indexShardNum)
	fidx.Build(db.dataFilePath, db.readBufSize)
}

func (db *DB) InitFind() {
	df, e := os.Open(db.dataFilePath)
	if e != nil {
		panic(e)
	}

	db.dataFile = df
	db.fidx = OpenFastIndex(db.indexFileDir, db.indexShardNum)
}

func (db *DB) Find(key int64) string {
	vBuf := make([]byte, db.maxValueLength)
	vsize, vpos := db.fidx.Find(key)
	n, _ := db.dataFile.ReadAt(vBuf, vpos)
	if n <= 0 {
		//fmt.Println("find error at key:", key)
		return "error"
	}
	v := string(vBuf[:vsize])
	return v
}

func (db *DB) FindLoop(loopCnt int) int {
	vBuf := make([]byte, db.maxValueLength)
	totalTime := 0
	for i := 0; i < loopCnt; i++ {
		start := time.Now()

		key := rand.Int63n(db.maxKey)
		_, vpos := db.fidx.Find(key)
		n, _ := db.dataFile.ReadAt(vBuf, vpos)
		if n <= 0 {
			//fmt.Println("find error at key:", key)
			continue
		}
		end := time.Now()
		totalTime += int(end.Sub(start))
		//fmt.Println("key:", key, ", v:", v)
	}
	return totalTime / loopCnt
}
