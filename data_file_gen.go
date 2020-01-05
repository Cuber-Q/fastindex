package fastindex

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"strconv"
)

// To generate raw data file which is formatted as
// <key_size, key, value_size, value>, which key_zie and value_size
// have 8-byte length. The key will be as most 1KB length and the value is
// at most 2MB length.
//

const (
	KB int64 = 1024
	MB       = 1024 * KB
	GB       = 1024 * MB
	TB       = 1024 * GB
)

type DataFileGen struct {
	maxKey         int64
	maxValueLength int64
	// maxSize is max size of the whole data file. No more than 1TB
	maxSize      int64
	writeBufSize int

	// data file's path
	path string
	file *os.File
}

func (self *DataFileGen) generate() error {
	if len(self.path) == 0 {
		panic("data file path can't be empty")
	}

	// create data file
	createDirIfNotExist(self.path)
	dataFilePath := self.path
	f, e := os.Create(dataFilePath)
	if e != nil {
		return e
	}

	var totalSize int64 = 0
	buf := bytes.NewBuffer([]byte{})

	// write data into file
	for totalSize < self.maxSize {

		// fill buf with random data
		self.fillBuf(buf)

		// calc total length that has been wrote
		len := buf.Len()

		// flush into disk when the buf is big enough
		if len > self.writeBufSize/2 {
			//fmt.Println("writeTo file success, size:", buf.Len())
			if _, e := buf.WriteTo(f); e != nil {
				return e
			}
			totalSize += int64(len)
		}
	}

	return nil
}

func (self *DataFileGen) fillBuf(buf *bytes.Buffer) int64 {
	key := rand.Int63n(self.maxKey)
	keyStr := strconv.FormatInt(key, 10)
	valueSize := rand.Int63n(self.maxValueLength)

	vRepeat := int(valueSize / int64(len(keyStr)))
	valueSize = int64(vRepeat * len(keyStr))

	_buf := make([]byte, 8)

	// key_size
	binary.BigEndian.PutUint64(_buf, uint64(8))
	buf.Write(_buf)

	// key
	binary.BigEndian.PutUint64(_buf, uint64(key))
	buf.Write(_buf)

	// value_size
	binary.BigEndian.PutUint64(_buf, uint64(valueSize))
	buf.Write(_buf)

	// value
	for i := 0; i < vRepeat; i++ {
		buf.WriteString(keyStr)
	}

	// k-v pair's length
	return 8 + 8 + 8 + valueSize
}

func randomGen(maxLength int64) ([]byte, []byte) {
	v := rand.Int63n(maxLength)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, v)
	return bytesBuffer.Bytes(), bytesBuffer.Bytes()
}
