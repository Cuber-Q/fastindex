package main

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func Test_read(t *testing.T) {
	reader := &Reader{
		readBufSize: 1024 * 1024,
		fileName:    "/Users/Cuber_Q/goproj/fastindex/data/data.d",
	}
	buf := reader.read()
	var offset int64 = 0

	for i := 0; i < 10; i++ {
		keySizeByte := buf[offset : offset+8]
		keySize := int64(binary.BigEndian.Uint64(keySizeByte))
		offset += 8

		keyByte := buf[offset : offset+keySize]
		key := int64(binary.BigEndian.Uint64(keyByte))
		offset += keySize

		valueSizeByte := buf[offset : offset+8]
		valueSize := int64(binary.BigEndian.Uint64(valueSizeByte))
		offset += 8

		value := string(buf[offset : offset+valueSize])
		offset += valueSize

		fmt.Println("key:", key, ", value:", value)
	}

}
