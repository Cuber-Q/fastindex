package db

import "os"

type Reader struct {
	readBufSize int64
	fileName    string
}

func (r *Reader) read() []byte {
	if len(r.fileName) == 0 {
		panic("fileName is empty")
	}

	f, e := os.Open(r.fileName)
	if e != nil {
		panic("open dataFile error")
	}

	if r.readBufSize <= 0 {
		r.readBufSize = 1024 * 1024
	}
	buf := make([]byte, r.readBufSize)
	if _, e := f.Read(buf); e != nil {
		panic(e)
	}
	return buf
}
