package fastindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func Test_gen(t *testing.T) {
	gen := &DataFileGen{
		maxKey:         30,
		maxValueLength: KB,
		writeBufSize:   int(MB),
		maxSize:        1 * GB,
		path:           "/Users/Cuber_Q/goproj/fastindex/data",
	}

	if e := gen.generate(); e != nil {
		panic(e)
	}
}

func Test_buffer(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	//buf := bytes.NewBufferString("")
	buf.WriteString("124315451435245")
	buf.WriteString("ncadknvdsnf")

	_buf := make([]byte, 8)
	binary.BigEndian.PutUint64(_buf, uint64(1024))
	v := int64(binary.BigEndian.Uint64(_buf))

	fmt.Println(v)
	buf.Write(_buf)

	fmt.Println(buf)
}
