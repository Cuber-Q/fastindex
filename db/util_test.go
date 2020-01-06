package db

import (
	"fmt"
	"testing"
)

func Test_readable_time(t *testing.T) {
	fmt.Println(ReadableTime(32 * 100 * 1000 * 1000))
}

func Test_size_parse(t *testing.T) {
	n, ok := ParseSize("1T")
	if ok {
		fmt.Println(n)
	}
}
