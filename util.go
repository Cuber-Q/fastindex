package main

import (
	"os"
	"strconv"
	"syscall"
	"unsafe"
)

func createDirIfNotExist(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			panic(err)
		}
	}
}

// NOTE: This function is copied from stdlib because it is not available on darwin.
func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}

func readableTime(time int) string {
	if time < 1000 {
		return strconv.Itoa(time) + "ns"
	} else if time < 1e3 {
		return strconv.Itoa(time/1e3) + "us"
	} else if time < 1e6 {
		return strconv.Itoa(time/1e6) + "ms"
	} else if time < 1e9 {
		return strconv.Itoa(time/1e9) + "s"
	} else if time < 60*1e9 {
		return strconv.Itoa(time/(1e9*60)) + "min"
	} else if time < 60*60*1e9 {
		return strconv.Itoa(time/(1e9*60*60)) + "h"
	} else {
		return "a century..."
	}
}
