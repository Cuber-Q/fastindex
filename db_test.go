package main

import (
	"fmt"
	"sync"
	"testing"
)

const dir = "/Users/Cuber_Q/goproj/fastindex"

func Test_db_create_data(t *testing.T) {
	db := OpenDB(dir)
	db.CreateData()
}

func Test_db_create_index(t *testing.T) {
	db := OpenDB(dir)
	db.CreateIndex()
}

func Test_db_find(t *testing.T) {
	db := OpenDB(dir)
	db.InitFind()

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			db.FindLoop(10)
			wg.Done()
		}()
	}
	wg.Wait()
}

func Test_db_find_one(t *testing.T) {
	db := OpenDB(dir)
	db.InitFind()

	var k int64 = 413002649
	v := db.Find(k)
	fmt.Println("key:", k, ", v:", v)
}
