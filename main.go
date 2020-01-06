package main

import (
	dbBase "fastindex/db"
	"flag"
	"fmt"
	"sync"
	"time"
)

func main() {
	// create data
	var dir string
	var dataSize string
	var cmd string
	flag.StringVar(&dir, "dir", "", "specify the base dir")
	flag.StringVar(&dataSize, "size", "16G", "specify the dataSize, such as: 4M, 16G, 128G, 1T")
	flag.StringVar(&cmd, "cmd", "", "createData: create data file; createIndex: create indexFile; findTest: testing find k-v")
	flag.Parse()

	if dir == "" {
		fmt.Println("invalid dir")
		return
	}

	if cmd == "createData" {
		size, ok := dbBase.ParseSize(dataSize)
		if !ok {
			fmt.Println("invalid size")
			return
		}
		createData(dir, size, dataSize)
		return
	} else if cmd == "createIndex" {
		createIndex(dir)
		return
	} else if cmd == "findTest" {
		findTest(dir)
		return
	} else {
		fmt.Println("unsupported cmd ")
	}

}

func createData(dir string, size int64, dataSize string) {
	fmt.Println("call createData... ")
	start := time.Now()

	db := dbBase.OpenDB(dir)
	db.CreateData(size)

	end := time.Now()
	costTime := dbBase.ReadableTime(int(end.Sub(start)))
	fmt.Printf("createData successfully. createData size:%v, cost time:%v", dataSize, costTime)
	fmt.Println()
}

func createIndex(dir string) {
	fmt.Println("call createIndex... ")
	start := time.Now()

	db := dbBase.OpenDB(dir)
	db.CreateIndex()

	end := time.Now()
	costTime := dbBase.ReadableTime(int(end.Sub(start)))
	fmt.Println("createIndex successfully. cost time:", costTime)
}

func findTest(dir string) {
	fmt.Println("call findTest... ")
	start := time.Now()

	db := dbBase.OpenDB(dir)
	db.InitFind()

	concurrent := 10
	loopCnt := 1000 * 10
	avgTimes := make([]int, concurrent)

	wg := sync.WaitGroup{}
	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func(curr int) {
			_start := time.Now()
			db.FindLoop(loopCnt)
			_end := time.Now()

			avgTimes[curr] = int(_end.Sub(_start))
			wg.Done()
		}(i)
	}
	wg.Wait()

	end := time.Now()
	costTime := dbBase.ReadableTime(int(end.Sub(start)))

	// avg time
	for i := 1; i < concurrent; i++ {
		avgTimes[0] += avgTimes[i]
	}

	avgTime := dbBase.ReadableTime(avgTimes[0] / (concurrent * loopCnt))
	fmt.Printf("findTest successfully. total op:%d, cost time:%v, avg time:%v", concurrent*loopCnt, costTime, avgTime)
	fmt.Println()
}
