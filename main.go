package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

func main() {
	// create data
	var dir string
	var cmd string
	flag.StringVar(&dir, "dir", "/fastindex", "specify the base dir")
	flag.StringVar(&cmd, "cmd", "", "createData: create data file; createIndex: create indexFile; findTest: testing find k-v")
	flag.Parse()

	if cmd == "createData" {
		createData(dir)
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

func createData(dir string) {
	fmt.Println("call createData... ")
	start := time.Now()

	db := OpenDB(dir)
	db.CreateData()

	end := time.Now()
	costTime := readableTime(int(end.Sub(start)))
	fmt.Println("createData successfully. cost time:", costTime)
}

func createIndex(dir string) {
	fmt.Println("call createIndex... ")
	start := time.Now()

	db := OpenDB(dir)
	db.CreateIndex()

	end := time.Now()
	costTime := readableTime(int(end.Sub(start)))
	fmt.Println("createIndex successfully. cost time:", costTime)
}

func findTest(dir string) {
	fmt.Println("call findTest... ")
	start := time.Now()

	db := OpenDB(dir)
	db.InitFind()

	concurrent := 10
	avgTimes := make([]int, concurrent)
	wg := sync.WaitGroup{}
	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func(curr int) {
			avgTimes[curr] = db.FindLoop(1e5)
			wg.Done()
		}(i)
	}
	wg.Wait()

	end := time.Now()
	costTime := readableTime(int(end.Sub(start)))

	// avg time
	for i := 1; i < concurrent; i++ {
		avgTimes[0] += avgTimes[i]
	}

	avgTime := readableTime(int(avgTimes[0] / concurrent))
	fmt.Println("findTest successfully. cost time:", costTime, ", find-operation avg time:", avgTime)
}
