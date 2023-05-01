package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {

	in := make(chan interface{})
	out := make(chan interface{})
	wg := sync.WaitGroup{}

	for _, j := range jobs {
		wg.Add(1)
		go func(job job, in, out chan interface{}) {
			defer wg.Done()
			job(in, out)
			close(out)
		}(j, in, out)
		in = out
		out = make(chan interface{})
	}

	wg.Wait()

}

func SingleHash(in chan interface{}, out chan interface{}) {
	mu := &sync.Mutex{}

	for val := range in {
		data := fmt.Sprintf("%s", val)

		mu.Lock()
		Md5 := DataSignerMd5(data)
		mu.Unlock()

		Crc32Md5 := make(chan string)
		Crc32Data := make(chan string)

		go func(crc32Chan1 chan<- string, data string) {
			crc32 := DataSignerCrc32(data)
			crc32Chan1 <- crc32
		}(Crc32Data, data)

		go func(crc32Chan2 chan<- string, md5Hash string) {
			crc32 := DataSignerCrc32(md5Hash)
			crc32Chan2 <- crc32
		}(Crc32Md5, Md5)

		out <- fmt.Sprintf("%v~%v", <-Crc32Data, <-Crc32Md5)
	}
}

func MultiHash(in chan interface{}, out chan interface{}) {
	for val := range in {
		data := fmt.Sprintf("%v", val)
		var wg sync.WaitGroup
		hashes := make([]string, 6)
		mutexes := make([]sync.Mutex, 6)

		for i := 0; i < 6; i++ {
			wg.Add(1)

			go func(i int, data string) {
				defer wg.Done()
				mutexes[i].Lock()
				hash := DataSignerCrc32(strconv.Itoa(i) + data)
				hashes[i] = hash
				mutexes[i].Unlock()
			}(i, data)
		}

		wg.Wait()
		out <- strings.Join(hashes, "")
	}
}

func CombineResults(in chan interface{}, out chan interface{}) {
	var results []string
	mu := sync.Mutex{}
	for val := range in {
		mu.Lock()
		data := fmt.Sprintf("%v", val)
		results = append(results, data)
		mu.Unlock()
	}

	sort.Strings(results)

	out <- strings.Join(results, "_")
}
