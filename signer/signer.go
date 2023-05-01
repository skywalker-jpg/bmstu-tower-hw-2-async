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

		Crc32Md5 := make(chan interface{})

		go func() {
			Crc32Md5 <- DataSignerCrc32(Md5)
		}()

		Crc32Data := make(chan interface{})

		go func() {
			Crc32Data <- DataSignerCrc32(data)
		}()

		out <- fmt.Sprintf("%s~%s", <-Crc32Data, <-Crc32Md5)
	}
}

func MultiHash(in chan interface{}, out chan interface{}) {
	wg := sync.WaitGroup{}
	var hashes [6]string

	for val := range in {
		data := fmt.Sprintf("%s", val)

		for i := 0; i < 5; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()
				hashes[i] = DataSignerCrc32(strconv.Itoa(i) + data)
			}()

		}
	}

	wg.Wait()
	out <- strings.Join(hashes[:], "")

}

func CombineResults(in chan interface{}, out chan interface{}) {
	var results []int
	combined := make([]string, len(results))

	for val := range in {
		data := fmt.Sprintf("%v", val)
		num, _ := strconv.Atoi(data)
		results = append(results, num)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i] < results[j]
	})

	for val := range in {
		combined = append(combined, fmt.Sprintf("%v", val))
	}

	out <- strings.Join(combined, "_")
}
