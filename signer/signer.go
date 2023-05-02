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
	wg := &sync.WaitGroup{}
	mu := sync.Mutex{}
	for val := range in {
		data := fmt.Sprintf("%v", val)

		mu.Lock()
		m5Data := DataSignerMd5(data)
		mu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			crc32Chan := make(chan string)
			crc32M5DataChan := make(chan string)

			go func() {
				crc32Chan <- DataSignerCrc32(data)
			}()

			go func() {
				crc32M5DataChan <- DataSignerCrc32(m5Data)
			}()

			crc32Data := <-crc32Chan
			crc32Md5Data := <-crc32M5DataChan
			out <- crc32Data + "~" + crc32Md5Data
		}()
	}
	wg.Wait()
}

func MultiHash(in chan interface{}, out chan interface{}) {
	wg := sync.WaitGroup{}

	for val := range in {
		wg.Add(1)

		val := val
		go func() {
			defer wg.Done()
			newWg := &sync.WaitGroup{}
			mutexes := make([]sync.Mutex, 6)
			hashes := make([]string, 6)
			mu := &sync.Mutex{}
			for i := 0; i < 6; i++ {
				newWg.Add(1)
				data := strconv.Itoa(i) + val.(string)

				go func(slc []string, ind int, data string, newWg *sync.WaitGroup, mu *sync.Mutex) {
					defer newWg.Done()
					data = DataSignerCrc32(data)
					mutexes[ind].Lock()
					hashes[ind] = data
					mutexes[ind].Unlock()
				}(hashes, i, data, newWg, mu)

			}

			newWg.Wait()
			out <- strings.Join(hashes, "")
		}()
	}

	wg.Wait()
}

func CombineResults(in chan interface{}, out chan interface{}) {
	var results []string
	for val := range in {
		data := fmt.Sprintf("%v", val)
		results = append(results, data)
	}

	sort.Strings(results)

	out <- strings.Join(results, "_")
}
