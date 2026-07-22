package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var counter int64

func raceCounter() {
	var wg sync.WaitGroup

	counter = 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				counter++
			}
		}()
	}

	wg.Wait()
	fmt.Println("raceCounter: ", counter)
}

func mutexCounter() {
	var wg sync.WaitGroup
	var mu sync.Mutex

	counter = 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				mu.Lock()
				counter++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	fmt.Println("mutexCounter: ", counter)
}

func atomicCounter() {
	var wg sync.WaitGroup

	counter = 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				atomic.AddInt64(&counter, 1)
			}
		}()
	}

	wg.Wait()
	fmt.Println("atomicCounter: ", counter)
}

func main() {
	start := time.Now()
	raceCounter()
	fmt.Println("time:", time.Since(start))

	start = time.Now()
	mutexCounter()
	fmt.Println("time:", time.Since(start))

	start = time.Now()
	atomicCounter()
	fmt.Println("time:", time.Since(start))
}