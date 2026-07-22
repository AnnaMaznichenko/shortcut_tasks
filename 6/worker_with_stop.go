package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func worker(wg *sync.WaitGroup, stopped *atomic.Bool, jobs chan int, stopCh chan struct{}, number int) {
	defer wg.Done()

	for {
		//дополнительный неблокирующий селект, так как в основном может быть случайный выбор
		select {
		case <-stopCh:
			stopped.Store(true)
			fmt.Println("Работа завершена по каналу stop в worker")
			return
		default:
		}

		select {
		case <-stopCh:
			stopped.Store(true)
			fmt.Println("Работа завершена по каналу stop в worker")
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			//на случай если другой воркер успел установить флаг
			if stopped.Load() {
				return
			}

			processJob(wg, number, job, stopCh)
		}
	}
}

func processJob(wg *sync.WaitGroup, number int, job int, stopCh <-chan struct{}) {
	done := make(chan struct{})
	cancel := make(chan struct{})
	defer close(cancel)

	wg.Add(1)
	go func() {
		defer wg.Done()

		timer := time.NewTimer(1500 * time.Millisecond) //таймер для имитации работы
		select {
		case <-timer.C:
			fmt.Println("Отработал воркер ", number, "Задание: ", job)
			close(done)
		case <-stopCh:
			timer.Stop()
			fmt.Println("Работа завершена по каналу stop в анонимной горутине")
			return
		case <-cancel:
			timer.Stop()
			return
		}
	}()

	select {
	case <-stopCh:
		fmt.Println("Работа завершена по каналу stop в processJob")
		return
	case <-time.After(2 * time.Second): // для демонстрации отмены по таймауту изменить значение на 1
		fmt.Println("Работа завершена по таймауту")
		return
	case <-done:
		return
	}
}

func main() {
	var wg sync.WaitGroup
	var stopped atomic.Bool
	jobs := make(chan int)
	stopCh := make(chan struct{})

	workerCount := 5
	wg.Add(workerCount)
	for i := 1; i <= workerCount; i++ {
		go worker(&wg, &stopped, jobs, stopCh, i)
	}

	jobsCount := 20
	for j := 1; j <= jobsCount; j++ {
		jobs <- j
	}
	close(jobs)

	time.Sleep(1200 * time.Millisecond)
	close(stopCh)

	wg.Wait()
	fmt.Println("Работа завершена")
}
