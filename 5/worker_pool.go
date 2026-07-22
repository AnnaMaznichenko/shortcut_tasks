package main

import (
	"fmt"
	"sync"
	"time"
)

func worker(wg *sync.WaitGroup, jobs <-chan int, results chan<- int, sem chan struct{}) {
	defer wg.Done()

	for job := range jobs {
		sem <- struct{}{}
		fmt.Printf("Обработка задания %d\n", job)
		time.Sleep(time.Second)
		result := job * job
		<-sem
		results <- result
	}
}

func printer(wg *sync.WaitGroup, results <-chan int) {
	defer wg.Done()

	for result := range results {
		fmt.Printf("Результат: %d\n", result)
	}
}

func main() {
	var wg sync.WaitGroup
	jobs := make(chan int, 5)
	results := make(chan int, 5)  //если убрать буфер, воркеры будут блокироваться на отправке, пока printer не прочитает
	sem := make(chan struct{}, 2) //канал для ограничения параллельности

	workersCount := 5
	wg.Add(workersCount)
	for i := 0; i < workersCount; i++ {
		go worker(&wg, jobs, results, sem)
	}

	var wgPrinter sync.WaitGroup
	wgPrinter.Add(1)
	go printer(&wgPrinter, results)

	jobsCount := 20
	for j := 1; j <= jobsCount; j++ {
		fmt.Println("Отправка задания ", j)
		jobs <- j
	}
	close(jobs)

	wg.Wait()
	close(results)

	wgPrinter.Wait()
	fmt.Println("Работа завершена")
}
