package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Необходимо реализовать систему обработки метрик на Go.

// В системе есть 10 воркеров, каждый из которых обрабатывает одну метрику за 30-70 секунд.

// Метрики поступают в систему каждые 5 секунд, то есть в среднем 12 метрик в минуту.
// Для передачи метрик используется небуферизованный канал.

// metrics 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
// workers 1 2 3 4 5 6 7 8 9 10       1  2  3

var (
	minT = 30
	maxT = 70
)

type MetricsI interface {
	Do()
}

type Metrics struct {
	id int
}

func (t *Metrics) Do() {
	fmt.Println("start ", t.id)
	time.Sleep(time.Duration(minT+rand.Intn(maxT-minT+1)) * time.Second)
	fmt.Println("finish", t.id)
}

type ManagerI interface {
	Do() chan MetricsI
	Stop()
}

type Manager struct {
	ch        chan MetricsI
	done      chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

func NewManager() ManagerI {
	return &Manager{
		ch:   make(chan MetricsI),
		done: make(chan struct{}),
	}
}

func (m *Manager) Do() chan MetricsI {
	m.startOnce.Do(m.start)
	return m.ch
}

func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.done)
	})
	m.wg.Wait()
}

func (m *Manager) start() {
	metricToWork := make(chan MetricsI)

	for i := 0; i < 10; i++ {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			for metric := range metricToWork {
				metric.Do()
			}
		}()
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer close(metricToWork)

		var pending MetricsI
		hasPending := false

		for {
			var out chan MetricsI
			if hasPending {
				out = metricToWork
			}

			select {
			case metric, ok := <-m.ch:
				if !ok {
					return
				}
				pending = metric
				hasPending = true
			case out <- pending:
				hasPending = false
			case <-m.done:
				return
			}
		}
	}()
}

func main() {
	var wg sync.WaitGroup
	m := NewManager()
	ch := m.Do()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			ch <- &Metrics{id: i}
			fmt.Println("sent", i)
   			time.Sleep(1 * time.Second)
		}
	}()

	wg.Wait()
	m.Stop()
}
