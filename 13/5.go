package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

/*
Есть приложение с микросервисной архитектурой.
Микросервис можно абстрагировать с помощью интерфейса Backend.
Для доступа к одному экземпляру микросервиса можно использовать
тип BackendImpl, который уже реализован.


Для каждого микросервиса есть несколько десятков запущенных
экземпляров, каждый из которых доступен по своему адресу addr.
Однако отдельные экземпляры микросервиса ненадежны:
они могут падать, быть недоступными либо перегруженными.
Поэтому вам нужно реализовать тип Balancer, который также реализует
интерфейс Backend и осуществляет у балансировку нагрузки
между экземплярами микросервиса, выбирая каждый раз **наименее нагруженный** экземпляр.
*/

type Request interface{}

type Response interface{}

type Backend interface {
	Invoke(ctx context.Context, req Request) (Response, error)
}

var _ Backend = &BackendImpl{}

type BackendFull struct {
	Connections    int
	AvgExecTime    time.Duration
	TotalTime      time.Duration
	CompletedCount int
	FailedCount    int
	Backend
}

type BackendImpl struct {
	addr          string
	failedPercent int
	baseDelay     time.Duration
}

func (b *BackendImpl) Invoke(ctx context.Context, req Request) (Response, error) {
	delay := b.baseDelay + time.Duration(rand.IntN(50))*time.Millisecond

	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	if rand.IntN(100) < b.failedPercent {
		return "", errors.New("service failed")
	}

	return b.addr, nil
}

func (b *BackendImpl) String() string { return b.addr }

func NewBackend(addr string, indx int) *BackendImpl {
	return &BackendImpl{
		addr:          addr,
		failedPercent: indx * 2,
		baseDelay:     time.Duration(10+indx*20) * time.Millisecond,
	}
}

type Balancer interface {
	Backend
}

type BalancerImpl struct {
	backends []BackendFull
	mu       sync.Mutex
}

var _ Balancer = &BalancerImpl{}

func NewBalancer(addrs []string) *BalancerImpl {
	if len(addrs) == 0 {
		return nil
	}

	backends := make([]BackendFull, 0, len(addrs))
	for i, addr := range addrs {
		backend := NewBackend(addr, i)
		backends = append(backends, BackendFull{Backend: backend})
	}

	return &BalancerImpl{
		backends: backends,
	}
}

func (b *BalancerImpl) Invoke(ctx context.Context, req Request) (Response, error) {
	selectedBackend, err := b.balance()
	if err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := selectedBackend.Invoke(ctx, req)
	duration := time.Since(start)

	b.mu.Lock()
	defer b.mu.Unlock()

	if err != nil {
		selectedBackend.FailedCount++
	} else {
		selectedBackend.CompletedCount++
		selectedBackend.TotalTime += duration
		selectedBackend.AvgExecTime = selectedBackend.TotalTime / time.Duration(selectedBackend.CompletedCount)
	}
	selectedBackend.Connections--

	return resp, err
}

func (b *BalancerImpl) balance() (*BackendFull, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.backends) == 0 {
		return nil, errors.New("backends list is empty")
	}

	selected := &b.backends[0]
	equal := []*BackendFull{selected}
	for i := 1; i < len(b.backends); i++ {
		cand := &b.backends[i]
		switch {
		// cand better selected
		case prefer(selected, cand):
			selected = cand
			equal = equal[:0]
			equal = append(equal, cand)
			// equal
		case !prefer(cand, selected):
			equal = append(equal, cand)
		}
	}
	selected = equal[rand.N(len(equal))]
	selected.Connections++

	return selected, nil
}

// prefer(a, b) == true if b better a
func prefer(a, b *BackendFull) bool {
	switch {
	case a.Connections < b.Connections:
		return false
	case b.Connections < a.Connections:
		return true
	default:
	}

	switch {
	case failRate(a) < failRate(b):
		return false
	case failRate(b) < failRate(a):
		return true
	default:
	}

	if a.AvgExecTime != b.AvgExecTime {
		if a.AvgExecTime == 0 {
			return true
		}
		if b.AvgExecTime == 0 {
			return false
		}

		return b.AvgExecTime < a.AvgExecTime
	}

	return false
}

func failRate(a *BackendFull) float64 {
	total := a.FailedCount + a.CompletedCount
	if total == 0 {
		return 0
	}

	return float64(a.FailedCount) / float64(total)
}

func main() {
	addrs := make([]string, 0, 10)
	for i := range 10 {
		addrs = append(addrs, fmt.Sprintf("backend-%d", i))
	}
	balancer := NewBalancer(addrs)
	if balancer == nil {
		log.Fatal("no backends")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var okCount, errCount atomic.Int64
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 10 {
				_, err := balancer.Invoke(ctx, nil)
				if err != nil {
					errCount.Add(1)
				} else {
					okCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	fmt.Printf("successes: %d, errors: %d\n", okCount.Load(), errCount.Load())
	for _, b := range balancer.backends {
		fmt.Printf("%v: connections=%d failed=%d completed=%d avg=%v\n",
			b.Backend, b.Connections, b.FailedCount, b.CompletedCount, b.AvgExecTime,
		)
	}
}
