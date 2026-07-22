package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	ServiceProfile string = "profile"
	ServiceOrders  string = "orders"
	ServiceRecs    string = "recs"
)

type ProfileData struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type OrdersData struct {
	Orders []int `json:"orders"`
}

type RecsData struct {
	Recs []string `json:"recs"`
}

type UserData struct {
	Profile *ProfileData `json:"profile,omitempty"`
	Orders  *OrdersData  `json:"orders,omitempty"`
	Recs    *RecsData    `json:"recs,omitempty"`
}

type Response struct {
	Service string
	Data    interface{}
	Err     error
}

func fetchProfile(ctx context.Context) (string, error) {
	delay := time.Duration(rand.Intn(300)+500) * time.Millisecond
	select {
	case <-time.After(delay):
		break
	case <-ctx.Done():
		return "", ctx.Err()
	}

	if rand.Float32() < 0.15 {
		return "", fmt.Errorf("profile service unavailable")
	}
	if rand.Float32() < 0.05 {
		panic("profile panic")
	}

	return `{"id":1, "name":"Ann"}`, nil
}

func fetchOrders(ctx context.Context) (string, error) {
	delay := time.Duration(rand.Intn(200)+400) * time.Millisecond
	select {
	case <-time.After(delay):
		break
	case <-ctx.Done():
		return "", ctx.Err()
	}

	if rand.Float32() < 0.15 {
		return "", fmt.Errorf("orders service unavailable")
	}
	if rand.Float32() < 0.05 {
		panic("orders panic")
	}

	return `{"orders":[1,2,3]}`, nil
}

func fetchRecs(ctx context.Context) (string, error) {
	delay := time.Duration(rand.Intn(100)+300) * time.Millisecond
	select {
	case <-time.After(delay):
		break
	case <-ctx.Done():
		return "", ctx.Err()
	}

	if rand.Float32() < 0.15 {
		return "", fmt.Errorf("recommendations service unavailable")
	}
	if rand.Float32() < 0.05 {
		panic("recommendations panic")
	}

	return `{"recs":["item_1","item_2"]}`, nil
}

func sendResult(ctx context.Context, results chan<- Response, resp Response) {
	select {
	case results <- resp:
	case <-ctx.Done():
	}
}

// FanIn собирает ответы из канала results
// Возвращает частично заполненную UserData и ошибку, если хотя бы один сервис завершился с ошибкой
func FanIn(results chan Response) (UserData, error) {
	var data UserData
	var errs []error

	for result := range results {
		if result.Err != nil {
			errs = append(errs, result.Err)
			continue
		}

		switch result.Service {
		case ServiceProfile:
			data.Profile = result.Data.(*ProfileData)
		case ServiceOrders:
			data.Orders = result.Data.(*OrdersData)
		case ServiceRecs:
			data.Recs = result.Data.(*RecsData)
		}
	}

	if len(errs) > 0 {
		return data, errors.Join(errs...)
	}

	return data, nil
}

func main() {
	baseCtx, shutdown := context.WithCancel(context.Background())
	defer shutdown()

	time.AfterFunc(500*time.Millisecond, shutdown)

	ctx, cancel := context.WithTimeout(baseCtx, 1*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	results := make(chan Response)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				sendResult(ctx, results, Response{Err: fmt.Errorf("panic when receiving a profile: %v", r)})
			}
		}()

		data, err := fetchProfile(ctx)
		if err != nil {
			sendResult(ctx, results, Response{Err: err})
			return
		}

		var profile ProfileData
		err = json.Unmarshal([]byte(data), &profile)
		if err != nil {
			sendResult(ctx, results, Response{Err: err})
			return
		}

		sendResult(ctx, results, Response{Service: ServiceProfile, Data: &profile})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				sendResult(ctx, results, Response{Err: fmt.Errorf("panic when receiving an orders: %v", r)})
			}
		}()

		data, err := fetchOrders(ctx)
		if err != nil {
			sendResult(ctx, results, Response{Err: err})
			return
		}

		var orders OrdersData
		err = json.Unmarshal([]byte(data), &orders)
		if err != nil {
			sendResult(ctx, results, Response{Err: err})
			return
		}

		sendResult(ctx, results, Response{Service: ServiceOrders, Data: &orders})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				sendResult(ctx, results, Response{Err: fmt.Errorf("panic when receiving a recs: %v", r)})
			}
		}()

		data, err := fetchRecs(ctx)
		if err != nil {
			sendResult(ctx, results, Response{Err: err})
			return
		}

		var recs RecsData
		err = json.Unmarshal([]byte(data), &recs)
		if err != nil {
			sendResult(ctx, results, Response{Err: err})
			return
		}

		sendResult(ctx, results, Response{Service: ServiceRecs, Data: &recs})
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	data, err := FanIn(results)
	if err != nil {
		fmt.Printf("Aggregation errors: %v\n", err)
	}

	jsonData, _ := json.MarshalIndent(data, "", "  ")
	fmt.Println(string(jsonData))
}
