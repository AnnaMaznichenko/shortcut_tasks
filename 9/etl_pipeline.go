// etl_pipeline.go
//
//
//   genWeb   ─ normalize ─┐
//                         ├─ merge (fan-in) ─ Event ── batch(size + timeout) ─ consume
//   genApp   ─ normalize ─┘

package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// --- Структуры источников: разная форма, близкий смысл ---

type WebEvent struct {
	SessionID string
	URL       string
	TS        int64 // unix seconds
}

type AppEvent struct {
	DeviceID  string
	Screen    string
	EventTime time.Time
}

// --- Общий нормализованный вид ---

type Event struct {
	Source string // web | app
	UserID string // sessions | device
	Action string // url | screen
	At     time.Time
}

// --- Генераторы ---

func genWeb(ctx context.Context, every time.Duration) <-chan WebEvent {
	out := make(chan WebEvent)
	go func() {
		defer close(out)
		t := time.NewTicker(every)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				i++
				ev := WebEvent{
					SessionID: "web-" + strconv.Itoa(i),
					URL:       "/p/" + strconv.Itoa(rand.Intn(5)),
					TS:        time.Now().Unix(),
				}
				// Отправку тоже прикрываем ctx, иначе на отмене
				// горутина зависнет на out <- ev, если читателя уже нет.
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func genApp(ctx context.Context, every time.Duration) <-chan AppEvent {
	out := make(chan AppEvent)
	go func() {
		defer close(out)
		t := time.NewTicker(every)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				i++
				ev := AppEvent{
					DeviceID:  "dev-" + strconv.Itoa(i),
					Screen:    "screen_" + strconv.Itoa(rand.Intn(5)),
					EventTime: time.Now(),
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func consume(in <-chan []Event) {
	n := 0
	for b := range in {
		n++
		web, app := 0, 0
		for _, e := range b {
			switch e.Source {
			case "web":
				web++
			case "app":
				app++
			}
		}
		log.Printf("batch #%d: %d событий (web=%d app=%d)", n, len(b), web, app)
	}
	log.Printf("готово, всего батчей: %d", n)
}

func normalizeWeb(ctx context.Context, webEvent <-chan WebEvent) <-chan Event {
	eventCh := make(chan Event)

	go func() {
		defer close(eventCh)

		var event Event

		for {
			select {
			case <-ctx.Done():
				return 
			case webEventBuff, ok := <- webEvent:
				if !ok {
					return
				}
				event = Event{
					Source: "web",
					UserID: webEventBuff.SessionID,
					Action: webEventBuff.URL,
					At: time.Unix(webEventBuff.TS, 0),
				}
			}

			select {
			case <-ctx.Done():
				return
			case eventCh <- event:
			}
		}
		
	}()

	return eventCh
}

func normalizeApp(ctx context.Context, appEvent <-chan AppEvent) <-chan Event {
	eventCh := make(chan Event)

	go func() {
		defer close(eventCh)
		
		var event Event

		for {
			select {
			case <-ctx.Done():
				return 
			case appEventBuff, ok := <- appEvent:
				if !ok {
					return
				}
				event = Event{
					Source: "app",
					UserID: appEventBuff.DeviceID,
					Action: appEventBuff.Screen,
					At: appEventBuff.EventTime,
				}
			}

			select {
			case <-ctx.Done():
				return
			case eventCh <- event:
			}
		}
		
	}()

	return eventCh
}

func merge(ctx context.Context, eventsCh []<-chan Event) <-chan Event {
	var wg sync.WaitGroup
	result := make(chan Event)

	for _, event := range eventsCh {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return 
				case resultBuff, ok := <-event:
					if !ok {
						return 
					}
					result <- resultBuff
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(result)
	}()

	return result
}

func batch(ctx context.Context, events <-chan Event, size int, timeout time.Duration) <-chan []Event {
	batch := make([]Event, 0, size)
	out := make(chan []Event)

	go func() {
		defer close(out)

		timer := time.NewTimer(timeout)
		sendAndClearBatch := func() {
			if len(batch) == 0 {
				resetTimer(timer, timeout)
				return
			}

			sendBatch(ctx, out, batch, timer, timeout)
			batch = batch[:0]
		}

		for event := range events {
			select {
			case <-ctx.Done():
				return 
			case <-timer.C:
				sendAndClearBatch()
			default:
				if len(batch) == size {
					sendAndClearBatch()
				}

				batch = append(batch, event)
			}
		}

		if len(batch) > 0 {
			sendAndClearBatch()
		}
	}()

	return out
}

func sendBatch(ctx context.Context, out chan []Event, batch []Event, timer *time.Timer, timeout time.Duration) {
	batchCopy := make([]Event, len(batch))
	copy(batchCopy, batch)

	select {
	case <-ctx.Done():
		return
	case out <- batchCopy:
	}

	resetTimer(timer, timeout)
}

func resetTimer(t *time.Timer, d time.Duration) {
    if !t.Stop() {
        <-t.C
    }
    t.Reset(d)
}

func RunPipeline(ctx context.Context, web <-chan WebEvent, app <-chan AppEvent, batchSize int, timeout time.Duration) <-chan []Event {
    merged := merge(ctx, []<-chan Event{
		normalizeWeb(ctx, web),
		normalizeApp(ctx, app),
	})

    return batch(ctx, merged, batchSize, timeout)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
        time.Sleep(30 * time.Second)
        cancel()
    }()

	every := time.Duration(1*time.Second)
	webEvent := genWeb(ctx, every)
	appEvent := genApp(ctx, every)

	batchSize := 10
	timeout := time.Duration(20*time.Second)
	in := RunPipeline(ctx, webEvent, appEvent, batchSize, timeout)
	consume(in)
}

