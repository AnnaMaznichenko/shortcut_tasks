package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const initCountEvents = 100

type Event struct {
	ID        int
	Type      string
	Data      string
	Timestamp time.Time
}

type EventStore struct {
	mu                  sync.RWMutex
	eventsByID          map[int]Event
	eventsIDByType      map[string][]int
	eventsIDByTimestamp map[time.Time][]int
	uniqueTimes         []time.Time
	nextID              int
}

func NewEventStore() *EventStore {
	return &EventStore{
		eventsByID:          make(map[int]Event, initCountEvents),
		eventsIDByType:      make(map[string][]int, initCountEvents),
		eventsIDByTimestamp: make(map[time.Time][]int, initCountEvents),
		uniqueTimes:         make([]time.Time, 0, initCountEvents),
		nextID:              0,
	}
}

func (es *EventStore) Add(eventType string, data string) int {
	// TODO: создать событие, добавить в хранилище, вернуть ID
	es.mu.Lock()
	defer es.mu.Unlock()

	event := es.prepareEvent(eventType, data)

	es.eventsByID[event.ID] = event
	es.eventsIDByType[eventType] = append(es.eventsIDByType[eventType], event.ID)
	if _, ok := es.eventsIDByTimestamp[event.Timestamp]; !ok {
		idx := sort.Search(len(es.uniqueTimes), func(i int) bool {
			return es.uniqueTimes[i].After(event.Timestamp)
		})
		es.uniqueTimes = append(es.uniqueTimes, time.Time{})
		copy(es.uniqueTimes[idx+1:], es.uniqueTimes[idx:])
		es.uniqueTimes[idx] = event.Timestamp
	}
	es.eventsIDByTimestamp[event.Timestamp] = append(es.eventsIDByTimestamp[event.Timestamp], event.ID)

	return event.ID
}

func (es *EventStore) prepareEvent(eventType string, data string) Event {
	es.nextID++
	timestamp := time.Now().Truncate(time.Second)

	return Event{
		ID:        es.nextID,
		Type:      eventType,
		Data:      data,
		Timestamp: timestamp,
	}
}

func (es *EventStore) GetAll() []Event {
	// TODO: вернуть копию всех событий
	es.mu.RLock()
	defer es.mu.RUnlock()

	events := make([]Event, 0, len(es.eventsByID))

	for _, event := range es.eventsByID {
		events = append(events, event)
	}

	return events
}

func (es *EventStore) GetByID(id int) (Event, bool) {
	// TODO: найти событие по ID
	es.mu.RLock()
	defer es.mu.RUnlock()

	if event, ok := es.eventsByID[id]; ok {
		return event, ok
	}

	return Event{}, false
}

func (es *EventStore) Count() int {
	// TODO: получить количество событий
	es.mu.RLock()
	defer es.mu.RUnlock()

	return len(es.eventsByID)
}

func (es *EventStore) GetByType(eventType string) []Event {
	// TODO: Возвращает все события указанного типа
	es.mu.RLock()
	defer es.mu.RUnlock()

	ids, ok := es.eventsIDByType[eventType]
	if !ok {
		return []Event{}
	}

	events := make([]Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, es.eventsByID[id])
	}

	return events
}

func (es *EventStore) FindAfter(timestamp time.Time) []Event {
	// TODO: Возвращает события после указанного времени
	es.mu.RLock()
	defer es.mu.RUnlock()

	indexTime := es.searchTime(timestamp)
	if indexTime < len(es.uniqueTimes) && es.uniqueTimes[indexTime].Equal(timestamp) {
		indexTime++
	}
	times := es.uniqueTimes[indexTime:]

	var ids []int
	for _, t := range times {
		ids = append(ids, es.eventsIDByTimestamp[t]...)
	}

	events := make([]Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, es.eventsByID[id])
	}

	return events
}

func (es *EventStore) searchTime(timestamp time.Time) int {
	left, right := 0, len(es.uniqueTimes)-1

	for left <= right {
		middle := (left + right) / 2

		if es.uniqueTimes[middle].Equal(timestamp) {
			return middle
		}
		if es.uniqueTimes[middle].Before(timestamp) {
			left = middle + 1
		} else {
			right = middle - 1
		}
	}

	return left
}

func (es *EventStore) GetRange(startID, endID int) []Event {
	// TODO: Возвращает события в диапазоне ID, включая startID и endID
	es.mu.RLock()
	defer es.mu.RUnlock()

	if startID < 1 {
		startID = 1
	}
	if endID > es.nextID {
		endID = es.nextID
	}
	if endID < startID {
		return []Event{}
	}

	events := make([]Event, 0, endID-startID+1)
	for i := startID; i <= endID; i++ {
		if event, ok := es.eventsByID[i]; ok {
			events = append(events, event)
		}
	}

	return events
}

func (es *EventStore) Filter(predicate func(Event) bool) []Event {
	// TODO: вернуть события, для которых predicate вернул true
	es.mu.RLock()
	events := make([]Event, 0, len(es.eventsByID))
	for _, event := range es.eventsByID {
		events = append(events, event)
	}
	es.mu.RUnlock()

	n := 0
	for _, event := range events {
		if predicate(event) {
			events[n] = event
			n++
		}
	}

	if n < len(events) / 2 {
		result := make([]Event, n)
		copy(result, events[:n])
		
		return result
	}

	return events[:n]
}

func main() {
	store := NewEventStore()

	// Добавляем тестовые события
	id1 := store.Add("user.login", "user: alice")
	id2 := store.Add("user.logout", "user: alice")
	id3 := store.Add("user.login", "user: bob")
	id4 := store.Add("system.start", "service: api")
	id5 := store.Add("user.login", "user: charlie")

	fmt.Println("=== Исходные события ===")
	fmt.Printf("Добавлены ID: %d, %d, %d, %d, %d\n\n", id1, id2, id3, id4, id5)

	// === 1. Проверка GetByID ===
	fmt.Println("=== 1. GetByID ===")
	if event, ok := store.GetByID(id1); ok {
		fmt.Printf("Event %d: %s - %s at %v\n",
			event.ID, event.Type, event.Data, event.Timestamp)
	}
	if _, ok := store.GetByID(999); !ok {
		fmt.Println("Event 999: not found (OK)")
	}
	fmt.Println()

	// === 2. Проверка GetByType ===
	fmt.Println("=== 2. GetByType ===")
	events := store.GetByType("user.login")
	fmt.Printf("Found %d events of type 'user.login':\n", len(events))
	for _, event := range events {
		fmt.Printf("  ID: %d, Data: %s\n", event.ID, event.Data)
	}

	events = store.GetByType("unknown")
	fmt.Printf("Type 'unknown': %d events (should be 0)\n\n", len(events))

	// === 3. Проверка GetAll ===
	fmt.Println("=== 3. GetAll ===")
	allEvents := store.GetAll()
	fmt.Printf("Total events: %d\n", len(allEvents))
	for _, e := range allEvents {
		fmt.Printf("  ID: %d, Type: %s, Data: %s, Time: %v\n",
			e.ID, e.Type, e.Data, e.Timestamp.Format("15:04:05"))
	}
	fmt.Println()

	// === 4. Проверка GetRange ===
	fmt.Println("=== 4. GetRange ===")
	rangeEvents := store.GetRange(2, 4)
	fmt.Printf("Events in range 2-4: %d\n", len(rangeEvents))
	for _, e := range rangeEvents {
		fmt.Printf("  ID: %d, Type: %s, Data: %s\n", e.ID, e.Type, e.Data)
	}

	// Проверка с выходом за границы
	rangeEvents = store.GetRange(1, 10)
	fmt.Printf("Range 1-10 (with correction): %d events\n", len(rangeEvents))
	fmt.Println()

	// === 5. Проверка Filter ===
	fmt.Println("=== 5. Filter ===")
	filtered := store.Filter(func(e Event) bool {
		return e.Type == "user.login" && len(e.Data) > 10
	})
	fmt.Printf("Filtered (user.login and data length > 10): %d events\n", len(filtered))
	for _, e := range filtered {
		fmt.Printf("  ID: %d, Type: %s, Data: %s\n", e.ID, e.Type, e.Data)
	}

	// === 6. Проверка FindAfter ===
	fmt.Println("\n=== 6. FindAfter ===")
	// Берём время второго события
	if event2, ok := store.GetByID(id2); ok {
		afterTime := event2.Timestamp.Add(-1 * time.Second) // за секунду до второго события
		afterEvents := store.FindAfter(afterTime)
		fmt.Printf("Events after %v: %d events\n",
			afterTime.Format("15:04:05"), len(afterEvents))
		for _, e := range afterEvents {
			fmt.Printf("  ID: %d, Type: %s, Time: %v\n",
				e.ID, e.Type, e.Timestamp.Format("15:04:05"))
		}
	}

	// Проверка FindAfter с временем, которого нет
	futureTime := time.Now().Add(24 * time.Hour)
	afterEvents := store.FindAfter(futureTime)
	fmt.Printf("Events after future time: %d events (should be 0)\n", len(afterEvents))
	fmt.Println()

	// === 7. Проверка Count ===
	fmt.Println("=== 7. Count ===")
	fmt.Printf("Total events: %d (should be 5)\n", store.Count())
}
