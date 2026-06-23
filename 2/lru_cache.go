package main

import (
	"container/list"
	"fmt"
	"sync"
)

type CacheEntry struct {
	Key   string
	Value interface{}
}

type CacheStats struct {
	Hits      int
	Misses    int
	Evictions int
}

type LRUCache struct {
	mu       sync.RWMutex
	capacity int
	elements map[string]*list.Element
	list     *list.List
	stats    *CacheStats
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		mu:       sync.RWMutex{},
		capacity: capacity,
		elements: make(map[string]*list.Element, capacity),
		list:     list.New(),
		stats:    &CacheStats{},
	}
}

func (c *LRUCache) Get(key string) (interface{}, bool) {
	// TODO: найти элемент и отметить как использованный
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.elements[key]
	if !ok {
		c.stats.Misses++
		return nil, false
	}

	c.list.MoveToFront(element)
	entry := element.Value.(*CacheEntry)
	c.stats.Hits++

	return entry.Value, true

}

func (c *LRUCache) Put(key string, value interface{}) {
	// TODO: добавить/обновить элемент
	// TODO: при переполнении удалить наименее использованный
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.elements[key]; ok {
		entry := element.Value.(*CacheEntry)
		entry.Value = value
		c.list.MoveToFront(element)

		return
	}

	if len(c.elements) >= c.capacity {
		old := c.list.Back()
		entry := old.Value.(*CacheEntry)
		delete(c.elements, entry.Key)
		c.list.Remove(old)
		c.stats.Evictions++
	}

	entry := &CacheEntry{
		Key:   key,
		Value: value,
	}
	element := c.list.PushFront(entry)
	c.elements[key] = element
}

func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.elements)
}

func (c *LRUCache) Keys() []string {
	// TODO: Возвращает все ключи (в любом порядке)
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, len(c.elements))
	for key := range c.elements {
		keys = append(keys, key)
	}

	return keys
}

func (c *LRUCache) Clear() {
	// TODO: Очищает весь кэш
	c.mu.Lock()
	defer c.mu.Unlock()

	c.list.Init()
	c.elements = make(map[string]*list.Element, c.capacity)
	c.stats.Hits = 0
	c.stats.Misses = 0
	c.stats.Evictions = 0
}

func (c *LRUCache) Delete(key string) bool {
	// TODO: вернуть true если элемент был удалён
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.elements[key]
	if !ok {
		return false
	}

	delete(c.elements, key)
	c.list.Remove(element)

	return true
}

func (c *LRUCache) Contains(key string) bool {
	// TODO: проверить без обновления статистики использования
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.elements[key]
	return ok
}

func (c *LRUCache) Clone() *LRUCache {
	// TODO: создать независимую копию с той же capacity
	c.mu.RLock()
	defer c.mu.RUnlock()

	newCache := &LRUCache{
		mu:       sync.RWMutex{},
		capacity: c.capacity,
		elements: make(map[string]*list.Element, c.capacity),
		list:     list.New(),
		stats: &CacheStats{
			Hits:      c.stats.Hits,
			Misses:    c.stats.Misses,
			Evictions: c.stats.Evictions,
		},
	}

	for element := c.list.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*CacheEntry)
		newEntry := &CacheEntry{
			Key:   entry.Key,
			Value: entry.Value,
		}
		newElement := newCache.list.PushBack(newEntry)
		newCache.elements[entry.Key] = newElement
	}

	return newCache
}

func (c *LRUCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return *c.stats
}

func (c *LRUCache) HitRate() float64 {
	// TODO: вычислите процент попаданий
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.stats.Hits + c.stats.Misses
	if total == 0 {
		return 0.0
	}

	return float64(c.stats.Hits) / float64(total) * 100
}

func main() {
	cache := NewLRUCache(3)

	cache.Put("a", "one")
	cache.Put("b", "two")
	cache.Put("c", "three")

	fmt.Println(cache.Get("a")) // "one", true - теперь "a" недавно использован

	cache.Put("d", "four") // Должно вытеснить "b" (наименее использованный)

	fmt.Println(cache.Get("b")) // nil, false
	fmt.Println(cache.Get("a")) // "one", true

	fmt.Println("\n=== Дополнительные проверки ===")

	// 1. Проверка Len()
	fmt.Printf("Len() = %d (ожидается 3: a, c, d)\n", cache.Len())

	// 2. Проверка Keys()
	fmt.Printf("Keys() = %v (в любом порядке)\n", cache.Keys())

	// 3. Проверка Contains()
	fmt.Printf("Contains('a') = %v (ожидается true)\n", cache.Contains("a"))
	fmt.Printf("Contains('b') = %v (ожидается false, был вытеснен)\n", cache.Contains("b"))
	fmt.Printf("Contains('d') = %v (ожидается true)\n", cache.Contains("d"))

	// 4. Проверка Delete()
	fmt.Printf("Delete('c') = %v (ожидается true)\n", cache.Delete("c"))
	fmt.Printf("После удаления c: Len() = %d, Keys() = %v\n", cache.Len(), cache.Keys())
	fmt.Printf("Delete('c') повторно = %v (ожидается false)\n", cache.Delete("c"))

	// 5. Проверка Stats()
	fmt.Printf("Stats() = %+v\n", cache.Stats())

	// 6. Проверка HitRate()
	fmt.Printf("HitRate() = %.2f%%\n", cache.HitRate())

	// 7. Проверка Clone()
	clone := cache.Clone()
	fmt.Printf("Clone: Len() = %d, Keys() = %v\n", clone.Len(), clone.Keys())

	// Проверяем, что клон независим
	cache.Put("e", "five")
	fmt.Printf("После добавления 'e' в оригинал:\n")
	fmt.Printf("  Оригинал: Len() = %d, Keys() = %v\n", cache.Len(), cache.Keys())
	fmt.Printf("  Клон:     Len() = %d, Keys() = %v (не должен измениться)\n", clone.Len(), clone.Keys())

	// 8. Проверка Clear()
	fmt.Printf("До Clear: Len() = %d, Keys() = %v\n", cache.Len(), cache.Keys())
	cache.Clear()
	fmt.Printf("После Clear: Len() = %d, Keys() = %v\n", cache.Len(), cache.Keys())
	fmt.Printf("Stats после Clear: %+v (должны быть нули)\n", cache.Stats())

	// 9. Проверка обновления значения через Put
	cache.Put("x", "initial")
	val, _ := cache.Get("x")
	fmt.Printf("Put('x', 'initial'): Get('x') = %v\n", val)
	cache.Put("x", "updated")
	val, _ = cache.Get("x")
	fmt.Printf("Put('x', 'updated'): Get('x') = %v\n", val)

	// 10. Проверка вытеснения (должен вытесниться самый старый - z)
	cache.Put("y", "one")
	cache.Put("z", "two")
	cache.Put("x", "three")
	cache.Get("y")
	cache.Get("x")
	cache.Put("w", "four")

	fmt.Printf("После добавления w: Keys() = %v\n", cache.Keys())

	valZ, okZ := cache.Get("z")
	fmt.Printf("Get('z') = %v, %v (ожидается nil, false - z вытеснен)\n", valZ, okZ)

	valY, okY := cache.Get("y")
	fmt.Printf("Get('y') = %v, %v (ожидается 'one', true)\n", valY, okY)

	valX, okX := cache.Get("x")
	fmt.Printf("Get('x') = %v, %v (ожидается 'three', true)\n", valX, okX)
}
