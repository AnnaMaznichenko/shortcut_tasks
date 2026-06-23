package main

import (
	"fmt"
	"sync"
	"time"
)

type User struct {
	ID   int
	Name string
}

type Connection interface {
	Type() string
	Weight() int
}

type Friend struct {
	Since string // Дата начала дружбы
}

func (f Friend) Type() string {
	return "friend"
}

func (f Friend) Weight() int {
	return 10
}

type Follower struct {
	Notifications bool
}

func (f Follower) Type() string {
	return "follower"
}

func (f Follower) Weight() int {
	return 5
}

type Blocked struct {
	Reason string
}

func (b Blocked) Type() string {
	return "blocked"
}

func (b Blocked) Weight() int {
	return -1
}

type Graph struct {
	mu          sync.RWMutex
	users       map[int]*User
	connections map[int]map[int]Connection
}

func NewGraph() *Graph {
	return &Graph{
		mu:          sync.RWMutex{},
		users:       make(map[int]*User),
		connections: make(map[int]map[int]Connection),
	}
}

func (g *Graph) AddUser(id int, name string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.users[id] = &User{
		ID:   id,
		Name: name,
	}
}

func (g *Graph) GetUser(id int) (*User, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if user, ok := g.users[id]; ok {
		return user, ok
	}

	return nil, false
}

func (g *Graph) AddConnection(fromID, toID int) bool {
	// TODO: вернуть false если один из пользователей не существует
	g.mu.Lock()
	defer g.mu.Unlock()

	_, ok := g.users[fromID]
	if !ok {
		return false
	}
	_, ok = g.users[toID]
	if !ok {
		return false
	}

	if g.connections[fromID] == nil {
		g.connections[fromID] = make(map[int]Connection)
	}

	g.connections[fromID][toID] = Follower{Notifications: true}

	return true
}

func (g *Graph) GetConnections(userID int) []*User {
	// TODO: вернуть слайс указателей на пользователей
	g.mu.RLock()
	defer g.mu.RUnlock()

	if _, ok := g.users[userID]; !ok {
		return nil
	}

	toIDs := g.connections[userID]
	users := make([]*User, 0, len(toIDs))
	for id := range toIDs {
		if user, ok := g.users[id]; ok {
			users = append(users, user)
		}
	}
	
	return users
}

func (g *Graph) HasConnection(fromID, toID int) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	_, ok := g.connections[fromID][toID]

	return ok
}

func (g *Graph) UserCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.users)
}

func (g *Graph) RemoveConnection(fromID, toID int) bool {
	// TODO: вернуть true если связь была удалена
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.connections[fromID]; !ok {
		return false
	}
	if _, ok := g.connections[fromID][toID]; !ok {
		return false
	}
	delete(g.connections[fromID], toID)

	return true
}

func (g *Graph) RemoveUser(id int) bool {
	// TODO: удалить пользователя и все его связи
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.users[id]; !ok {
		return false
	}

	delete(g.users, id)
	delete(g.connections, id)

	for fromID, connections := range g.connections {
		delete(connections, id)
		if len(connections) == 0 {
			delete(g.connections, fromID)
		}
	}

	return true
}

func (g *Graph) IsMutual(id1, id2 int) bool {
	// TODO: проверить связь в обе стороны
	g.mu.RLock()
	defer g.mu.RUnlock()

	_, ok1 := g.connections[id1][id2]
	_, ok2 := g.connections[id2][id1]

	return ok1 && ok2
}

func (g *Graph) ConnectionCount(userID int) int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if _, ok := g.users[userID]; !ok {
		return -1
	}

	return len(g.connections[userID])
}

func (g *Graph) CommonConnections(id1, id2 int) []*User {
	// TODO: найти пользователей, с которыми связаны оба
	g.mu.RLock()
	defer g.mu.RUnlock()

	connections1, ok1 := g.connections[id1]
	connections2, ok2 := g.connections[id2]
	if !ok1 || !ok2 {
		return nil
	}

	ids := make([]int, 0, min(len(connections1), len(connections2)))

	for connID1 := range connections1 {
		if _, ok := connections2[connID1]; ok {
			ids = append(ids, connID1)
		}
	}
	if len(ids) == 0 {
		return nil
	}

	users := make([]*User, 0, len(ids))
	for _, id := range ids {
		if user, ok := g.users[id]; ok {
			users = append(users, user)
		}
	}

	return users
}

func (g *Graph) SuggestConnections(userID int) []*User {
	// TODO: найти друзей друзей, исключая текущие связи и самого пользователя
	g.mu.RLock()
	defer g.mu.RUnlock()

	if _, ok := g.connections[userID]; !ok {
		return nil
	}

	suggestIDs := make(map[int]struct{})
	for friendID := range g.connections[userID] {
		for candidateID := range g.connections[friendID] {
			if candidateID == userID {
				continue
			}
			if _, isFriend := g.connections[userID][candidateID]; isFriend {
				continue
			}
			if _, exists := suggestIDs[candidateID]; exists {
				continue
			}

			suggestIDs[candidateID] = struct{}{}
		}
	}

	suggests := make([]*User, 0, len(suggestIDs))
	for suggestID := range suggestIDs {
		if user, ok := g.users[suggestID]; ok {
			suggests = append(suggests, user)
		}
	}

	return suggests
}

func (g *Graph) GetAllUsers() []*User {
	g.mu.RLock()
	defer g.mu.RUnlock()

	users := make([]*User, 0, len(g.users))
	for _, user := range g.users {
		users = append(users, user)
	}

	return users
}

func (g *Graph) AddTypedConnection(fromID, toID int, conn Connection) bool {
	// TODO: Добавляет связь с типом
	g.mu.Lock()
	defer g.mu.Unlock()

	_, ok := g.users[fromID]
	if !ok {
		return false
	}
	_, ok = g.users[toID]
	if !ok {
		return false
	}

	var newConnection Connection
	switch connection := conn.(type) {
	case Friend:
		newConnection = Friend{Since: time.Now().Format("02.01.2006")}
	case Follower:
		newConnection = Follower{Notifications: connection.Notifications}
	case Blocked:
		newConnection = Blocked{Reason: connection.Reason}
	default:
		return false
	}

	if g.connections[fromID] == nil {
		g.connections[fromID] = make(map[int]Connection)
	}

	g.connections[fromID][toID] = newConnection

	return true
}

func (g *Graph) GetConnectionsByType(userID int, connType string) []*User {
	// TODO: Возвращает связи определённого типа
	g.mu.RLock()
	defer g.mu.RUnlock()

	connections, ok := g.connections[userID]
	if !ok {
		return nil
	}

	users := make([]*User, 0, len(connections))
	for toID, connection := range connections {
		user, ok := g.users[toID]
		if !ok {
			continue
		}
		if connection.Type() == connType {
			users = append(users, user)
		}
	}

	return users
}

func (g *Graph) GetConnectionInfo(fromID, toID int) (Connection, bool) {
	// TODO: Возвращает информацию о связи
	g.mu.RLock()
	defer g.mu.RUnlock()

	if _, ok := g.connections[fromID]; !ok {
		return nil, false
	}

	conn, ok := g.connections[fromID][toID]
	if !ok {
		return nil, false
	}

	return conn, true
}

func main() {
	graph := NewGraph()

	graph.AddUser(1, "Alice")
	graph.AddUser(2, "Bob")
	graph.AddUser(3, "Charlie")

	graph.AddConnection(1, 2) // Alice -> Bob
	graph.AddConnection(1, 3) // Alice -> Charlie
	graph.AddConnection(2, 3) // Bob -> Charlie

	if user, ok := graph.GetUser(1); ok {
		fmt.Printf("User: %s\n", user.Name)
		friends := graph.GetConnections(1)
		fmt.Printf("Friends: %d\n", len(friends))
		for _, friend := range friends {
			fmt.Printf("  - %s\n", friend.Name)
		}
	}

	fmt.Printf("Alice and Bob connected: %v\n",
		graph.HasConnection(1, 2))

	// IsMutual (добавляем обратную связь)
	graph.AddConnection(2, 1)
	fmt.Printf("IsMutual(1,2): %v\n", graph.IsMutual(1, 2))
	fmt.Printf("ConnectionCount(1): %d\n", graph.ConnectionCount(1))

	// Общие друзья и рекомендации
	fmt.Print("CommonConnections(1,2):")
	for _, u := range graph.CommonConnections(1, 2) {
		fmt.Printf(" %s", u.Name)
	}
	fmt.Println()

	fmt.Print("SuggestConnections(1):")
	for _, u := range graph.SuggestConnections(1) {
		fmt.Printf(" %s", u.Name)
	}
	fmt.Println()

	// Типизированные связи
	graph.AddTypedConnection(2, 3, Friend{Since: "2024-01-01"})
	graph.AddTypedConnection(3, 1, Follower{Notifications: true})

	fmt.Print("GetConnectionsByType(2,'friend'):")
	for _, u := range graph.GetConnectionsByType(2, "friend") {
		fmt.Printf(" %s", u.Name)
	}
	fmt.Println()

	if conn, ok := graph.GetConnectionInfo(2, 3); ok {
		fmt.Printf("GetConnectionInfo(2,3): type=%s, weight=%d\n", conn.Type(), conn.Weight())
	}

	// Удаление
	fmt.Printf("RemoveConnection(1,3): %v\n", graph.RemoveConnection(1, 3))
	fmt.Printf("RemoveUser(3): %v\n", graph.RemoveUser(3))
	fmt.Printf("UserCount: %d\n", graph.UserCount())

	fmt.Print("All users:")
	for _, u := range graph.GetAllUsers() {
		fmt.Printf(" %s", u.Name)
	}
	fmt.Println()
}
