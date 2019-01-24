package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

func NewRing() *Ring {
	return &Ring{Nodes: Nodes{}}
}

func NewNode(id string) *Node {
	return &Node{
		ID:     id,
		HashID: crc32.ChecksumIEEE([]byte(id)),
	}
}

func (r *Ring) AddNode(id string) {
	r.Lock()
	defer r.Unlock()
	node := NewNode(id)
	r.Nodes = append(r.Nodes, node)
	sort.Sort(r.Nodes)
}

func (r *Ring) RemoveNode(id string) error {
	r.Lock()
	defer r.Unlock()

	searchFn := func(i int) bool {
		return r.Nodes[i].HashID >= crc32.ChecksumIEEE([]byte(id))
	}

	i := sort.Search(r.Nodes.Len(), searchFn)
	if i >= r.Nodes.Len() || r.Nodes[i].ID != id {
		return errors.New("node not found")
	}
	r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)
	return nil
}

func (r *Ring) Get(key string) string {
	searchFn := func(i int) bool {
		return r.Nodes[i].HashID >= crc32.ChecksumIEEE([]byte(key))
	}

	i := sort.Search(r.Nodes.Len(), searchFn)
	if i >= r.Nodes.Len() {
		i = 0
	}
	return r.Nodes[i].ID
}

type Ring struct {
	Nodes Nodes
	sync.Mutex
}

type Nodes []*Node

func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Less(i, j int) bool { return n[i].HashID < n[j].HashID }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

type Node struct {
	ID     string
	HashID uint32
}

func main() {
	ring := NewRing()
	ring.AddNode("1")
	ring.AddNode("2")

	node := ring.Get("2")
	ring.RemoveNode("2")
	node = ring.Get("2")
	fmt.Println(node)
}
