package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type TopologyBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type ReadRespBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type NodeServer struct {
	Node *maelstrom.Node

	Messages   map[int]struct{}
	MessagesMu sync.RWMutex

	Topology   []string
	TopologyMu sync.RWMutex
}

func (s *NodeServer) handleBroadcast(msg maelstrom.Message) error {
	var body BroadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	go s.Node.Reply(msg, maelstrom.MessageBody{
		Type: "broadcast_ok",
	})

	message, messageExists := s.extractMessage(body)

	if !(messageExists) {
		s.saveMessage(message)
		s.broadcast(body, msg.Src)
	}

	return nil
}

func (s *NodeServer) saveMessage(message int) {
	s.MessagesMu.Lock()
	defer s.MessagesMu.Unlock()

	s.Messages[message] = struct{}{}
}

func (s *NodeServer) extractMessage(body BroadcastBody) (int, bool) {
	s.MessagesMu.RLock()
	defer s.MessagesMu.RUnlock()

	message := body.Message
	_, messageExists := s.Messages[message]

	return message, messageExists
}

func (s *NodeServer) broadcast(body BroadcastBody, src string) {
	s.TopologyMu.RLock()
	defer s.TopologyMu.RUnlock()

	topology := make([]string, len(s.Topology))
	copy(topology, s.Topology)

	for _, node := range topology {
		if node != src {
			go s.sendWithRetries(node, body)
		}
	}
}

func (s *NodeServer) sendWithRetries(dest string, body BroadcastBody) {
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(2*(i+1))*time.Second)
		defer cancel()

		_, err := s.Node.SyncRPC(context, dest, body)
		if err == nil {
			return
		}
	}
}

func (s *NodeServer) handleRead(msg maelstrom.Message) error {
	s.MessagesMu.RLock()
	defer s.MessagesMu.RUnlock()

	values := make([]int, 0)
	for value := range s.Messages {
		values = append(values, value)
	}

	return s.Node.Reply(msg, ReadRespBody{
		Type:     "read_ok",
		Messages: values,
	})
}

func (s *NodeServer) handleTopology(msg maelstrom.Message) error {
	var body TopologyBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.TopologyMu.Lock()
	s.Topology = body.Topology[s.Node.ID()]
	s.TopologyMu.Unlock()

	return s.Node.Reply(msg, maelstrom.MessageBody{
		Type: "topology_ok",
	})
}

func main() {
	n := maelstrom.NewNode()

	server := NodeServer{
		Node:     n,
		Messages: make(map[int]struct{}),
	}

	n.Handle("broadcast", server.handleBroadcast)
	n.Handle("read", server.handleRead)
	n.Handle("topology", server.handleTopology)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
