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

type BatchBroadcastBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
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

	UnbroadcastedMessages   []int
	UnbroadcastedMessagesMu sync.RWMutex
}

func (s *NodeServer) handleBroadcast(msg maelstrom.Message) error {
	var body BroadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	go s.Node.Reply(msg, maelstrom.MessageBody{
		Type: "broadcast_ok",
	})

	s.handleMessage(body.Message)

	return nil
}

func (s *NodeServer) checkMessageExists(message int) bool {
	s.MessagesMu.Lock()
	defer s.MessagesMu.Unlock()

	_, messageExists := s.Messages[message]
	return messageExists
}

func (s *NodeServer) handleMessage(message int) {
	messageExists := s.checkMessageExists(message)

	if !(messageExists) {
		s.saveMessage(message)
	}
}

func (s *NodeServer) handleBatchBroadcast(msg maelstrom.Message) error {
	var body BatchBroadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	go s.Node.Reply(msg, maelstrom.MessageBody{
		Type: "batch_broadcast_ok",
	})

	messages := body.Messages
	for _, msg := range messages {
		s.handleMessage(msg)
	}

	return nil
}

func (s *NodeServer) saveMessage(message int) {
	s.MessagesMu.Lock()
	s.Messages[message] = struct{}{}
	s.MessagesMu.Unlock()

	s.UnbroadcastedMessagesMu.Lock()
	s.UnbroadcastedMessages = append(s.UnbroadcastedMessages, message)
	s.UnbroadcastedMessagesMu.Unlock()
}

func (s *NodeServer) broadcastInBatch() {
	topology := s.getTopology()
	messages := s.getUnbroadcastMessages()

	for _, node := range topology {
		go s.sendWithRetries(node, BatchBroadcastBody{
			Type:     "batch_broadcast",
			Messages: messages,
		})
	}

	s.resetUnbroadcastMessages()
}

func (s *NodeServer) getUnbroadcastMessages() []int {
	s.UnbroadcastedMessagesMu.RLock()
	defer s.UnbroadcastedMessagesMu.RUnlock()

	messages := make([]int, len(s.UnbroadcastedMessages))
	copy(messages, s.UnbroadcastedMessages)
	return messages
}

func (s *NodeServer) getTopology() []string {
	s.TopologyMu.RLock()
	defer s.TopologyMu.RUnlock()

	topology := make([]string, len(s.Topology))
	copy(topology, s.Topology)
	return topology
}

func (s *NodeServer) resetUnbroadcastMessages() {
	s.UnbroadcastedMessagesMu.Lock()
	defer s.UnbroadcastedMessagesMu.Unlock()

	s.UnbroadcastedMessages = make([]int, 0)
}

func (s *NodeServer) sendWithRetries(dest string, body any) bool {
	maxRetries := 100
	for i := 0; i < maxRetries; i++ {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(2*(i+1))*time.Second)
		defer cancel()

		_, err := s.Node.SyncRPC(context, dest, body)
		if err == nil {
			return true
		}
	}

	return false
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
	if s.Node.ID() == s.Node.NodeIDs()[0] {
		s.Topology = s.Node.NodeIDs()[1:]
	} else {
		s.Topology = []string{s.Node.NodeIDs()[0]}
	}
	// s.Topology = body.Topology[s.Node.ID()]
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
	n.Handle("batch_broadcast", server.handleBatchBroadcast)

	go func() {
		for {
			server.broadcastInBatch()
			time.Sleep(700 * time.Millisecond)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
