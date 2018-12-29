package kafka

import (
	"time"
)

// PartitionerType define different partition category for producer,
// here are 3 choices: Random, RoundRobin and Hash
type PartitionerType int8

const (
	// PartitionerRandom 消息发送会随机发送到partition
	PartitionerRandom PartitionerType = iota
	// PartitionerRoundRobin 消息会轮询的发送到所有partition
	PartitionerRoundRobin
	// PartitionerHash 通过message key来确定发送到指定的partition，此方式可以保证顺序
	PartitionerHash
)

// ProducerConfig configuration for Producer
type ProducerConfig struct {
	Brokers        []string
	Partitioner    PartitionerType
	FlushFrequency time.Duration
	MaxRetry       int
}

// ConsumerConfig configuration for consumer group
type ConsumerConfig struct {
	Brokers []string
	Topic   []string
	GroupID string
	// Workers count of workers per each partition consumer
	Workers  int
	MaxRetry int
}

// Message for producer & consumer format
type Message struct {
	// Type is used for different register handler
	Type   string `json:"type"`
	Entity []byte `json:"entity"`
}
