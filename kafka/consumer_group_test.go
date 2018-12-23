package kafka_test

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/PoacherBro/golab/kafka"
	"github.com/Shopify/sarama"
)

type tmpConsumerGroupMsgHandler struct {
	log *testing.T
}

func (t *tmpConsumerGroupMsgHandler) Consume(msg *kafka.Message) error {
	logMsg := fmt.Sprintf("Receive message (key=%s, value=[%s]", string(msg.Key), string(msg.Entity))

	t.log.Log(logMsg)
	log.Println(logMsg)

	return nil
}

func initConsumerGroup(t *testing.T) (*kafka.ConsumerGroup, error) {
	cfg := &kafka.ConsumerConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    []string{"test1"},
		MaxRetry: 5,
		GroupID:  "kafka-test",
		Workers:  3,
	}
	handler := &tmpConsumerGroupMsgHandler{log: t}
	cg, err := kafka.NewConsumerGroup(cfg)
	if err != nil {
		return nil, err
	}
	cg.RegisterHandler("test1", "testkey", handler)
	return cg, err
}

func TestConsumerGroup(t *testing.T) {
	log.SetFlags(log.Ldate | log.Lshortfile)
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	cg, err := initConsumerGroup(t)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := cg.Close(); err != nil {
			log.Printf("Kafka Consumer Group close failed, %v", err)
		}
	}()

	// Create signal channel
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	go cg.Consume()

	<-sigchan
	log.Println("Receive signal, exited")
}
