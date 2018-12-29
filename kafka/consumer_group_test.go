package kafka_test

import (
	"context"
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

func (t *tmpConsumerGroupMsgHandler) Consume(ctx context.Context, topic, key string, msg interface{}) error {
	logMsg := fmt.Sprintf("Receive message (key=%s, value=[%v]", key, msg)

	t.log.Log(logMsg)
	log.Println(logMsg)

	return nil
}

func initClient(t *testing.T) (*kafka.Client, error) {
	cfg := &kafka.ConsumerConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    []string{"test1"},
		MaxRetry: 5,
		Workers:  3,
	}
	client, err := kafka.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func initConsumerGroup(t *testing.T, groupID string, topic []string, client *kafka.Client) (*kafka.ConsumerGroup, error) {
	cg, err := kafka.NewConsumerGroupFromClient(groupID, topic, client)
	if err != nil {
		return nil, err
	}
	return cg, err
}

func TestConsumerGroup(t *testing.T) {
	log.SetFlags(log.Ldate | log.Lshortfile)
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	// Create signal channel
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	client, err := initClient(t)
	if err != nil {
		t.Error(err)
	}

	topic := "test1"
	handler := &tmpConsumerGroupMsgHandler{log: t}
	for i := 0; i < 2; i++ {
		cg, err := initConsumerGroup(t, fmt.Sprintf("kafka-test%d", i), []string{"test1"}, client)
		if err != nil {
			t.Error(err)
		}
		cg.RegisterHandler(topic, "id", kafka.JSONEncoder(), kafka.ConsumerMessageHandler(handler.Consume))
		defer func(c *kafka.ConsumerGroup) {
			if err := c.Close(); err != nil {
				log.Printf("Kafka Consumer Group close failed, %v", err)
			}
		}(cg)

		go cg.Consume()
	}

	<-sigchan
	log.Println("Receive signal, exited")
}
