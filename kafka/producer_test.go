package kafka_test

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/PoacherBro/golab/kafka"
	"github.com/Shopify/sarama"
)

func initProducer() (*kafka.Producer, error) {
	cfg := &kafka.ProducerConfig{
		Brokers:        []string{"localhost:9092"},
		MaxRetry:       5,
		Partitioner:    kafka.PartitionerRandom,
		FlushFrequency: 1 * time.Second,
	}

	return kafka.NewAsyncProducer(cfg)
}

func TestProducer(t *testing.T) {
	log.SetFlags(log.Ldate | log.Lshortfile)
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	topics := []string{"test1"}
	producer, err := initProducer()
	if err != nil {
		t.Error(err)
	}
	defer producer.Close()

	// Create signal channel
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for _, topic := range topics {
		go func(topic string) {
			for {
				key := "leo"
				t := time.Now()
				value := t.Format("20060102150405")
				// err := producer.Push(topic, value)
				err := producer.PushWithKey(topic, key, []byte(value))
				if err != nil {
					log.Printf(fmt.Sprintf("Topic[%s] push msg err=%v", topic, err))
				}
				log.Printf("Pushed value: key[%s]-value[%s]", key, value)

				time.Sleep(3 * time.Second)
			}
		}(topic)
	}

	<-sigchan
}
