package kafka

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// Producer Kafka Async Producer client
type Producer struct {
	asynProducer sarama.AsyncProducer
	pushTimeout  time.Duration
	encoder      Encoder
	quit         chan struct{}
}

// NewAsyncProducer create new Producer instance
func NewAsyncProducer(cfg *ProducerConfig) (*Producer, error) {
	config := sarama.NewConfig()
	config.Metadata.RefreshFrequency = 3 * time.Minute
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Partitioner = createPartitioner(cfg)
	config.Producer.Flush.Frequency = cfg.FlushFrequency
	config.Producer.Retry.Max = cfg.MaxRetry
	config.Producer.Return.Successes = true

	config.Net.DialTimeout = 3 * time.Second
	config.Net.ReadTimeout = 3 * time.Second
	config.Net.WriteTimeout = 3 * time.Second

	asynProducer, err := sarama.NewAsyncProducer(cfg.Brokers, config)
	if err != nil {
		log.Printf("Kafka Producer init err=%v", err)
		return nil, err
	}

	// 保证push时channel容量足够
	pushTimeout := cfg.FlushFrequency + 1*time.Second
	p := &Producer{
		asynProducer: asynProducer,
		pushTimeout:  pushTimeout,
		encoder:      JSONEncoder(),
		quit:         make(chan struct{}),
	}

	go func() {
		for {
			select {
			case <-p.quit: // 放在前面避免asynProducer close后为nil
				return
			case err, ok := <-p.asynProducer.Errors():
				if ok {
					msg := err.Msg
					log.Printf("Kafka Producer: fail push topic[%s]-partition[%d]-offset[%d] error=%s",
						msg.Topic, msg.Partition, msg.Offset, err.Error())
				}
			case msg, ok := <-p.asynProducer.Successes():
				if ok {
					log.Printf("Kafka Producer: success push topic[%s]-partition[%d]-offset[%d]",
						msg.Topic, msg.Partition, msg.Offset)
				}
			}
		}
	}()

	log.Println("Kafka Producer init success")
	return p, nil
}

// Push pubsh message to kafka, can be a JSON string.
//
// Attention that if config.Partitioner is set HashPartitioner, then all message will be
// pushed to the only ONE partition.
func (p *Producer) Push(topic string, msg []byte) error {
	return p.PushWithKey(topic, "", msg)
}

// PushWithKey push message to kafka with specified key, which means this message
// **should** be sent to specified partition if the config.Partitioner is HashPartitioner
func (p *Producer) PushWithKey(topic, key string, msg []byte) error {
	return p.push(topic, key, msg)
}

func (p *Producer) push(topic, key string, value []byte) error {
	if p.asynProducer == nil {
		return fmt.Errorf("Kafka Producer not init")
	}
	pMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	select {
	case p.asynProducer.Input() <- pMsg:
		return nil
	case <-time.After(p.pushTimeout):
		err := fmt.Errorf("Kafka Producer: %s publish msg timeout", p.getID(topic, key))
		return err
	}
}

func createPartitioner(cfg *ProducerConfig) sarama.PartitionerConstructor {
	switch cfg.Partitioner {
	case PartitionerRoundRobin:
		return sarama.NewRoundRobinPartitioner
	case PartitionerHash:
		return sarama.NewHashPartitioner
	default:
		return sarama.NewRandomPartitioner
	}
}

// Close application MUST close the producer to make sure the remaining messages
// are pushed to Kafka when application exit
func (p *Producer) Close() error {
	var err error
	if p.asynProducer == nil {
		return err
	}
	if err = p.asynProducer.Close(); err != nil {
		log.Printf("Kafka Producer: close fail, %v", err)
	}
	close(p.quit)
	p.asynProducer = nil
	return err
}

func (p *Producer) getID(topic, key string) string {
	return fmt.Sprintf("topic[%s]-key[%s]", topic, key)
}

// SetEncoder set encoder for produced message
func (p *Producer) SetEncoder(encoder Encoder) {
	p.encoder = encoder
}
