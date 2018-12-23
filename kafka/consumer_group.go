package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// StatusCode represent the message status
type StatusCode int

const (
	// StatusNormal means message is consumed firstly
	StatusNormal StatusCode = iota
	// StatusRetry means message is consumed more then once
	StatusRetry
)

// Message represent the received message body from Kafka
type Message struct {
	// Key is used for register handler
	Key    string `json:"key"`
	Status int    `json:"status"`
	Entity []byte `json:"entity"`
}

// ConsumerGroupMessageHandler will be implemented by user
type ConsumerGroupMessageHandler interface {
	Consume(msg *Message) error
}

type consumerGroupMsgHandler struct {
	msgHanlder    ConsumerGroupMessageHandler
	consumerGroup *ConsumerGroup
	quit          chan struct{}
	msgPool       chan *Message
}

func (h consumerGroupMsgHandler) Setup(ccs sarama.ConsumerGroupSession) error {
	h.quit = make(chan struct{})
	h.msgPool = make(chan *Message, 256)
	for i := 0; i < h.consumerGroup.cfg.Workers; i++ {
		go func(workerNo int) {
			log.Printf("Consumer Group: worker[%d] startup", workerNo)
			for {
				select {
				case msg, ok := <-h.msgPool:
					if !ok {
						return
					}
					for i := 0; i < h.consumerGroup.cfg.MaxRetry; i++ {
						err := h.msgHanlder.Consume(msg)
						if err == nil {
							break
						}
					}
				case <-h.quit:
					return
				}
			}
		}(i)
	}
	return nil
}

func (h consumerGroupMsgHandler) Cleanup(ccs sarama.ConsumerGroupSession) error {
	h.quit <- struct{}{}
	close(h.quit)
	close(h.msgPool)
	return nil
}

func (h consumerGroupMsgHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		m := &Message{}
		err := json.Unmarshal(msg.Value, m)
		if err != nil {
			log.Fatalf("Consumer Group: unexpected message struct, err[%v]", err)
		} else {
			h.msgPool <- m
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

// ConsumerGroup instance of high level consumer group
type ConsumerGroup struct {
	clientName string
	cfg        *ConsumerConfig
	cg         sarama.ConsumerGroup
	handlers   map[string]map[string]ConsumerGroupMessageHandler
}

// NewConsumerGroup init a sarama consumer-group
func NewConsumerGroup(cfg *ConsumerConfig) (*ConsumerGroup, error) {
	c := sarama.NewConfig()
	c.Version = sarama.V0_10_2_0
	c.Metadata.RefreshFrequency = 1 * time.Minute
	c.Consumer.Offsets.Initial = sarama.OffsetNewest
	c.Consumer.Return.Errors = true
	clientName := generateClientID(cfg.GroupID)
	c.ClientID = clientName
	cg, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, c)
	if err != nil {
		return nil, err
	}
	go func() {
		for err := range cg.Errors() {
			log.Printf("Consumer Group: [%s] consumer err %v", cfg.GroupID, err)
		}
	}()

	consumerGroup := &ConsumerGroup{
		clientName: clientName,
		cfg:        cfg,
		cg:         cg,
		handlers:   make(map[string]map[string]ConsumerGroupMessageHandler),
	}
	return consumerGroup, nil
}

// Consume start to consume messages
func (c *ConsumerGroup) Consume() {
	if len(c.handlers) == 0 {
		log.Fatalf("Consumer Group: no handlers for group [%s]", c.cfg.GroupID)
	}
	h := consumerGroupMsgHandler{consumerGroup: c}
	for {
		err := c.cg.Consume(context.Background(), c.cfg.Topic, h)
		if err != nil {
			log.Println(err)
		}
	}
}

//RegisterHandler based on topic and message key, register related hanlder
func (c *ConsumerGroup) RegisterHandler(topic, key string, handler ConsumerGroupMessageHandler) {
	if keyHandler, ok := c.handlers[topic]; ok {
		if _, ok := keyHandler[key]; !ok {
			keyHandler[key] = handler
		} else {
			log.Printf("Consumer Group: repeatedly registed handler for topic[%s]-key[%s]", topic, key)
		}
	} else {
		c.handlers[topic] = make(map[string]ConsumerGroupMessageHandler)
		c.handlers[topic][key] = handler
	}
}

// Close consumer group to release resources
func (c *ConsumerGroup) Close() error {
	return c.cg.Close()
}
