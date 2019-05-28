package kafka

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

const (
	defaultPatitionConsumerWorkers = 1
)

// ConsumerMessage struct of passed to message handler
type ConsumerMessage struct {
	*sarama.ConsumerMessage
}

// MessageHandler registed by application to process message, each consumer has one message handler.
// Application should handle different message struct per different topic if he consumes multiple topics.
// Returned error means consume failed, then message will be retied ConsumerConfig.MaxRetry times.
type MessageHandler interface {
	Consume(msg *ConsumerMessage) error
}

// Consumer kafka consumer group
// Group means a biz unit, should process the related biz based on subscribed topics
type Consumer struct {
	clientName       string
	cfg              *ConsumerConfig
	consumer         *cluster.Consumer
	msgHandler       MessageHandler
	partitionWorkers []*partitionConsumerWorker
}

// NewConsumer create a consumer to subscribe many topics
func NewConsumer(cfg *ConsumerConfig, handler MessageHandler) (*Consumer, error) {
	clusterConfig := cluster.NewConfig()
	clusterConfig.Metadata.RefreshFrequency = 1 * time.Minute
	clusterConfig.Group.Mode = cluster.ConsumerModePartitions
	clusterConfig.Group.Return.Notifications = true
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	clusterConfig.Consumer.Return.Errors = true
	clientName := generateClientID(cfg.GroupID)
	clusterConfig.ClientID = clientName

	c, err := cluster.NewConsumer(cfg.Brokers, cfg.GroupID, cfg.Topic, clusterConfig)
	if err != nil {
		log.Printf("Kafka Consumer: [%s] init fail, %v", clientName, err)
		return nil, err
	}

	validConfigValue(cfg)
	consumer := &Consumer{
		clientName:       clientName,
		cfg:              cfg,
		consumer:         c,
		msgHandler:       handler,
		partitionWorkers: make([]*partitionConsumerWorker, 0),
	}
	log.Printf("Kafka Consumer: [%s] init success", clientName)

	return consumer, nil
}

// generate client ID for debug, default use groupID-hostName,
// but if get host name failed, then use the unix nano time
func generateClientID(groupID string) string {
	hostName, err := os.Hostname()
	if err != nil || len(hostName) == 0 {
		now := time.Now().UnixNano()
		hostName = strconv.FormatInt(now, 10)
	}
	return fmt.Sprintf("%s-%s", groupID, hostName)
}

func validConfigValue(cfg *ConsumerConfig) {
	if cfg.Workers <= 0 {
		cfg.Workers = defaultPatitionConsumerWorkers
	}
	if cfg.MaxRetry < 0 {
		cfg.MaxRetry = 0
	}
}

// Close consumer
func (c *Consumer) Close() error {
	for _, w := range c.partitionWorkers {
		w.close()
	}
	err := c.consumer.Close()
	log.Printf("Kafka Consumer: [%s] consumer quit", c.clientName)
	return err
}

// Consume message per partition, each partition have numbers of workers
func (c *Consumer) Consume() {
	log.Printf("Kafka Consumer: [%s] start consume", c.clientName)
	for {
		select {
		case p, ok := <-c.consumer.Partitions():
			if !ok {
				log.Printf("Kafka Consumer: [%s] partition chan closed", c.clientName)
				return
			}
			for i := 0; i < c.cfg.Workers; i++ {
				worker := c.newPartitionConsumerWorker(p, i)
				c.partitionWorkers = append(c.partitionWorkers, worker)
			}
		case n, ok := <-c.consumer.Notifications():
			if !ok {
				log.Printf("Kafka Consumer: [%s] notification chan closed", c.clientName)
				return
			}
			log.Printf("Kafka Consumer: [%s] %s, info{claimed: %v, released: %v, current: %v}",
				c.clientName, n.Type.String(), n.Claimed, n.Released, n.Current)
			switch n.Type {
			case cluster.RebalanceStart:
				c.closePartitionWorkerWithRebalance()
			case cluster.RebalanceError:
				log.Printf("Kafka Consumer: [%s] rebalance error, should restart", c.clientName)
				for _, worker := range c.partitionWorkers {
					// close worker and partition consumer, will trigger a rebalance again
					worker.close()
				}
				// or just delete consumer directly
				// go c.Close()
			case cluster.RebalanceOK:
				for _, worker := range c.partitionWorkers {
					worker.waitGroup.Add(1)
					go worker.startConsume()
				}
			default:
			}
		case err, ok := <-c.consumer.Errors():
			if !ok {
				log.Printf("Kafka Consumer: [%s] error chan closed", c.clientName)
				return
			}
			log.Printf("Kafka Consumer: [%s] error: %v", c.clientName, err)
		}
	}
}

// Release previous workers after rebalance as it will create new one.
// Pls note the partitionConsumer will be closed by sarama-cluster automatically, no need to close again.
func (c *Consumer) closePartitionWorkerWithRebalance() {
	if len(c.partitionWorkers) == 0 {
		return
	}
	c.partitionWorkers = nil // mark as workers can be gc
}

type partitionConsumerWorker struct {
	name      string
	pc        cluster.PartitionConsumer
	waitGroup sync.WaitGroup
	handler   MessageHandler
	maxRetry  int
	workerNo  int
}

func (c *Consumer) newPartitionConsumerWorker(pc cluster.PartitionConsumer, workerNo int) *partitionConsumerWorker {
	name := fmt.Sprintf("(%s<%s>)-%s-p%d-%d", c.clientName, time.Now().Format("20060102150405"), pc.Topic(), pc.Partition(), workerNo)
	return &partitionConsumerWorker{
		pc:       pc,
		workerNo: workerNo,
		maxRetry: c.cfg.MaxRetry + 1, // must execute once
		handler:  c.msgHandler,
		name:     name,
	}
}

func (w *partitionConsumerWorker) startConsume() {
	log.Printf("Kafka Consumer: start partition consumer [%s]", w.name)
	defer func() {
		w.waitGroup.Done()
	}()
	for {
		select {
		case msg, more := <-w.pc.Messages():
			if !more {
				log.Printf("Kafka Consumer: [%s] partition msg chan closed", w.name)
				return
			}
			for i := 0; i < w.maxRetry; i++ {
				err := w.handler.Consume(&ConsumerMessage{msg})
				if err == nil {
					break
				}
				time.Sleep(1 * time.Second) // release CPU during retrying
			}
			w.pc.MarkOffset(msg.Offset, "")
		case err, ok := <-w.pc.Errors():
			if !ok {
				log.Printf("Kafka Consumer: [%s] partition error chan closed", w.name)
				return
			}
			log.Printf("Kafka Consumer: [%s] receive msg error(%s)", w.name, err.Error())
		}
	}
}

func (w *partitionConsumerWorker) close() {
	w.pc.AsyncClose() // will trigger a rebalance
	w.waitGroup.Wait()
	log.Printf("Kafka Consumer: [%s] finished work", w.name)
}
