package main

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"golang.org/x/sync/errgroup"
)

type TopicProcessor struct {
	consumerClient sarama.Client
	producerClient sarama.Client
	topic          string
	cg             string // Consumer Group name
	om             sarama.OffsetManager
	consumer       sarama.Consumer
	producer       sarama.SyncProducer
}

func (c *TopicProcessor) Run(ctx context.Context) error {

	log.Print("starting consumer")
	defer log.Print("consumer stopped")

	partitions, err := c.consumerClient.Partitions(c.topic)
	if err != nil {
		return fmt.Errorf("error getting partitions: %w", err)
	}

	c.consumer, err = sarama.NewConsumerFromClient(c.consumerClient)
	if err != nil {
		return fmt.Errorf("error initializing consumer: %w", err)
	}
	defer c.consumer.Close()

	if c.producerClient != nil {
		c.producer, err = sarama.NewSyncProducerFromClient(c.producerClient)
		if err != nil {
			return fmt.Errorf("error initializing producer: %w", err)
		}
		defer c.producer.Close()
	}

	if len(c.cg) > 0 {
		c.om, err = sarama.NewOffsetManagerFromClient(c.cg, c.consumerClient)
		if err != nil {
			return fmt.Errorf("error initializing offset manager: %w", err)
		}
		defer c.om.Close()
	}

	var pps []*PartitionProcessor
	for _, partition := range partitions {
		pp, err := c.NewPartitionProcessor(partition)
		if err != nil {
			return fmt.Errorf("error instantiating partition processor: %w", err)
		}
		if pp != nil {
			pps = append(pps, pp)
		}
	}

	errGrp, errCtx := errgroup.WithContext(ctx)

	for _, pp := range pps {
		errGrp.Go(func() error {
			return pp.Process(errCtx)
		})
	}

	log.Print("before wait")
	return errGrp.Wait()
}

func NewTopicProcessor(consumerClient, producerClient sarama.Client, topic string, cg string) *TopicProcessor {

	return &TopicProcessor{
		consumerClient: consumerClient,
		producerClient: producerClient,
		topic:          topic,
		cg:             cg,
	}
}
