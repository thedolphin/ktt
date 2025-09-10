package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/thedolphin/luarunner"
)

type PartitionProcessor struct {
	newest    int64
	partition int32
	lua       *luarunner.LuaRunner
	pom       sarama.PartitionOffsetManager
	pc        sarama.PartitionConsumer
	producer  sarama.SyncProducer
}

func (c *TopicProcessor) NewPartitionProcessor(partition int32) (*PartitionProcessor, error) {

	var err error
	pp := &PartitionProcessor{
		partition: partition,
		producer:  c.producer,
	}

	pp.lua, err = luaInit()
	if err != nil {
		return nil, fmt.Errorf("error initializing lua: %w", err)
	}

	pp.newest, err = c.consumerClient.GetOffset(config.srcTopic, partition, sarama.OffsetNewest)
	if err != nil {
		return nil, fmt.Errorf("error getting newest offset: %w", err)
	}

	oldest := int64(-1)

	if c.om != nil {
		pp.pom, err = c.om.ManagePartition(c.topic, partition)
		if err != nil {
			return nil, fmt.Errorf("error instantiating partition offset manager: %w", err)
		}

		oldest, _ = pp.pom.NextOffset() // defaults to config.srcClusterConfig.Consumer.Offsets.Initial
	}

	if oldest < 0 {
		oldest, err = c.consumerClient.GetOffset(config.srcTopic, partition, sarama.OffsetOldest)
		if err != nil {
			return nil, fmt.Errorf("error getting initial offset for group: %w", err)
		}
	}

	if oldest >= pp.newest { // nothing to read in partition
		if pp.pom != nil {
			pp.pom.AsyncClose()
		}
		return nil, nil
	}

	pp.pc, err = c.consumer.ConsumePartition(c.topic, partition, oldest)
	if err != nil {
		if pp.pom != nil {
			pp.pom.AsyncClose()
		}
		return nil, fmt.Errorf("error instatiating partition consumer: %w", err)
	}

	log.Printf("got offsets for partition #%d: %d->%d", partition, oldest, pp.newest)

	return pp, nil
}

func (pp *PartitionProcessor) Process(ctx context.Context) error {

	log.Printf("starting partition %d consumer", pp.partition)
	defer log.Printf("stopping partition %d consumer", pp.partition)

	if pp.pom != nil {
		defer pp.pom.AsyncClose()
	}

	defer pp.pc.Close()

	run := true
	for run {
		select {
		case msg, ok := <-pp.pc.Messages():
			if !ok {
				return nil
			}

			if msg.Offset >= pp.newest-1 {
				run = false
			}

			var (
				sendMsg *sarama.ProducerMessage
				flags   uint8
				err     error
			)

			if pp.lua != nil {

				flags, sendMsg, err = luaProcess(pp.lua, msg)
				if err != nil {
					return fmt.Errorf("error processing message: %w", err)
				}

				if flags&LuaResultCommit > 0 && pp.pom != nil {
					pp.pom.MarkOffset(msg.Offset+1, "")
				}
				if flags&LuaResultStop > 0 {
					run = false
				}
				if flags&LuaResultPass == 0 {
					continue
				}
			}

			log.Printf("part: %d, ofs: %d", msg.Partition, msg.Offset)

			if config.print {
				if config.raw {
					fmt.Print(string(msg.Value))
				} else {
					var pretty bytes.Buffer
					if err := json.Indent(&pretty, msg.Value, "", "  "); err != nil {
						return fmt.Errorf("cannot format json message: %w", err)
					}
					fmt.Print(pretty.String())
				}
			}

			if pp.producer != nil {
				pp.producer.SendMessage(sendMsg)
			}

		case <-ctx.Done():
			run = false
		}
	}

	return nil
}
