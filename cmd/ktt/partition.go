package main

import (
	"context"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/thedolphin/luarunner"
)

type PartitionProcessor struct {
	newest    int64
	partition int32
	lua       *luarunner.LuaRunner
	pom       sarama.PartitionOffsetManager
	pc        sarama.PartitionConsumer
	output    *Output
}

func (c *TopicProcessor) NewPartitionProcessor(partition int32) (*PartitionProcessor, error) {

	var err error
	pp := &PartitionProcessor{
		partition: partition,
		output:    c.output,
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

	fmt.Fprintf(os.Stderr, "got offsets for partition #%d: %d->%d\n", partition, oldest, pp.newest)

	return pp, nil
}

func (pp *PartitionProcessor) Process(ctx context.Context) error {

	fmt.Fprintf(os.Stderr, "starting partition %d consumer\n", pp.partition)
	defer fmt.Fprintf(os.Stderr, "stopping partition %d consumer\n", pp.partition)

	if pp.pom != nil {
		defer pp.pom.AsyncClose()
	}

	defer pp.pc.Close()

	run := true
	for run {
		select {
		case <-ctx.Done():
			run = false

		case msg, ok := <-pp.pc.Messages():
			if !ok {
				return nil
			}

			if msg.Offset >= pp.newest-1 {
				run = false
			}

			var sendMsg *sarama.ConsumerMessage

			if pp.lua == nil {

				sendMsg = msg

			} else {

				var (
					flags uint8
					err   error
				)

				if flags, sendMsg, err = luaProcess(pp.lua, msg); err != nil {
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

			pp.output.Write(sendMsg)

		}
	}

	return nil
}
