package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {

	if err := initConfig(); err != nil {
		log.Fatal("initialization error: ", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	kafkaSrcClient, err := sarama.NewClient(config.srcClusterBrokers, config.srcClusterConfig)
	if err != nil {
		log.Fatalf("error initializing client for cluster '%v': %v", config.srcClusterName, err)
	}
	defer kafkaSrcClient.Close()

	var kafkaDstClient sarama.Client
	if config.write {
		if config.srcClusterName == config.dstClusterName {
			kafkaDstClient = kafkaSrcClient
		} else {
			kafkaDstClient, err := sarama.NewClient(config.dstClusterBrokers, config.dstClusterConfig)
			if err != nil {
				log.Fatalf("error initializing client for cluster '%v': %v", config.dstClusterName, err)
			}
			defer kafkaDstClient.Close()
		}
	}

	processor := NewTopicProcessor(kafkaSrcClient, kafkaDstClient, config.srcTopic, config.srcGroup)

	err = processor.Run(ctx)
	if err != nil {
		log.Fatal("error initializing processor: ", err)
	}
}
