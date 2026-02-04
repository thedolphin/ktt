package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

var Version = "dev"

func main() {

	fmt.Fprintln(os.Stderr, "ktt", Version)

	if err := initConfig(); err != nil {
		fmt.Fprintln(os.Stderr, "initialization error:", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	kafkaSrcClient, err := sarama.NewClient(config.srcClusterBrokers, config.srcClusterConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error initializing client for cluster '%v': %v\n", config.srcClusterName, err)
		return
	}
	defer kafkaSrcClient.Close()

	var kafkaDstClient sarama.Client
	if config.write {
		if config.srcClusterName == config.dstClusterName {
			kafkaDstClient = kafkaSrcClient
		} else {
			kafkaDstClient, err := sarama.NewClient(config.dstClusterBrokers, config.dstClusterConfig)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error initializing client for cluster '%v': %v\n", config.dstClusterName, err)
				return
			}
			defer kafkaDstClient.Close()
		}
	}

	processor := NewTopicProcessor(kafkaSrcClient, kafkaDstClient, config.srcTopic, config.srcGroup, &Output{})

	err = processor.Run(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error initializing processor:", err)
	}
}
