package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	kafconfig "github.com/birdayz/kaf/pkg/config"
)

var config struct {
	kafConfig         kafconfig.Config "github.com/birdayz/kaf/pkg/config"
	kafConfigFile     string
	filter            string
	script            string
	srcClusterConfig  *sarama.Config
	srcClusterBrokers []string
	srcClusterName    string
	srcTopic          string
	srcGroup          string
	dstClusterConfig  *sarama.Config
	dstClusterName    string
	dstClusterBrokers []string
	print             bool
	write             bool
	raw               bool
}

func initConfig() (err error) {

	// flag.Usage = func() {
	// 	fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
	// 	flag.PrintDefaults()
	// }

	flag.StringVar(&config.kafConfigFile, "c", "", "kaf config file location, default $HOME/.kaf/config")
	flag.StringVar(&config.filter, "e", "", "lua script for filtering and mangling")
	flag.StringVar(&config.script, "f", "", "lua script file for filtering and mangling")
	flag.StringVar(&config.srcClusterName, "s", "", "source kafka cluster name, default active cluster")
	flag.StringVar(&config.dstClusterName, "d", "", "destination kafka cluster name, default active cluster")
	flag.StringVar(&config.srcTopic, "t", "", "source topic name")
	flag.StringVar(&config.srcGroup, "g", "", "consumer group name")
	flag.BoolVar(&config.write, "w", false, "send found and modified messages to kafka")
	flag.BoolVar(&config.print, "p", false, "print found original messages")
	flag.BoolVar(&config.raw, "r", false, "pass raw message value to filter, disable JSON parsing")

	flag.Parse()

	if !(config.print || config.write) {
		return errors.New(`action, "-p" or "-w", must be specified`)
	}

	if len(config.srcTopic) == 0 {
		return errors.New(`topic, "-t" must be specified`)
	}

	if len(config.script) > 0 {
		if len(config.filter) > 0 {
			return errors.New(`"-e" and "-f" are mutually exclusive`)
		}
		script, err := os.ReadFile(config.script)
		if err != nil {
			return fmt.Errorf("error while reading script file: %w", err)
		}

		config.script = string(script)
	}

	config.kafConfig, err = kafconfig.ReadConfig(config.kafConfigFile)
	if err != nil {
		return
	}

	config.srcClusterBrokers, config.srcClusterConfig, err = newSaramaConfig(config.srcClusterName)
	if err != nil {
		return
	}

	if config.write {
		if config.dstClusterName == config.srcClusterName {
			config.dstClusterBrokers = config.srcClusterBrokers
			config.dstClusterConfig = config.srcClusterConfig
			return
		}

		config.dstClusterBrokers, config.dstClusterConfig, err = newSaramaConfig(config.dstClusterName)
	}

	return
}

func newSaramaConfig(clusterName string) (brokers []string, saramaCfg *sarama.Config, err error) {

	var kafCluster *kafconfig.Cluster

	config.kafConfig.ClusterOverride = clusterName
	kafCluster = config.kafConfig.ActiveCluster()

	if len(clusterName) == 0 {
		if len(config.kafConfig.CurrentCluster) == 0 {
			err = errors.New("no cluster name specified and no current-cluster defined")
			return
		}

		clusterName = config.kafConfig.CurrentCluster
	}

	for _, item := range config.kafConfig.Clusters {
		if item.Name == clusterName {
			kafCluster = item
			break
		}
	}

	if kafCluster == nil {
		err = fmt.Errorf("cluster '%v' not found in config", clusterName)
		return
	}

	saramaCfg = sarama.NewConfig()
	brokers = make([]string, len(kafCluster.Brokers))
	copy(brokers, kafCluster.Brokers)

	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Producer.Return.Successes = true

	if kafCluster.TLS != nil {
		saramaCfg.Net.TLS.Enable = true
	}

	if kafCluster.SASL != nil {
		if len(kafCluster.SASL.Username) > 0 {
			saramaCfg.Net.SASL.Enable = true
			saramaCfg.Net.SASL.User = kafCluster.SASL.Username

			if len(kafCluster.SASL.Password) > 0 {
				saramaCfg.Net.SASL.Password = kafCluster.SASL.Password
			}

			if len(kafCluster.SASL.Mechanism) > 0 {
				switch kafCluster.SASL.Mechanism {
				case "PLAIN":
					saramaCfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
				case "SCRAM-SHA-256":
					saramaCfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
					saramaCfg.Net.SASL.SCRAMClientGeneratorFunc = XdgSha256ScramClientGenerator
				case "SCRAM-SHA-512":
					saramaCfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
					saramaCfg.Net.SASL.SCRAMClientGeneratorFunc = XdgSha512ScramClientGenerator
				}
			}
		}
	}

	return
}
