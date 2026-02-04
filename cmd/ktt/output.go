package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

type Output struct {
	m        sync.Mutex
	producer sarama.SyncProducer
}

func (o *Output) Write(cm *sarama.ConsumerMessage) error {

	var errs []error

	if o.producer != nil {

		pm := &sarama.ProducerMessage{
			Topic:     cm.Topic,
			Partition: cm.Partition,
			Offset:    cm.Offset,
			Timestamp: cm.Timestamp,
			Key:       sarama.ByteEncoder(cm.Key),
			Value:     sarama.ByteEncoder(cm.Value),
			Headers:   make([]sarama.RecordHeader, len(cm.Headers)),
		}

		for i := range cm.Headers {
			pm.Headers[i] = *cm.Headers[i]
		}

		if _, _, err := o.producer.SendMessage(pm); err != nil {
			errs = append(errs, err)
		}
	}

	var buf *bytes.Buffer

	if config.print {

		if config.prettyPrint {
			buf = &bytes.Buffer{}
			if err := json.Indent(buf, cm.Value, "", "  "); err != nil {
				errs = append(errs, fmt.Errorf("cannot format json message: %w", err))
			}
		} else {
			buf = bytes.NewBuffer(cm.Value)

		}

		buf.WriteByte('\n')
		defer o.m.Unlock()
		o.m.Lock()
	}

	fmt.Fprintf(os.Stderr, "part: %d, ofs: %d, ts: %s\n", cm.Partition, cm.Offset, cm.Timestamp.Format("2006-01-02 15:04:05"))

	if config.print {
		buf.WriteTo(os.Stdout)
	}

	return errors.Join(errs...)
}
