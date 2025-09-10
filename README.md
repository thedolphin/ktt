# Kafka Topic Tool

A tool for processing Kafka topic messages, for example:
* finding messages by JSON field value
* copying selected messages to another cluster and topic
* shifting consumer group offset to the found message
* fixing and resending messages with offset
* repartition the topic
* ...and much more

# Filter

The filter specified by `-e` is Lua code, with `msg` table defined. Every field of `msg` can be modified.
`msg` contains the following fields:
* "Timestamp"
* "Topic"
* "Partition"
* "Key"
* "Value"
* "Headers"

Three functions are available:
* `pass()` - pass a message further: print or send to Kafka
* `commit()` - if in group consumer mode, commit the offset of the current message
* `stop()` - stop partition processing

If filter is not specified, message is processed with specified action

# Configuration

Cluster configuratin is taken from [kaf](https://github.com/birdayz/kaf) configuration. Cluster parameters are taken from cluster names in the config file. If cluster names are not specified on the command line, the default cluster name from the config is used.

# Actions

* `-p` for print the original message
* `-w` for send to Kafka

You can also print from the filter code, but messages in the console may be mixed due to parallel partition processing.

# Start and finish
`kaf` starts at topic start offset or group offset and finishes at newest offset got from topic at start 

# Examples

Pretty print values from topic `topic`
```
ktt -p -t topic
```

Pretty print values from topic `topic` and copy to kafka cluster `cluster2`
```
ktt -p -w -t topic -d cluster2
```

Find values in topic `topic` , pretty print and copy to kafka cluster `cluster2` to other topic and partition
```
ktt -p -w -t topic -d cluster2 -e 'if msg.Value.field1 == "value" then msg.Topic = "newtopic" msg.Partition = 0 pass() end'
```

Skip invalid values, resend valid values, shift offset
```
ktt -p -w -t topic -e 'if msg.Value.field1 != "value" then pass() end commit()'
```
