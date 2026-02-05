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
* `Timestamp`
* `Topic`
* `Partition`
* `Key`
* `Value`
* `Headers`

`Timestamp` field contains seconds since Unix epoch.

If `-r` is not specified, then `msg.Value` contains decoded JSON message. Decoding and encoding is done with [yyjson-nd](https://github.com/thedolphin/lua-yyjson-nd) non-destructive library.

Three functions are available:
* `pass()` - pass a message further: print or send to Kafka
* `commit()` - if in group consumer mode, commit the offset of the current message
* `stop()` - stop partition processing

Globals are not allowed.

If filter is not specified, message is processed with specified action

<details>
<summary>Filter code wrapped in lua function.</summary>

Be careful with manual returns.

```lua
function __process__(msg)
  local __pass__, __stop__, __commit__ = false, false, false
  local function pass() __pass__ = true end
  local function stop() __stop__ = true end
  local function commit() __commit__ = true end

  -- if '-r' not specified
  msg.Value = yyjson.load_mut(msg.Value)

  -- >>> here comes user filter code

  -- if '-r' not specified
  msg.Value = tostring(msg.Value)

  return msg, __pass__, __stop__, __commit__
end
```
</details>

# Configuration

Cluster configuratin is taken from [kaf](https://github.com/birdayz/kaf) configuration. Cluster parameters are taken from cluster names in the config file. If cluster names are not specified on the command line, the default cluster name from the config is used.

# Actions

* `-p` for print message
* `-P` pretty print JSON message (mutually exclusive with `-r`)
* `-w` for send to Kafka

You can also print from the filter code, but messages in the console may be mixed due to parallel partition processing.

# Start and finish
`kaf` starts at topic start offset or group offset and finishes at newest offset got from topic at start 

# Examples

Pretty print values from topic `topic`
```
ktt -P -t topic
```

Pretty print values from topic `topic` and copy to kafka cluster `cluster2`
```
ktt -P -w -t topic -d cluster2
```

Find values in topic `topic` , pretty print and copy to kafka cluster `cluster2` to other topic and partition
```
ktt -P -w -t topic -d cluster2 -g somegroup -e 'if msg.Value.field1 == "value" then msg.Topic = "newtopic" msg.Partition = 0 pass() end'
```

Skip invalid values, resend valid values, shift offset
```
ktt -w -t topic -e 'if msg.Value.field1 != "value" then pass() end commit()'
```

Unwrap message field and send to other topic in other cluster with repartition
```
ktt -w -t topic -d cluster2 -e 'msg.Value = msg.Value.some_field msg.Topic = newTopic msg.Partition = hash.xxhash(msg.Value.id)'
```
