package main

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/thedolphin/luarunner"
)

const (
	LuaResultPass = uint8(1) << iota
	LuaResultStop
	LuaResultCommit
)

func luaInit() (*luarunner.LuaRunner, error) {

	if len(config.filter) == 0 {
		return nil, nil
	}

	lua, err := luarunner.New()
	if err != nil {
		return nil, err
	}

	lua.StrictRead()

	filter := `function __process__(msg) ` +
		`local __pass__, __stop__, __commit__ = false, false, false ` +
		`local function pass() __pass__ = true end ` +
		`local function stop() __stop__ = true end ` +
		`local function commit() __commit__ = true end`

	if !config.raw {
		filter += ` msg.Value = yyjson.load_mut(msg.Value)`
	}

	filter += "\n" + config.filter + "\n"

	if config.write {
		if !config.raw {
			filter += "msg.Value = tostring(msg.Value) "
		}
		filter += "return msg, __pass__, __stop__, __commit__ end"
	} else {
		filter += "return __pass__, __stop__, __commit__ end"
	}

	err = lua.Load(filter)
	if err != nil {
		lua.Close()
		return nil, err
	}

	err = lua.Run()
	if err != nil {
		lua.Close()
		return nil, err
	}

	lua.StrictWrite()

	return lua, nil
}

func luaProcess(
	lua *luarunner.LuaRunner,
	msg *sarama.ConsumerMessage,
) (
	uint8, *sarama.ProducerMessage, error,
) {

	headers := make(map[string]any, len(msg.Headers))
	for _, header := range msg.Headers {
		headers[string(header.Key)] = string(header.Value)
	}

	lua.GetGlobal("__process__")
	lua.Push(map[string]any{
		"Timestamp": msg.Timestamp,
		"Topic":     msg.Topic,
		"Partition": msg.Partition,
		"Key":       msg.Key,
		"Value":     msg.Value,
		"Headers":   headers,
	})

	err := lua.Call(1, -1) // LUA_MULTRET
	if err != nil {
		return 0, nil, fmt.Errorf("error calling filter code: %w", err)
	}

	var flags uint8

	// flags
	for mask := LuaResultCommit; mask > 0; mask >>= 1 {
		retValueAny, _ := lua.Pop()
		if retValueAny.(bool) {
			flags |= mask
		}
	}

	var retMsg *sarama.ProducerMessage
	if config.write {

		vAny, err := lua.Pop()
		if err != nil {
			return 0, nil, fmt.Errorf("error getting message return value: %w", err)
		}

		ok := true
		v := As[map[string]any](vAny, &ok)
		if !ok {
			return 0, nil, fmt.Errorf("error parsing message: cannot cast to map[string]any, got %T", vAny)
		}

		retHeaders := As[map[string]any](v["Headers"], &ok)
		if !ok {
			return 0, nil, fmt.Errorf("error parsing message: cannot cast Headers field to map[string]any, got %T", v["Headers"])
		}

		retMsg = &sarama.ProducerMessage{
			Topic:     As[string](v["Topic"], &ok),
			Partition: int32(As[float64](v["Partition"], &ok)),
			Key:       sarama.StringEncoder(As[string](v["Key"], &ok)),
			Value:     sarama.StringEncoder(As[string](v["Value"], &ok)),
			Headers:   make([]sarama.RecordHeader, len(retHeaders)),
		}

		if !ok {
			return 0, nil, fmt.Errorf("error parsing message: cannot cast one of Msg fields: Topic[string], Partition[float64], Key[string], Value[string]")
		}

		for headerKey, headerValue := range retHeaders {
			retMsg.Headers = append(retMsg.Headers, sarama.RecordHeader{
				Key:   []byte(headerKey),
				Value: []byte(As[string](headerValue, &ok))})
		}

		if !ok {
			return 0, nil, fmt.Errorf("error parsing msg: cannot cast Headers values to string")
		}
	}

	return flags, retMsg, nil
}
