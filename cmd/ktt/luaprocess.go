package main

import (
	"fmt"
	"strings"
	"time"

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

	filter := &strings.Builder{}

	filter.WriteString(`function __process__(msg) ` +
		`local __pass__, __stop__, __commit__ = false, false, false ` +
		`local function pass() __pass__ = true end ` +
		`local function stop() __stop__ = true end ` +
		`local function commit() __commit__ = true end`)

	if !config.raw {
		filter.WriteString(` msg.Value = yyjson.load_mut(msg.Value)`)
	}

	filter.WriteByte('\n')
	filter.WriteString(config.filter)
	filter.WriteByte('\n')

	if !config.raw {
		filter.WriteString("msg.Value = tostring(msg.Value) ")
	}

	filter.WriteString("return msg, __pass__, __stop__, __commit__ end")

	err = lua.Load(filter.String())
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
	uint8, *sarama.ConsumerMessage, error,
) {

	headers := make(map[string]any, len(msg.Headers))
	for _, header := range msg.Headers {
		headers[string(header.Key)] = string(header.Value)
	}

	lua.GetGlobal("__process__")
	lua.Push(map[string]any{
		"Timestamp": msg.Timestamp.Unix(), // Lua uses Unix Epoch
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

	var retMsg *sarama.ConsumerMessage

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

	retMsg = &sarama.ConsumerMessage{
		Timestamp: time.Unix(int64(As[float64](v["Timestamp"], &ok)), 0),
		Topic:     As[string](v["Topic"], &ok),
		Partition: int32(As[float64](v["Partition"], &ok)),
		Key:       []byte(As[string](v["Key"], &ok)),
		Value:     []byte(As[string](v["Value"], &ok)),
		Headers:   make([]*sarama.RecordHeader, len(retHeaders)),
	}

	if !ok {
		return 0, nil, fmt.Errorf("error parsing message: cannot cast one of Msg fields: Timestamp[number], Topic[string], Partition[number], Key[string], Value[string]")
	}

	for headerKey, headerValue := range retHeaders {
		retMsg.Headers = append(retMsg.Headers, &sarama.RecordHeader{
			Key:   []byte(headerKey),
			Value: []byte(As[string](headerValue, &ok))})
	}

	if !ok {
		return 0, nil, fmt.Errorf("error parsing msg: cannot cast Headers values to string")
	}

	return flags, retMsg, nil
}
