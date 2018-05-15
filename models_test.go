/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestSettings() *Settings {
	v, err := NewMessageValidator("schema.json")
	if err != nil {
		panic(err)
	}
	return &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		Publisher:    "myapp",
		QueueName:    "DEV-MYAPP",
		Validator:    v,
	}
}

func TestCreateMetadata(t *testing.T) {
	assertions := assert.New(t)

	now := time.Now()
	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()

	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	assertions.Equal(headers, metadata.Headers)
	assertions.Equal(settings.Publisher, metadata.Publisher)
	assertions.Empty(metadata.Receipt)
	assertions.True(time.Time(metadata.Timestamp).UnixNano() > now.UnixNano())
}

func TestNewMessageWithIDSuccess(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{}

	m, err := newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, &data)
	require.NoError(t, err)

	expectedSchema := fmt.Sprintf(
		"%s#/schemas/%s/%s", settings.Validator.SchemaRoot(), msgDataType, msgDataSchemaVersion)

	assertions.Equal(data, *m.Data.(*FakeHedwigDataField))
	assertions.Equal(headers, metadata.Headers)
	assertions.Equal(settings.Publisher, metadata.Publisher)
	assertions.Equal(FormatVersionV1, m.FormatVersion)
	assertions.Equal(id, m.ID)
	assertions.Equal(expectedSchema, m.Schema)
	assertions.Equal(msgDataType, m.dataType)
	assertions.Equal(msgDataSchemaVersion, m.dataSchemaVersion)
	assertions.Equal(settings.Validator, m.validator)

	// Set after validation
	assertions.Nil(m.callback)
}

func TestNewMessageWithIDEmptySchemaVersion(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := ""

	_, err = newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, &FakeHedwigDataField{})
	assertions.NotNil(err)
}

func TestNewMessageWithIDInvalidSchemaVersion(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "a.b"

	_, err = newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, &FakeHedwigDataField{})
	assertions.NotNil(err)
}

func TestNewMessageWithIDNilMetadata(t *testing.T) {
	assertions := assert.New(t)

	settings := createTestSettings()

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"

	_, err := newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, nil, &FakeHedwigDataField{})
	assertions.NotNil(err)
}

func TestNewMessageWithIDNilData(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"

	_, err = newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, nil)
	assertions.NotNil(err)
}

func TestNewMessage(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{}

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, headers, &data)
	require.NoError(t, err)

	expectedSchema := fmt.Sprintf(
		"%s#/schemas/%s/%s", settings.Validator.SchemaRoot(), msgDataType, msgDataSchemaVersion)

	assertions.Equal(data, *m.Data.(*FakeHedwigDataField))
	assertions.Equal(FormatVersionV1, m.FormatVersion)
	assertions.True(len(m.ID) > 0 && len(m.ID) < 40)
	assertions.Equal(headers, m.Metadata.Headers)
	assertions.Equal(expectedSchema, m.Schema)
	assertions.Equal(msgDataSchemaVersion, m.dataSchemaVersion)
	assertions.Equal(msgDataType, m.dataType)
	assertions.Equal(settings.Validator, m.validator)
}

func TestMessageJSONString(t *testing.T) {
	assertions := assert.New(t)
	require := require.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{
		VehicleID: "C_123",
	}

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, headers, &data)
	require.NoError(err)

	jsonStr, err := m.JSONString()
	require.NoError(err)

	b := []byte(jsonStr)
	var jsonObj map[string]interface{}
	err = json.Unmarshal(b, &jsonObj)
	require.NoError(err)

	// Expecting data, format version, id, metadata, schema
	assertions.Equal(5, len(jsonObj))

	v, ok := jsonObj["format_version"]
	require.True(ok)
	vStr, ok := v.(string)
	require.True(ok)
	assertions.Equal(FormatCurrentVersion, vStr)

	v, ok = jsonObj["id"]
	require.True(ok)
	vStr, ok = v.(string)
	require.True(ok)
	assertions.Equal(vStr, m.ID)

	v, ok = jsonObj["schema"]
	require.True(ok)
	assertions.Equal(m.Schema, v)

	v, ok = jsonObj["metadata"]
	require.True(ok)
	metadata, ok := v.(map[string]interface{})
	require.True(ok)

	publisher, ok := metadata["publisher"]
	require.True(ok)
	publisherStr, ok := publisher.(string)
	require.True(ok)
	assertions.Equal(m.Metadata.Publisher, publisherStr)

	timestamp, ok := metadata["timestamp"]
	require.True(ok)
	expectedTimestamp := time.Time(m.Metadata.Timestamp).UnixNano() / int64(time.Millisecond)
	assertions.Equal(float64(expectedTimestamp), timestamp)

	v, ok = jsonObj["data"]
	require.True(ok)
	dataObj, ok := v.(map[string]interface{})
	require.True(ok)
	JSONStringvehicleID, ok := dataObj["vehicle_id"]
	require.True(ok)
	assertions.Equal(data.VehicleID, JSONStringvehicleID)
}

func TestMessageTopic(t *testing.T) {
	assertions := assert.New(t)

	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{
		VehicleID: "C_123",
	}

	expectedTopic := "dev-vehicle-created"
	settings := createTestSettings()
	settings.MessageRouting = map[MessageRouteKey]string{
		{
			MessageType:    msgDataType,
			MessageVersion: msgDataSchemaVersion,
		}: expectedTopic,
	}

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	require.NoError(t, err)

	topic, err := m.topic(settings)
	require.NoError(t, err)
	assertions.Equal(expectedTopic, topic)
}

func TestValidateSuccess(t *testing.T) {
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}

	settings := createTestSettings()

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	require.NoError(t, err)

	err = m.validate()
	require.NoError(t, err)
}

func TestValidateFailSchema(t *testing.T) {
	assertions := assert.New(t)

	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}

	settings := createTestSettings()

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	m.Schema = "foo"
	require.NoError(t, err)

	err = m.validate()
	assertions.NotNil(err)
}

func TestValidateFailSchemaVersion(t *testing.T) {
	assertions := assert.New(t)

	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}

	settings := createTestSettings()

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	m.Schema = "https://hedwig.automatic.com/schema#/schemas/trip_created/foo"
	require.NoError(t, err)

	err = m.validate()
	assertions.NotNil(err)
}

func TestValidateFailMessage(t *testing.T) {
	assertions := assert.New(t)

	settings := createTestSettings()
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := struct {
		Blah int
	}{Blah: 12}

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	require.NoError(t, err)

	err = m.validate()
	assertions.NotNil(err)
}

func TestValidateCallbackValid(t *testing.T) {
	assertions := assert.New(t)

	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}

	settings := createTestSettings()
	expectedCallbackFn := func(ctx context.Context, m *Message) error { return nil }

	RegisterCallback(msgDataType, msgDataSchemaVersion, expectedCallbackFn, func() interface{} { return new(FakeHedwigDataField) })

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	require.NoError(t, err)

	err = m.validateCallback(settings)
	assertions.Nil(err)

	assertions.NotNil(m.callback)
	// reset back to empty for other tests
	callbackRegistry = make(map[CallbackKey]*callBackInfo)
}

func TestValidateCallbackInvalid(t *testing.T) {
	assertions := assert.New(t)

	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}

	expectedCallback := func(ctx context.Context, m *Message) error { return nil }
	settings := createTestSettings()

	RegisterCallback(msgDataType, "2.0", expectedCallback, func() interface{} { return new(FakeHedwigDataField) })

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	require.NoError(t, err)

	err = m.validateCallback(settings)
	assertions.NotNil(err)
	assertions.Nil(m.callback)
	// reset back to empty for other tests
	callbackRegistry = make(map[CallbackKey]*callBackInfo)
}
