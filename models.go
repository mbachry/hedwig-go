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
	"regexp"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

var (
	schemaRe = regexp.MustCompile(`([^/]+)\/([^/]+)$`)
)

// CallbackFunction is the function signiture for a hedwig callback function
type CallbackFunction func(context.Context, *Message) error

type metadata struct {
	Headers   map[string]string `json:"headers,omitempty"`
	Publisher string            `json:"publisher"`
	Receipt   string            `json:"-"`
	Timestamp JSONTime          `json:"timestamp"`
}

// Message model for hedwig messages.
type Message struct {
	Data          interface{} `json:"data"`
	FormatVersion string      `json:"format_version"`
	ID            string      `json:"id"`
	Metadata      *metadata   `json:"metadata"`
	Schema        string      `json:"schema"`

	dataSchemaVersion string
	dataType          string
	validator         IMessageValidator

	// Set after validation
	callback CallbackFunction
}

func createMetadata(settings *Settings, headers map[string]string) (*metadata, error) {
	return &metadata{
		Headers:   headers,
		Publisher: settings.Publisher,
		Timestamp: JSONTime(time.Now()),
	}, nil
}

// DataJSONString returns a string representation of Message
func (m *Message) DataJSONString() (string, error) {
	o, err := json.Marshal(m.Data)
	if err != nil {
		return "", errors.Wrapf(err, "unable to serialize message data")
	}
	return string(o), nil
}

// JSONString returns a string representation of Message
func (m *Message) JSONString() (string, error) {
	o, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(o), nil
}

// execCallback executes the callback associated with message
func (m *Message) execCallback(ctx context.Context, receipt string) error {
	m.Metadata.Receipt = receipt
	return m.callback(ctx, m)
}

// topic returns SNS message topic
func (m *Message) topic(settings *Settings) (string, error) {
	key := MessageRouteKey{
		MessageType:    m.dataType,
		MessageVersion: m.dataSchemaVersion,
	}

	topic, ok := settings.MessageRouting[key]
	if !ok {
		return "", errors.New("Callback is not defined for message")
	}

	return topic, nil
}

// UnmarshalJSON serializes the message from json
func (m *Message) UnmarshalJSON(b []byte) error {
	type MessageClone Message
	if err := json.Unmarshal(b, (*MessageClone)(m)); err != nil {
		return err
	}

	dataContainer := struct {
		// delay de-serializing data until we know data type
		Data json.RawMessage `json:"data"`
	}{}
	if err := json.Unmarshal(b, &dataContainer); err != nil {
		return err
	}
	m.setDataSchema()

	callbackKey := CallbackKey{
		MessageType:    m.dataType,
		MessageVersion: m.dataSchemaVersion,
	}
	if callBackInfo, ok := callbackRegistry[callbackKey]; ok {
		data := callBackInfo.NewData()
		if err := json.Unmarshal(dataContainer.Data, data); err != nil {
			return err
		}
		m.Data = data
	}

	return nil
}

func (m *Message) setDataSchema() error {
	groupMatches := schemaRe.FindStringSubmatch(m.Schema)
	if groupMatches == nil {
		return errors.Errorf("Invalid schema: %s", m.Schema)
	}
	if len(groupMatches) != 3 {
		return errors.Errorf("Invalid schema groups: %s", m.Schema)
	}
	m.dataType = groupMatches[1]
	// Validate schema version to be in <major>.<minor>
	dataSchemaVersion := groupMatches[2]
	_, err := semver.NewVersion(dataSchemaVersion)
	if err != nil {
		return err
	}
	m.dataSchemaVersion = dataSchemaVersion
	return nil
}

// validate is a convenience wrapper to validate the current message
func (m *Message) validate() error {
	// Validate schema
	m.setDataSchema()

	return m.validator.Validate(m)
}

// validateCallback is a convenience wrapper to validate and set callback
func (m *Message) validateCallback(settings *Settings) error {
	key := CallbackKey{
		MessageType:    m.dataType,
		MessageVersion: m.dataSchemaVersion,
	}

	callBackInfo, ok := callbackRegistry[key]
	if !ok {
		return errors.New("Callback is not defined for message")
	}

	m.callback = callBackInfo.CallbackFunction
	return nil
}

func (m *Message) withValidator(validator IMessageValidator) *Message {
	m.validator = validator
	return m
}

// newMessageWithID creates new Hedwig messages
func newMessageWithID(
	settings *Settings, id string, dataType string, dataSchemaVersion string,
	metadata *metadata, data interface{}) (*Message, error) {
	// Validate schema version to be in <major>.<minor>
	_, err := semver.NewVersion(dataSchemaVersion)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, errors.New("Expected non-nil data")
	}
	if metadata == nil {
		return nil, errors.New("Expected non-nil metadata")
	}

	return &Message{
		Data:          data,
		FormatVersion: FormatVersionV1,
		ID:            id,
		Metadata:      metadata,
		Schema:        fmt.Sprintf("%s#/schemas/%s/%s", settings.Validator.SchemaRoot(), dataType, dataSchemaVersion),

		dataSchemaVersion: dataSchemaVersion,
		dataType:          dataType,
		validator:         settings.Validator,
	}, nil
}

// NewMessage creates new Hedwig messages based off of message type and schema version
func NewMessage(settings *Settings, dataType string, dataSchemaVersion string, headers map[string]string, data interface{}) (*Message, error) {
	// Generate uuid for id
	msgUUID, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	msgID := msgUUID.String()

	metadata, err := createMetadata(settings, headers)
	if err != nil {
		return nil, err
	}

	return newMessageWithID(settings, msgID, dataType, dataSchemaVersion, metadata, data)
}
