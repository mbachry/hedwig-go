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
	uuid "github.com/satori/go.uuid"
)

var (
	schemaRe = regexp.MustCompile(`([^/]+)\/([^/]+)$`)
)

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

	callbackRegistry  *CallbackRegistry
	dataSchemaVersion *semver.Version
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
		MessageType:         m.dataType,
		MessageMajorVersion: int(m.dataSchemaVersion.Major()),
	}

	topic, ok := settings.MessageRouting[key]
	if !ok {
		return "", errors.New("Message route is not defined for message")
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
	err := m.setDataSchema()
	if err != nil {
		return err
	}

	if m.callbackRegistry == nil {
		return errors.New("callbackRegistry must be set")
	}

	dataFactory, err := m.callbackRegistry.getMessageDataFactory(CallbackKey{
		MessageType:         m.dataType,
		MessageMajorVersion: int(m.dataSchemaVersion.Major()),
	})
	if err != nil {
		return err
	}

	data := dataFactory()
	if err := json.Unmarshal(dataContainer.Data, data); err != nil {
		return err
	}
	m.Data = data

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
	// Validate schema version matches semvar versioning
	dataSchemaVersion := groupMatches[2]
	validatedVersion, err := semver.NewVersion(dataSchemaVersion)
	if err != nil {
		return err
	}

	m.dataSchemaVersion = validatedVersion
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
	callBackFn, err := m.callbackRegistry.getCallbackFunction(CallbackKey{
		MessageType:         m.dataType,
		MessageMajorVersion: int(m.dataSchemaVersion.Major()),
	})
	if err != nil {
		return err
	}

	m.callback = callBackFn
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
	if data == nil {
		return nil, errors.New("expected non-nil data")
	}
	if metadata == nil {
		return nil, errors.New("expected non-nil metadata")
	}
	if settings.CallbackRegistry == nil {
		return nil, errors.New("expected callback registry to be set")
	}

	m := &Message{
		Data:          data,
		FormatVersion: FormatVersionV1,
		ID:            id,
		Metadata:      metadata,
		Schema:        fmt.Sprintf("%s#/schemas/%s/%s", settings.Validator.SchemaRoot(), dataType, dataSchemaVersion),

		callbackRegistry: settings.CallbackRegistry,
		dataType:         dataType,
		validator:        settings.Validator,
	}
	err := m.setDataSchema()
	if err != nil {
		return nil, err
	}
	return m, nil
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
