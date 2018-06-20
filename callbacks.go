/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"errors"
)

// CallbackKey is a key identifying a hedwig callback
type CallbackKey struct {
	// Message type
	MessageType string
	// Message major version
	MessageVersion string
}

// CallbackFunction is the function signature for a hedwig callback function
type CallbackFunction func(context.Context, *Message) error

// NewData is a function that returns a pointer to struct type that a hedwig message data should conform to
type NewData func() interface{}

// CallbackRegistry maps hedwig messages to callback functions and callback datas
type CallbackRegistry struct {
	datas     map[CallbackKey]NewData
	functions map[CallbackKey]CallbackFunction
}

// NewCallbackRegistry creates a callback registry
func NewCallbackRegistry() *CallbackRegistry {
	return &CallbackRegistry{
		datas:     map[CallbackKey]NewData{},
		functions: map[CallbackKey]CallbackFunction{},
	}
}

// RegisterCallback registers the given callback function to the given message type and message major version.
// Required for consumers. An error will be returned if an incoming message is missing a callback.
func (cr *CallbackRegistry) RegisterCallback(cbk CallbackKey, cbf CallbackFunction, newData NewData) {
	cr.functions[cbk] = cbf
	cr.datas[cbk] = newData
}

func (cr *CallbackRegistry) getCallbackFunction(cbk CallbackKey) (CallbackFunction, error) {
	fn, ok := cr.functions[cbk]
	if !ok {
		return nil, errors.New("Callback function is not defined for message")
	}
	return fn, nil
}

func (cr *CallbackRegistry) getMessageDataFactory(cbk CallbackKey) (NewData, error) {
	d, ok := cr.datas[cbk]
	if !ok {
		return nil, errors.New("Message data factory is not defined for message")
	}
	return d, nil
}
