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
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type FakeMessageDefaultHeadersHook struct {
	mock.Mock
}

func (fmdhh *FakeMessageDefaultHeadersHook) MessageDefaultHeadersHook(ctx context.Context, message *Message) map[string]string {
	args := fmdhh.Called(ctx, message)
	return args.Get(0).(map[string]string)
}

type FakePreSerializeHook struct {
	mock.Mock
}

func (fpsh *FakePreSerializeHook) PreSerializeHook(ctx *context.Context, messageData *string) error {
	args := fpsh.Called(ctx, messageData)
	return args.Error(0)
}

func TestPublishNoHooks(t *testing.T) {
	assertions := assert.New(t)

	ctx := context.Background()
	settings := createTestSettings()
	settings.MessageRouting = map[MessageRouteKey]string{
		{
			MessageType:         "vehicle_created",
			MessageMajorVersion: 1,
		}: "dev-vehicle-created",
	}
	awsClient := &FakeAWSClient{}

	publisher := &Publisher{
		awsClient: awsClient,
		settings:  settings,
	}

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(settings, "vehicle_created", "1.0", nil, &data)
	require.NoError(t, err)
	topic, err := message.topic(settings)
	require.NoError(t, err)
	messageBody, err := json.Marshal(message)
	require.NoError(t, err)
	messageBodyStr := string(messageBody)

	awsClient.On("PublishSNS", ctx, settings, topic, messageBodyStr, message.Metadata.Headers).Return(nil)

	err = publisher.Publish(ctx, message)
	assertions.Nil(err)

	awsClient.AssertExpectations(t)
}

func TestPublish(t *testing.T) {
	assertions := assert.New(t)

	fakeMessageDefaultHeadersHook := &FakeMessageDefaultHeadersHook{}
	fakePreSerializeHook := &FakePreSerializeHook{}

	ctx := context.Background()
	settings := createTestSettings()
	settings.MessageDefaultHeadersHook = fakeMessageDefaultHeadersHook.MessageDefaultHeadersHook
	settings.MessageRouting = map[MessageRouteKey]string{
		{
			MessageType:         "vehicle_created",
			MessageMajorVersion: 1,
		}: "dev-vehicle-created",
	}
	settings.PreSerializeHook = fakePreSerializeHook.PreSerializeHook
	awsClient := &FakeAWSClient{}

	publisher := &Publisher{
		awsClient: awsClient,
		settings:  settings,
	}

	headers := map[string]string{
		"key": "value",
	}
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(settings, "vehicle_created", "1.0", headers, &data)
	require.NoError(t, err)
	topic, err := message.topic(settings)
	require.NoError(t, err)

	defaultHeaders := map[string]string{
		"foo":  "bar",
		"boom": "blah",
	}
	fakeMessageDefaultHeadersHook.On("MessageDefaultHeadersHook", ctx, message).Return(defaultHeaders)

	message.Metadata.Headers = map[string]string{
		"key":  "value",
		"foo":  "bar",
		"boom": "blah",
	}
	msg, err := message.JSONString()
	require.NoError(t, err)
	fakePreSerializeHook.On("PreSerializeHook", &ctx, &msg).Return(nil)

	require.NoError(t, err)
	messageBody, err := json.Marshal(message)
	require.NoError(t, err)
	messageBodyStr := string(messageBody)

	awsClient.On("PublishSNS", ctx, settings, topic, messageBodyStr, message.Metadata.Headers).Return(nil)

	err = publisher.Publish(ctx, message)
	assertions.Nil(err)

	awsClient.AssertExpectations(t)
	fakeMessageDefaultHeadersHook.AssertExpectations(t)
	fakePreSerializeHook.AssertExpectations(t)
}

func TestPublishPreSerializeHookError(t *testing.T) {
	assertions := assert.New(t)

	fakePreSerializeHook := &FakePreSerializeHook{}

	ctx := context.Background()
	settings := createTestSettings()
	settings.MessageRouting = map[MessageRouteKey]string{
		{
			MessageType:         "vehicle_created",
			MessageMajorVersion: 1,
		}: "dev-vehicle-created",
	}
	settings.PreSerializeHook = fakePreSerializeHook.PreSerializeHook
	awsClient := &FakeAWSClient{}

	publisher := &Publisher{
		awsClient: awsClient,
		settings:  settings,
	}

	headers := map[string]string{
		"key": "value",
	}
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(settings, "vehicle_created", "1.0", headers, &data)
	require.NoError(t, err)

	message.Metadata.Headers = map[string]string{
		"key":  "value",
		"foo":  "bar",
		"boom": "blah",
	}
	msg, err := message.JSONString()
	require.NoError(t, err)

	fakePreSerializeHook.On("PreSerializeHook", &ctx, &msg).Return(errors.Errorf("Fake error!"))

	require.NoError(t, err)

	err = publisher.Publish(ctx, message)
	assertions.EqualError(errors.Cause(err), "Fake error!")

	awsClient.AssertExpectations(t)
	fakePreSerializeHook.AssertExpectations(t)
}

func TestPublishTopicError(t *testing.T) {
	assertions := assert.New(t)

	fakePreSerializeHook := &FakePreSerializeHook{}

	ctx := context.Background()
	settings := createTestSettings()
	settings.MessageRouting = map[MessageRouteKey]string{}
	settings.PreSerializeHook = fakePreSerializeHook.PreSerializeHook
	awsClient := &FakeAWSClient{}

	publisher := &Publisher{
		awsClient: awsClient,
		settings:  settings,
	}

	headers := map[string]string{
		"key": "value",
	}
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(settings, "vehicle_created", "1.0", headers, &data)
	require.NoError(t, err)

	message.Metadata.Headers = map[string]string{
		"key":  "value",
		"foo":  "bar",
		"boom": "blah",
	}
	msg, err := message.JSONString()
	require.NoError(t, err)

	fakePreSerializeHook.On("PreSerializeHook", &ctx, &msg).Return(nil)

	require.NoError(t, err)

	err = publisher.Publish(ctx, message)
	assertions.EqualError(errors.Cause(err), "Message route is not defined for message")

	awsClient.AssertExpectations(t)
	fakePreSerializeHook.AssertExpectations(t)
}

func TestNewPublisher(t *testing.T) {
	settings := createTestSettings()
	sessionCache := &AWSSessionsCache{}

	publisher := NewPublisher(sessionCache, settings)
	assert.NotNil(t, publisher)
}
