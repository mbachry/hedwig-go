/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConsumer_HandleLambdaEvent(t *testing.T) {
	ctx := context.Background()
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
	}
	snsEvent := events.SNSEvent{
		Records: []events.SNSEventRecord{
			{
				SNS: events.SNSEntity{
					MessageID: uuid.Must(uuid.NewV4()).String(),
					Message:   "message",
				},
			},
		},
	}
	awsClient := &FakeAWSClient{}
	awsClient.On("HandleLambdaEvent", ctx, snsEvent).Return(nil)
	consumer := lambdaConsumer{
		consumer: consumer{
			awsClient: awsClient,
			settings:  settings,
		},
	}
	err := consumer.HandleLambdaEvent(ctx, snsEvent)
	assert.NoError(t, err)
	awsClient.AssertExpectations(t)
}

func TestNewLambdaConsumer(t *testing.T) {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
	}

	sessionCache := &AWSSessionsCache{}

	iconsumer := NewLambdaConsumer(sessionCache, settings)
	assert.NotNil(t, iconsumer)
}

type fakeLambdaConsumer struct {
	mock.Mock
	ILambdaConsumer
}

func (lambdaConsumer *fakeLambdaConsumer) HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) error {
	args := lambdaConsumer.Called(ctx, snsEvent)
	return args.Error(0)
}

func TestLambdaHandler_Invoke(t *testing.T) {
	lambdaConsumer := &fakeLambdaConsumer{}
	handler := LambdaHandler{
		lambdaConsumer: lambdaConsumer,
	}
	ctx := context.Background()
	snsEvent := &events.SNSEvent{}
	payload, err := json.Marshal(snsEvent)
	require.NoError(t, err)

	lambdaConsumer.On("HandleLambdaEvent", ctx, *snsEvent).Return(nil)

	response, err := handler.Invoke(ctx, payload)
	assert.NoError(t, err)
	assert.Equal(t, []byte(""), response)

	lambdaConsumer.AssertExpectations(t)
}

func TestLambdaHandler_InvokeFailUnmarshal(t *testing.T) {
	lambdaConsumer := &fakeLambdaConsumer{}
	handler := LambdaHandler{
		lambdaConsumer: lambdaConsumer,
	}
	ctx := context.Background()

	response, err := handler.Invoke(ctx, []byte("bad payload"))
	assert.EqualError(t, err, "invalid character 'b' looking for beginning of value")
	assert.Nil(t, response)
}

func TestLambdaHandler_InvokeFailHandler(t *testing.T) {
	lambdaConsumer := &fakeLambdaConsumer{}
	handler := LambdaHandler{
		lambdaConsumer: lambdaConsumer,
	}
	ctx := context.Background()
	snsEvent := &events.SNSEvent{}
	payload, err := json.Marshal(snsEvent)
	require.NoError(t, err)

	lambdaConsumer.On("HandleLambdaEvent", ctx, *snsEvent).Return(errors.New("oops"))

	response, err := handler.Invoke(ctx, payload)
	assert.EqualError(t, err, "oops")
	assert.Nil(t, response)

	lambdaConsumer.AssertExpectations(t)
}

func TestNewLambdaHandler(t *testing.T) {
	lambdaConsumer := &fakeLambdaConsumer{}
	handler := NewLambdaHandler(lambdaConsumer)
	assert.NotNil(t, handler)
}
