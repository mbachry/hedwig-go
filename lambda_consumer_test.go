/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
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
