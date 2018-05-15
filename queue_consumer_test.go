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

	"github.com/stretchr/testify/assert"
)

func TestConsumer_ListenForMessages(t *testing.T) {
	ctx := context.Background()
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
	}
	awsClient := &FakeAWSClient{}
	numMessages := uint32(10)
	visibilityTimeoutS := uint32(10)
	awsClient.On("FetchAndProcessMessages", ctx, settings, numMessages, visibilityTimeoutS).Return(nil)
	consumer := queueConsumer{
		consumer: consumer{
			awsClient: awsClient,
			settings:  settings,
		},
	}
	listenRequest := ListenRequest{
		NumMessages:        numMessages,
		VisibilityTimeoutS: visibilityTimeoutS,
		LoopCount:          5,
	}
	err := consumer.ListenForMessages(ctx, &listenRequest)
	assert.NoError(t, err)
	awsClient.AssertExpectations(t)
	assert.Equal(t, len(awsClient.Calls), int(listenRequest.LoopCount))
}

func TestNewQueueConsumer(t *testing.T) {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
	}

	sessionCache := &AWSSessionsCache{}

	iconsumer := NewQueueConsumer(sessionCache, settings)
	assert.NotNil(t, iconsumer)
}
