/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"

	"github.com/sirupsen/logrus"
)

type queueConsumer struct {
	consumer
}

// ListenForMessages starts a hedwig listener for the provided message types
func (c *queueConsumer) ListenForMessages(ctx context.Context, request *ListenRequest) error {
	if request.NumMessages == 0 {
		request.NumMessages = 1
	}

	for i := uint32(0); request.LoopCount == 0 || i < request.LoopCount; i++ {
		if err := c.awsClient.FetchAndProcessMessages(ctx,
			c.settings, request.NumMessages, request.VisibilityTimeoutS); err != nil {
			logrus.WithError(err).Errorf("Failed to fetch and process message: %+v", err)
			return err
		}
	}
	return nil
}

// NewQueueConsumer creates a new consumer object used for a queue
func NewQueueConsumer(sessionCache *AWSSessionsCache, settings *Settings) IQueueConsumer {
	return &queueConsumer{
		consumer: consumer{
			awsClient: newAWSClient(sessionCache, settings),
			settings:  settings,
		},
	}
}
