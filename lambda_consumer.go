/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
)

type lambdaConsumer struct {
	consumer
}

// HandleLambdaInput processes hedwig messages for the provided message types for Lambda apps
func (c *lambdaConsumer) HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) error {
	return c.awsClient.HandleLambdaEvent(ctx, c.settings, snsEvent)
}

// NewLambdaConsumer creates a new consumer object used for lambda apps
func NewLambdaConsumer(sessionCache *AWSSessionsCache, settings *Settings) ILambdaConsumer {
	return &lambdaConsumer{
		consumer: consumer{
			awsClient: newAWSClient(sessionCache, settings),
			settings:  settings,
		},
	}
}
