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
	"github.com/pkg/errors"
)

// ListenRequest represents a request to listen for messages
type ListenRequest struct {
	NumMessages        uint32 // default 1
	VisibilityTimeoutS uint32 // defaults to queue configuration
	LoopCount          uint32 // defaults to infinite loops
}

// IQueueConsumer represents a hedwig queue consumer
type IQueueConsumer interface {
	// ListenForMessages starts a hedwig listener for the provided message types
	ListenForMessages(ctx context.Context, request *ListenRequest) error
}

// ILambdaConsumer represents a lambda event consumer
type ILambdaConsumer interface {
	// HandleLambdaInput processes hedwig messages for the provided message types for Lambda apps
	HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) error
}

const sqsWaitTimeoutSeconds int64 = 20

// ErrRetry should cause the task to retry, but not treat the retry as an error
var ErrRetry = errors.New("Retry error")

type consumer struct {
	awsClient iAmazonWebServicesClient
	settings  *Settings
}
