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

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
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

// LambdaHandler implements Lambda.Handler interface
type LambdaHandler struct {
	lambdaConsumer ILambdaConsumer
}

func (handler *LambdaHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	snsEvent := &events.SNSEvent{}
	err := json.Unmarshal(payload, snsEvent)
	if err != nil {
		return nil, err
	}

	err = handler.lambdaConsumer.HandleLambdaEvent(ctx, *snsEvent)
	if err != nil {
		return nil, err
	}
	return []byte(""), nil
}

// NewLambdaHandler returns a new lambda Handler that can be started like so:
//
//   func main() {
//       lambda.StartHandler(NewLambdaHandler(consumer))
//   }
//
// If you want to add additional error handle (e.g. panic catch etc), you can always use your own Handler,
// and call LambdaHandler.Invoke
func NewLambdaHandler(consumer ILambdaConsumer) lambda.Handler {
	return &LambdaHandler{
		lambdaConsumer: consumer,
	}
}
