/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// CallbackKey is a key identifying a hedwig callback
type CallbackKey struct {
	// Message type
	MessageType string
	// Message major version
	MessageVersion string
}

// MessageRouteKey is a key identifying a message route
type MessageRouteKey struct {
	// Message type
	MessageType string
	// Message major version
	MessageVersion string
}

// LambdaRequest contains request objects for a lambda
type LambdaRequest struct {
	// Context for request
	Context context.Context
	// SNS record for this request
	EventRecord *events.SNSEventRecord
}

// SQSRequest contains request objects for a SQS handler
type SQSRequest struct {
	// Context for request
	Context context.Context
	// SQS message for this request
	QueueMessage *sqs.Message
}

// MessageDefaultHeadersHook is called to return default headers per message
type MessageDefaultHeadersHook func(ctx context.Context, message *Message) map[string]string

// PreProcessHookLambda is called on a sns event before any processing happens for a lambda.
// This hook may be used to perform initializations such as set up a global request id based on message headers.
type PreProcessHookLambda func(r *LambdaRequest) error

// PreProcessHookSQS is called on a message before any processing happens for a SQS queue.
// This hook may be used to perform initializations such as set up a global request id based on message headers.
type PreProcessHookSQS func(r *SQSRequest) error

// PreSerializeHook is called before a message is serialized to JSON.
// This hook may be used to modify the format over the wire.
type PreSerializeHook func(ctx context.Context, messageData *string) error

// PostDeserializeHook is called after a message has been deserialized from JSON, but
// before a Message is created and validated. This hook may be used to modify the format over the wire.
type PostDeserializeHook func(ctx context.Context, messageData *string) error

// NewData finds a function that returns a pointer to struct type that a hedwig message data should conform to
type NewData func() interface{}

// Settings for Hedwig
type Settings struct {
	// AWS Region
	AWSRegion string
	// AWS account id
	AWSAccountID string
	// AWS access key
	AWSAccessKey string
	// AWS secret key
	AWSSecretKey string
	// AWS session tokenthat represents temporary credentials (i.e. for Lambda app)
	AWSSessionToken string

	// Returns default headers for a message before a message is published. This will apply to ALL messages.
	// Can be used to inject custom headers (i.e. request id).
	MessageDefaultHeadersHook MessageDefaultHeadersHook

	// Maps message type and major version to topic names
	//   <message type>, <message version> => topic name
	// An entry is required for every message type that the app wants to consumer or publish. It is
	// recommended that major versions of a message be published on separate topics.
	MessageRouting map[MessageRouteKey]string

	// Hedwig pre process hook called before any processing is done on message
	PreProcessHookLambda PreProcessHookLambda // optional
	PreProcessHookSQS    PreProcessHookSQS    // optional

	// Hedwig hook called before a message is serialized to JSON
	PreSerializeHook PreSerializeHook // optional

	// Hedwig hook called after a message has been deserialized from
	// JSON, but before a Message is created and validated
	PostDeserializeHook PostDeserializeHook // optional

	// Publisher name
	Publisher string

	// Hedwig queue name. Exclude the `HEDWIG-` prefix
	QueueName string

	// Message validator using JSON schema for validation. Additional JSON schema formats may be added.
	// Please see github.com/santhosh-tekuri/jsonschema for more details.
	Validator IMessageValidator
}

// callBackInfo defines callback function and function to produce a new data pointer field for a hedwig message
type callBackInfo struct {
	CallbackFunction CallbackFunction
	NewData          NewData
}

// callbackRegistry maps hedwig messages to callBackInfo
var callbackRegistry = make(map[CallbackKey]*callBackInfo)

// RegisterCallback registers the given callback function to the given message type and message major version.
// Required for consumers. An error will be returned if an incoming message is missing a callback.
func RegisterCallback(msgType string, msgVersion string, cbf CallbackFunction, newData NewData) {
	cbk := CallbackKey{

		MessageType:    msgType,
		MessageVersion: msgVersion,
	}
	cbi := callBackInfo{
		CallbackFunction: cbf,
		NewData:          newData,
	}
	callbackRegistry[cbk] = &cbi
}
