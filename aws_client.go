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
	"fmt"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/sirupsen/logrus"
)

// iAmazonWebServicesClient represents an interface to the AWS client
type iAmazonWebServicesClient interface {
	FetchAndProcessMessages(ctx context.Context, settings *Settings, numMessages uint32, visibilityTimeoutS uint32) error
	HandleLambdaEvent(ctx context.Context, settings *Settings, snsEvent events.SNSEvent) error
	PublishSNS(ctx context.Context, settings *Settings, messageTopic string, payload string, headers map[string]string) error
}

// waitGroupError is like sync.WaitGroup but provides one extra field for storing error
type waitGroupError struct {
	sync.WaitGroup
	Error error
}

// DoneWithError may be used instead .Done() when there's an error
// This method clobbers the original error so you only see the last set error
func (w *waitGroupError) DoneWithError(err error) {
	w.Error = err
	w.Done()
}

func getSQSQueueName(settings *Settings) string {
	return fmt.Sprintf("HEDWIG-%s", settings.QueueName)
}

func getSNSTopic(settings *Settings, messageTopic string) string {
	return fmt.Sprintf(
		"arn:aws:sns:%s:%s:hedwig-%s",
		settings.AWSRegion,
		settings.AWSAccountID,
		messageTopic)
}

// awsClient wrapper struct
type awsClient struct {
	sns snsiface.SNSAPI
	sqs sqsiface.SQSAPI
}

func (a *awsClient) processSQSMessage(ctx context.Context, settings *Settings,
	queueMessage *sqs.Message, queueURL *string, queueName string, wg *sync.WaitGroup) {
	defer wg.Done()
	sqsRequest := &SQSRequest{
		Context:      ctx,
		QueueMessage: queueMessage,
	}
	if settings.PreProcessHookSQS != nil {
		if err := settings.PreProcessHookSQS(sqsRequest); err != nil {
			logrus.Errorf("Failed to execute pre process hook for message: %v", err)
			return
		}
	}

	err := a.messageHandlerSQS(sqsRequest, settings, queueMessage)
	switch err {
	case nil:
		_, err := a.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      queueURL,
			ReceiptHandle: queueMessage.ReceiptHandle,
		})
		if err != nil {
			logrus.Errorf("Failed to delete message with error: %v", err)
		}
	case ErrRetry:
		logrus.Debug("Retrying due to exception")
	default:
		logrus.Errorf("Retrying due to unknown exception: %v", err)
	}
}

func (a *awsClient) FetchAndProcessMessages(ctx context.Context,
	settings *Settings, numMessages uint32, visibilityTimeoutS uint32) error {

	queueName := getSQSQueueName(settings)
	queueURL, err := a.getSQSQueueURL(ctx, queueName)
	if err != nil {
		return err
	}

	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(numMessages)),
		QueueUrl:            queueURL,
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}
	if visibilityTimeoutS != 0 {
		input.VisibilityTimeout = aws.Int64(int64(visibilityTimeoutS))
	}

	wg := sync.WaitGroup{}
	out, err := a.sqs.ReceiveMessageWithContext(ctx, input)
	if err != nil {
		return err
	}
	for i := range out.Messages {
		select {
		case <-ctx.Done():
			// Do nothing
		default:
			wg.Add(1)
			go a.processSQSMessage(ctx, settings, out.Messages[i], queueURL, queueName, &wg)
		}
	}
	wg.Wait()
	// if context was canceled, signal appropriately
	return ctx.Err()
}

func (a *awsClient) processSNSRecord(ctx context.Context, settings *Settings, record *events.SNSEventRecord, wge *waitGroupError) {
	lambdaRequest := &LambdaRequest{
		Context:     ctx,
		EventRecord: record,
	}
	if settings.PreProcessHookLambda != nil {
		if err := settings.PreProcessHookLambda(lambdaRequest); err != nil {
			logrus.Errorf("failed to execute pre process hook for lambda event: %v", err)
			wge.DoneWithError(err)
			return
		}
	}

	err := a.messageHandlerLambda(lambdaRequest, settings, record)
	if err != nil {
		logrus.Errorf("failed to process lambda event with error: %v", err)
		wge.DoneWithError(err)
		return
	}
	wge.Done()
}

func (a *awsClient) HandleLambdaEvent(ctx context.Context, settings *Settings, snsEvent events.SNSEvent) error {
	wge := waitGroupError{}
	for i := range snsEvent.Records {
		select {
		case <-ctx.Done():
			// Do nothing
		default:
			wge.Add(1)
			go a.processSNSRecord(ctx, settings, &snsEvent.Records[i], &wge)
		}
	}

	wge.Wait()
	if ctx.Err() != nil {
		// if context was canceled, signal appropriately
		return ctx.Err()
	}
	return wge.Error
}

// PublishSNS handles publishing to AWS SNS
func (a *awsClient) PublishSNS(ctx context.Context, settings *Settings, messageTopic string, payload string,
	headers map[string]string) error {

	topic := getSNSTopic(settings, messageTopic)

	attributes := make(map[string]*sns.MessageAttributeValue)
	for key, value := range headers {
		attributes[key] = &sns.MessageAttributeValue{
			StringValue: aws.String(value),
			DataType:    aws.String("String"),
		}
	}

	_, err := a.sns.PublishWithContext(
		ctx,
		&sns.PublishInput{
			TopicArn:          &topic,
			Message:           &payload,
			MessageAttributes: attributes,
		})
	return err
}

func (a *awsClient) getSQSQueueURL(ctx context.Context, queueName string) (*string, error) {
	out, err := a.sqs.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		return nil, err
	}
	return out.QueueUrl, nil
}

func (a *awsClient) messageHandler(ctx context.Context, settings *Settings, messageBody string, receipt string) error {
	var jsonData []byte
	if settings.PostDeserializeHook != nil {
		if err := settings.PostDeserializeHook(ctx, &messageBody); err != nil {
			return err
		}
	}
	jsonData = []byte(messageBody)

	message := Message{}
	err := json.Unmarshal(jsonData, &message)
	if err != nil {
		logrus.Errorf("invalid message, unable to unmarshal")
		return err
	}

	// Set validator
	message.withValidator(settings.Validator)

	err = message.validate()
	if err != nil {
		return err
	}

	err = message.validateCallback(settings)
	if err != nil {
		return err
	}

	return message.execCallback(ctx, receipt)
}

func (a *awsClient) messageHandlerSQS(sqsRequest *SQSRequest, settings *Settings, message *sqs.Message) error {
	return a.messageHandler(sqsRequest.Context, settings, *message.Body, *message.ReceiptHandle)
}

func (a *awsClient) messageHandlerLambda(lambdaRequest *LambdaRequest, settings *Settings, record *events.SNSEventRecord) error {
	return a.messageHandler(lambdaRequest.Context, settings, record.SNS.Message, "")
}

func newAWSClient(sessionCache *AWSSessionsCache, settings *Settings) iAmazonWebServicesClient {
	awsSession := sessionCache.GetSession(settings)
	awsClient := awsClient{
		sns: sns.New(awsSession),
		sqs: sqs.New(awsSession),
	}
	return &awsClient
}
