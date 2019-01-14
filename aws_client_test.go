/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

// FakeHedwigDataField is a fake data field for testing
type FakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

type FakeSQS struct {
	mock.Mock
	// fake interface here
	sqsiface.SQSAPI
}

func (fs *FakeSQS) SendMessageWithContext(ctx aws.Context, in *sqs.SendMessageInput, opts ...request.Option) (*sqs.SendMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.SendMessageOutput), args.Error(1)
}

func (fs *FakeSQS) GetQueueUrlWithContext(ctx aws.Context, in *sqs.GetQueueUrlInput, opts ...request.Option) (*sqs.GetQueueUrlOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.GetQueueUrlOutput), args.Error(1)
}

func (fs *FakeSQS) ReceiveMessageWithContext(ctx aws.Context, in *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func (fs *FakeSQS) DeleteMessageWithContext(ctx aws.Context, in *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.DeleteMessageOutput), args.Error(1)
}

type FakeSns struct {
	mock.Mock
	// fake interface here
	snsiface.SNSAPI
}

func (fs *FakeSns) PublishWithContext(ctx aws.Context, in *sns.PublishInput, opts ...request.Option) (*sns.PublishOutput, error) {
	args := fs.Called(ctx, in)
	return args.Get(0).(*sns.PublishOutput), args.Error(1)
}

type FakeAWSClient struct {
	mock.Mock
}

func (fa *FakeAWSClient) FetchAndProcessMessages(ctx context.Context, settings *Settings, numMessages uint32,
	visibilityTimeoutS uint32) error {

	args := fa.Called(ctx, settings, numMessages, visibilityTimeoutS)
	return args.Error(0)
}

func (fa *FakeAWSClient) HandleLambdaEvent(ctx context.Context, settings *Settings, snsEvent events.SNSEvent) error {
	args := fa.Called(ctx, snsEvent)
	return args.Error(0)
}

func (fa *FakeAWSClient) PublishSNS(ctx context.Context, settings *Settings, messageTopic string, payload string,
	headers map[string]string) error {

	args := fa.Called(ctx, settings, messageTopic, payload, headers)
	return args.Error(0)
}

type FakeCallback struct {
	mock.Mock
}

func (fc *FakeCallback) Callback(ctx context.Context, m *Message) error {
	args := fc.Called(ctx, m)
	return args.Error(0)
}

type FakePreProcessHookSQS struct {
	mock.Mock
}

func (fpph *FakePreProcessHookSQS) PreProcessHookSQS(r *SQSRequest) error {
	args := fpph.Called(r)
	return args.Error(0)
}

type FakePreProcessHookLambda struct {
	mock.Mock
}

func (fpph *FakePreProcessHookLambda) PreProcessHookLambda(r *LambdaRequest) error {
	args := fpph.Called(r)
	return args.Error(0)
}

type FakePreDeserializeHook struct {
	mock.Mock
}

func (fpdh *FakePreDeserializeHook) PreDeserializeHook(ctx *context.Context, messageData *string) error {
	args := fpdh.Called(ctx, messageData)
	return args.Error(0)
}

type AWSClientTestSuite struct {
	suite.Suite
	fakeCallback *FakeCallback
	settings     *Settings
}

func (suite *AWSClientTestSuite) SetupTest() {
	suite.fakeCallback = new(FakeCallback)
	suite.settings = createTestSettings()
	cbk := CallbackKey{
		MessageType:         "vehicle_created",
		MessageMajorVersion: 1,
	}
	suite.settings.CallbackRegistry.RegisterCallback(
		cbk, suite.fakeCallback.Callback, func() interface{} { return new(FakeHedwigDataField) })

}

func (suite *AWSClientTestSuite) TearDownTest() {
}

func (suite *AWSClientTestSuite) TestGetSqsQueueName() {
	settings := &Settings{
		QueueName: "DEV-MYAPP",
	}

	expectedQueue := "HEDWIG-DEV-MYAPP"
	queue := getSQSQueueName(settings)
	suite.Equal(expectedQueue, queue)
}

func (suite *AWSClientTestSuite) TestGetSnsTopic() {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
	}
	msgTopic := "dev-myapp"

	expectedTopic := "arn:aws:sns:us-east-1:1234567890:hedwig-dev-myapp"
	topic := getSNSTopic(settings, msgTopic)
	suite.Equal(expectedTopic, topic)
}

func (suite *AWSClientTestSuite) TestAWSClient_FetchAndProcessMessages() {
	ctx := context.Background()
	fakeCallback := suite.fakeCallback
	fakePreProcessHookSQS := &FakePreProcessHookSQS{}
	fakeSqs := &FakeSQS{}
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	expectedReceiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}

	suite.settings.PreProcessHookSQS = fakePreProcessHookSQS.PreProcessHookSQS

	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	fakeSqs.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	expectedMessages := make([]*Message, 2)
	outMessages := make([]*sqs.Message, 2)
	for i := 0; i < 2; i++ {
		data := FakeHedwigDataField{
			VehicleID: fmt.Sprintf("C_123456789012345%d", i),
		}
		message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
		suite.Require().NoError(err)

		// Override time so comparison does not fail due to precision
		message.Metadata.Timestamp = JSONTime(
			time.Unix(0, int64(i+1)*int64(time.Hour)))
		message.validate()
		message.validateCallback(suite.settings)

		// Have to use Anything cause comparison fails for function pointers
		fakeCallback.On("Callback", ctx, mock.Anything).Return(nil)

		msgJSON, err := message.JSONString()
		suite.Require().NoError(err)

		outMessages[i] = &sqs.Message{
			Body:          aws.String(msgJSON),
			ReceiptHandle: aws.String(uuid.Must(uuid.NewV4()).String()),
		}
		fakePreProcessHookSQS.On("PreProcessHookSQS", &SQSRequest{
			Context:      ctx,
			QueueMessage: outMessages[i],
		}).Return(nil)
		message.Metadata.Receipt = *outMessages[i].ReceiptHandle
		expectedMessages[i] = message

		expectedDeleteMessageInput := &sqs.DeleteMessageInput{
			QueueUrl:      &queueURL,
			ReceiptHandle: outMessages[i].ReceiptHandle,
		}
		fakeSqs.On("DeleteMessageWithContext", ctx, expectedDeleteMessageInput, mock.Anything).Return(&sqs.DeleteMessageOutput{}, nil)
	}

	receiveMessageOutput := &sqs.ReceiveMessageOutput{
		Messages: outMessages,
	}
	fakeSqs.On("ReceiveMessageWithContext", ctx, expectedReceiveMessageInput, mock.Anything).Return(receiveMessageOutput, nil)

	awsClient := &awsClient{
		sqs: fakeSqs,
	}
	err := awsClient.FetchAndProcessMessages(
		ctx, suite.settings, 10, 10,
	)
	suite.NoError(err)
	fakeCallback.AssertExpectations(suite.T())
	fakePreProcessHookSQS.AssertExpectations(suite.T())
	fakeSqs.AssertExpectations(suite.T())

	// Validate callback argument
	suite.Require().Equal(len(fakeCallback.Calls), len(expectedMessages))
	for _, expectedMsg := range expectedMessages {
		msgMatch := false
		for i := range fakeCallback.Calls {
			msg := fakeCallback.Calls[i].Arguments.Get(1).(*Message)

			if expectedMsg.ID != msg.ID {
				continue
			}

			// Spot check all fields except Callback. Go does not support function pointer comparison
			suite.Require().NoError(err)
			suite.Equal(*expectedMsg.Data.(*FakeHedwigDataField), *msg.Data.(*FakeHedwigDataField))
			suite.Equal(expectedMsg.Metadata, msg.Metadata)
			suite.Equal(expectedMsg.ID, msg.ID)
			suite.Equal(expectedMsg.Schema, msg.Schema)
			suite.Equal(expectedMsg.FormatVersion, msg.FormatVersion)

			// Validate callback is set
			suite.NotNil(msg.callback)

			msgMatch = true
		}

		// Ensure message matched at least one callback
		suite.True(msgMatch)
	}
}

func (suite *AWSClientTestSuite) TestAWSClient_FetchAndProcessMessagesHookError() {
	ctx := context.Background()
	fakeCallback := suite.fakeCallback
	fakePreProcessHookSQS := &FakePreProcessHookSQS{}
	fakeSqs := &FakeSQS{}
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	expectedReceiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}

	suite.settings.PreProcessHookSQS = fakePreProcessHookSQS.PreProcessHookSQS

	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	fakeSqs.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	outMessages := make([]*sqs.Message, 2)
	for i := 0; i < 2; i++ {
		data := FakeHedwigDataField{
			VehicleID: fmt.Sprintf("C_123456789012345%d", i),
		}
		message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
		suite.Require().NoError(err)

		msgJSON, err := message.JSONString()
		suite.Require().NoError(err)

		outMessages[i] = &sqs.Message{
			Body:          aws.String(msgJSON),
			ReceiptHandle: aws.String(uuid.Must(uuid.NewV4()).String()),
		}
		fakePreProcessHookSQS.On("PreProcessHookSQS", &SQSRequest{
			Context:      ctx,
			QueueMessage: outMessages[i],
		}).Return(errors.New("fail"))
	}

	receiveMessageOutput := &sqs.ReceiveMessageOutput{
		Messages: outMessages,
	}
	fakeSqs.On("ReceiveMessageWithContext", ctx, expectedReceiveMessageInput, mock.Anything).Return(receiveMessageOutput, nil)

	awsClient := &awsClient{
		sqs: fakeSqs,
	}
	err := awsClient.FetchAndProcessMessages(
		ctx, suite.settings, 10, 10,
	)
	suite.NoError(err)
	fakeCallback.AssertExpectations(suite.T())
	fakePreProcessHookSQS.AssertExpectations(suite.T())
	fakeSqs.AssertExpectations(suite.T())
}

func (suite *AWSClientTestSuite) TestAWSClient_FetchAndProcessMessagesNoHook() {
	ctx := context.Background()
	fakeCallback := suite.fakeCallback
	fakeSqs := &FakeSQS{}
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	expectedReceiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}

	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	fakeSqs.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	expectedMessages := make([]*Message, 2)
	outMessages := make([]*sqs.Message, 2)
	for i := 0; i < 2; i++ {
		data := FakeHedwigDataField{
			VehicleID: fmt.Sprintf("C_123456789012345%d", i),
		}
		message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
		suite.Require().NoError(err)

		// Have to use Anything cause comparison fails for function pointers
		fakeCallback.On("Callback", ctx, mock.Anything).Return(nil)

		msgJSON, err := message.JSONString()
		suite.Require().NoError(err)

		outMessages[i] = &sqs.Message{
			Body:          aws.String(msgJSON),
			ReceiptHandle: aws.String(uuid.Must(uuid.NewV4()).String()),
		}
		message.Metadata.Receipt = *outMessages[i].ReceiptHandle
		expectedMessages[i] = message

		expectedDeleteMessageInput := &sqs.DeleteMessageInput{
			QueueUrl:      &queueURL,
			ReceiptHandle: outMessages[i].ReceiptHandle,
		}
		fakeSqs.On("DeleteMessageWithContext", ctx, expectedDeleteMessageInput, mock.Anything).Return(&sqs.DeleteMessageOutput{}, nil)
	}

	receiveMessageOutput := &sqs.ReceiveMessageOutput{
		Messages: outMessages,
	}
	fakeSqs.On("ReceiveMessageWithContext", ctx, expectedReceiveMessageInput, mock.Anything).Return(receiveMessageOutput, nil)

	awsClient := &awsClient{
		sqs: fakeSqs,
	}
	err := awsClient.FetchAndProcessMessages(
		ctx, suite.settings, 10, 10,
	)
	suite.NoError(err)

	fakeCallback.AssertExpectations(suite.T())
	fakeSqs.AssertExpectations(suite.T())
}

func (suite *AWSClientTestSuite) TestAWSClient_FetchAndProcessMessagesNoDeleteOnError() {
	ctx := context.Background()
	hook := test.NewGlobal()
	logrus.StandardLogger().Out = ioutil.Discard
	defer hook.Reset()

	fakeCallback := suite.fakeCallback
	fakeSqs := &FakeSQS{}
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName

	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	fakeSqs.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	expectedReceiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
	suite.Require().NoError(err)

	// Override time so comparison does not fail due to precision
	message.Metadata.Timestamp = JSONTime(
		time.Unix(0, int64(1)*int64(time.Hour)))
	message.validate()
	message.validateCallback(suite.settings)

	// Have to use Anything cause comparison fails for function pointers
	fakeCallback.On("Callback", ctx, mock.Anything).Return(errors.New("my bad"))

	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	receiveMessageOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			{
				Body:          aws.String(msgJSON),
				ReceiptHandle: aws.String(uuid.Must(uuid.NewV4()).String()),
			},
		},
	}
	message.Metadata.Receipt = *receiveMessageOutput.Messages[0].ReceiptHandle
	fakeSqs.On("ReceiveMessageWithContext", ctx, expectedReceiveMessageInput, mock.Anything).Return(receiveMessageOutput, nil)

	awsClient := &awsClient{
		sqs: fakeSqs,
	}
	err = awsClient.FetchAndProcessMessages(ctx, suite.settings, 10, 10)
	// no error is returned here, but we log the error
	suite.NoError(err)

	suite.Equal(1, len(hook.Entries))
	suite.Equal(logrus.ErrorLevel, hook.LastEntry().Level)
	suite.Equal("Retrying due to unknown exception: my bad", hook.LastEntry().Message)

	fakeCallback.AssertExpectations(suite.T())
	fakeSqs.AssertExpectations(suite.T())

	// Validate callback argument
	suite.Require().Equal(len(fakeCallback.Calls), 1)

	expectedMsg := message
	msg := fakeCallback.Calls[0].Arguments.Get(1).(*Message)

	// Spot check all fields except Callback. Go does not support function pointer comparison
	suite.Require().NoError(err)
	suite.Equal(*expectedMsg.Data.(*FakeHedwigDataField), *msg.Data.(*FakeHedwigDataField))
	suite.Equal(expectedMsg.Metadata, msg.Metadata)
	suite.Equal(expectedMsg.ID, msg.ID)
	suite.Equal(expectedMsg.Schema, msg.Schema)
	suite.Equal(expectedMsg.FormatVersion, msg.FormatVersion)

	// Validate callback is set
	suite.NotNil(msg.callback)
}

func (suite *AWSClientTestSuite) TestAWSClient_HandleLambdaEvent() {
	ctx := context.Background()
	awsClient := &awsClient{}

	fakeCallback := suite.fakeCallback
	fakePreProcessHookLambda := &FakePreProcessHookLambda{}

	suite.settings.PreProcessHookLambda = fakePreProcessHookLambda.PreProcessHookLambda

	_, childCtx := errgroup.WithContext(ctx)
	snsRecords := make([]events.SNSEventRecord, 2)
	expectedMessages := make([]*Message, 2)
	for i := 0; i < 2; i++ {
		data := FakeHedwigDataField{
			VehicleID: fmt.Sprintf("C_123456789012345%d", i),
		}
		message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
		suite.Require().NoError(err)

		// Override time so comparison does not fail due to precision
		message.Metadata.Timestamp = JSONTime(
			time.Unix(0, int64(i+1)*int64(time.Hour)))
		message.validate()
		message.validateCallback(suite.settings)
		expectedMessages[i] = message

		// Have to use Anything cause comparison fails for function pointers
		fakeCallback.On("Callback", mock.Anything, mock.Anything).Return(nil)

		msgJSON, err := message.JSONString()
		suite.Require().NoError(err)

		snsRecords[i] = events.SNSEventRecord{
			SNS: events.SNSEntity{
				MessageID: uuid.Must(uuid.NewV4()).String(),
				Message:   msgJSON,
			},
		}
		fakePreProcessHookLambda.On("PreProcessHookLambda", &LambdaRequest{
			Context:     childCtx,
			EventRecord: &snsRecords[i],
		}).Return(nil)
	}
	snsEvent := events.SNSEvent{
		Records: snsRecords,
	}

	err := awsClient.HandleLambdaEvent(ctx, suite.settings, snsEvent)
	suite.NoError(err)

	fakePreProcessHookLambda.AssertExpectations(suite.T())

	// Validate callback argument
	suite.Equal(len(fakeCallback.Calls), len(expectedMessages))
	fakeCallback.AssertExpectations(suite.T())
	for _, expectedMsg := range expectedMessages {
		msgMatch := false
		for i := range fakeCallback.Calls {
			msg := fakeCallback.Calls[i].Arguments.Get(1).(*Message)

			if expectedMsg.ID != msg.ID {
				continue
			}

			// Spot check all fields except Callback. Go does not support function pointer comparison
			suite.Require().NoError(err)
			suite.Equal(*expectedMsg.Data.(*FakeHedwigDataField), *msg.Data.(*FakeHedwigDataField))
			suite.Equal(expectedMsg.Metadata, msg.Metadata)
			suite.Equal(expectedMsg.ID, msg.ID)
			suite.Equal(expectedMsg.Schema, msg.Schema)
			suite.Equal(expectedMsg.FormatVersion, msg.FormatVersion)

			// Validate callback is set
			suite.NotNil(msg.callback)

			msgMatch = true
		}

		// Ensure message matched at least one callback
		suite.True(msgMatch)
	}
}

func (suite *AWSClientTestSuite) TestAWSClient_HandleLambdaEventHookError() {
	ctx := context.Background()
	awsClient := &awsClient{}

	fakeCallback := suite.fakeCallback
	fakePreProcessHookLambda := &FakePreProcessHookLambda{}

	suite.settings.PreProcessHookLambda = fakePreProcessHookLambda.PreProcessHookLambda

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123450",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
	suite.Require().NoError(err)

	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	snsRecord := events.SNSEventRecord{
		SNS: events.SNSEntity{
			MessageID: uuid.Must(uuid.NewV4()).String(),
			Message:   msgJSON,
		},
	}
	_, childCtx := errgroup.WithContext(ctx)
	fakePreProcessHookLambda.On("PreProcessHookLambda", &LambdaRequest{
		Context:     childCtx,
		EventRecord: &snsRecord,
	}).Return(errors.New("fail"))

	snsEvent := events.SNSEvent{
		Records: []events.SNSEventRecord{
			snsRecord,
		},
	}

	err = awsClient.HandleLambdaEvent(ctx, suite.settings, snsEvent)
	suite.EqualError(errors.Cause(err), "fail")

	fakeCallback.AssertExpectations(suite.T())
	fakePreProcessHookLambda.AssertExpectations(suite.T())
}

func (suite *AWSClientTestSuite) TestAWSClient_HandleLambdaEventContextCancel() {
	ctx, cancel := context.WithCancel(context.Background())
	awsClient := &awsClient{}

	fakeCallback := suite.fakeCallback

	snsRecords := make([]events.SNSEventRecord, 2)
	for i := 0; i < 2; i++ {
		data := FakeHedwigDataField{
			VehicleID: fmt.Sprintf("C_123456789012345%d", i),
		}
		message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
		suite.Require().NoError(err)

		// Have to use Anything cause comparison fails for function pointers
		fakeCallback.On("Callback", mock.Anything, mock.Anything).Return(
			nil).Run(func(args mock.Arguments) {
			time.Sleep(100 * time.Millisecond)
		})

		msgJSON, err := message.JSONString()
		suite.Require().NoError(err)

		snsRecords[i] = events.SNSEventRecord{
			SNS: events.SNSEntity{
				MessageID: uuid.Must(uuid.NewV4()).String(),
				Message:   msgJSON,
			},
		}
	}
	snsEvent := events.SNSEvent{
		Records: snsRecords,
	}

	ch := make(chan bool)
	go func() {
		err := awsClient.HandleLambdaEvent(ctx, suite.settings, snsEvent)
		suite.Assert().EqualError(err, "context canceled")
		ch <- true
		close(ch)
	}()
	time.Sleep(2 * time.Millisecond)
	cancel()
	// wait for co-routine to finish
	<-ch
	fakeCallback.AssertExpectations(suite.T())
}
func (suite *AWSClientTestSuite) TestAWSClient_HandleLambdaEventNoHook() {
	ctx := context.Background()
	awsClient := &awsClient{}
	fakeCallback := suite.fakeCallback

	snsRecords := make([]events.SNSEventRecord, 2)
	for i := 0; i < 2; i++ {
		data := FakeHedwigDataField{
			VehicleID: fmt.Sprintf("C_123456789012345%d", i),
		}
		message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
		suite.Require().NoError(err)

		// Have to use Anything cause comparison fails for function pointers
		fakeCallback.On("Callback", mock.Anything, mock.Anything).Return(nil)

		msgJSON, err := message.JSONString()
		suite.Require().NoError(err)

		snsRecords[i] = events.SNSEventRecord{
			SNS: events.SNSEntity{
				MessageID: uuid.Must(uuid.NewV4()).String(),
				Message:   msgJSON,
			},
		}
	}
	snsEvent := events.SNSEvent{
		Records: snsRecords,
	}

	err := awsClient.HandleLambdaEvent(ctx, suite.settings, snsEvent)
	suite.NoError(err)

	fakeCallback.AssertExpectations(suite.T())

}

func (suite *AWSClientTestSuite) TestAWSClient_HandleLambdaEventCallbackError() {
	ctx := context.Background()
	hook := test.NewGlobal()
	logrus.StandardLogger().Out = ioutil.Discard
	defer hook.Reset()

	awsClient := &awsClient{}

	fakeCallback := suite.fakeCallback

	snsRecords := make([]events.SNSEventRecord, 2)
	expectedMessages := make([]*Message, 2)
	for i := 0; i < 2; i++ {
		data := FakeHedwigDataField{
			VehicleID: fmt.Sprintf("C_123456789012345%d", i),
		}

		message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
		suite.Require().NoError(err)

		// Override time so comparison does not fail due to precision
		message.Metadata.Timestamp = JSONTime(
			time.Unix(0, int64(1)*int64(time.Hour)))
		message.validate()
		message.validateCallback(suite.settings)
		expectedMessages[i] = message

		// Have to use Anything cause comparison fails for function pointers
		fakeCallback.On("Callback", mock.Anything, mock.Anything).Return(errors.New("my bad"))

		msgJSON, err := message.JSONString()
		suite.Require().NoError(err)

		snsRecords[i] = events.SNSEventRecord{
			SNS: events.SNSEntity{
				MessageID: uuid.Must(uuid.NewV4()).String(),
				Message:   msgJSON,
			},
		}
	}
	snsEvent := events.SNSEvent{
		Records: snsRecords,
	}

	err := awsClient.HandleLambdaEvent(ctx, suite.settings, snsEvent)
	suite.EqualError(err, "my bad")

	fakeCallback.AssertExpectations(suite.T())

	suite.Equal(2, len(hook.Entries))
	suite.Equal(logrus.ErrorLevel, hook.LastEntry().Level)
	suite.Equal("failed to process lambda event with error: my bad", hook.LastEntry().Message)

	suite.Require().Equal(len(fakeCallback.Calls), len(expectedMessages))
	for _, expectedMsg := range expectedMessages {
		msgMatch := false
		for i := range fakeCallback.Calls {
			msg := fakeCallback.Calls[i].Arguments.Get(1).(*Message)

			if expectedMsg.ID != msg.ID {
				continue
			}

			// Spot check all fields except Callback. Go does not support function pointer comparison
			suite.Equal(*expectedMsg.Data.(*FakeHedwigDataField), *msg.Data.(*FakeHedwigDataField))
			suite.Equal(expectedMsg.Metadata, msg.Metadata)
			suite.Equal(expectedMsg.ID, msg.ID)
			suite.Equal(expectedMsg.Schema, msg.Schema)
			suite.Equal(expectedMsg.FormatVersion, msg.FormatVersion)

			// Validate callback is set
			suite.NotNil(msg.callback)

			msgMatch = true
		}

		// Ensure message matched at least one callback
		suite.True(msgMatch)
	}
}

func (suite *AWSClientTestSuite) TestAWSClient_PublishSNS() {
	ctx := context.Background()
	fakeSns := &FakeSns{}
	awsClient := &awsClient{
		sns: fakeSns,
	}

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	headers := map[string]string{
		"RequestID": "abcdefgh",
		"foo":       "bar",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", headers, &data)
	suite.Require().NoError(err)
	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	msgTopic := "dev-myapp"
	expectedTopic := getSNSTopic(suite.settings, msgTopic)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String(headers["foo"]),
		},
		"RequestID": {
			DataType:    aws.String("String"),
			StringValue: aws.String(headers["RequestID"]),
		},
	}

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(string(msgJSON)),
		MessageAttributes: attributes,
	}

	fakeSns.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return((*sns.PublishOutput)(nil), nil)

	err = awsClient.PublishSNS(ctx, suite.settings, msgTopic, string(msgJSON), headers)
	suite.NoError(err)

	fakeSns.AssertExpectations(suite.T())
}

func (suite *AWSClientTestSuite) TestAWSClient_PublishSNSError() {
	ctx := context.Background()
	fakeSns := &FakeSns{}
	awsClient := &awsClient{
		sns: fakeSns,
	}

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	headers := map[string]string{
		"RequestID": "abcdefgh",
		"foo":       "bar",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
	suite.Require().NoError(err)
	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	msgTopic := "dev-myapp"
	expectedTopic := getSNSTopic(suite.settings, msgTopic)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String(headers["foo"]),
		},
		"RequestID": {
			DataType:    aws.String("String"),
			StringValue: aws.String(headers["RequestID"]),
		},
	}

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(string(msgJSON)),
		MessageAttributes: attributes,
	}

	fakeSns.On("PublishWithContext", ctx, expectedSnsInput).Return((*sns.PublishOutput)(nil), errors.New("no internet"))

	err = awsClient.PublishSNS(ctx, suite.settings, msgTopic, string(msgJSON), headers)
	suite.EqualError(errors.Cause(err), "no internet")

	fakeSns.AssertExpectations(suite.T())
}

func (suite *AWSClientTestSuite) TestAWSClient_getSQSQueueURL() {
	ctx := context.Background()
	queueName := getSQSQueueName(suite.settings)
	expectedQueueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName

	fakeSqs := &FakeSQS{}
	awsClient := &awsClient{
		sqs: fakeSqs,
	}

	expectedInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &expectedQueueURL,
	}

	fakeSqs.On("GetQueueUrlWithContext", ctx, expectedInput, mock.Anything).Return(output, nil)

	queueURL, err := awsClient.getSQSQueueURL(ctx, queueName)
	suite.NoError(err)
	suite.Equal(expectedQueueURL, *queueURL)
	fakeSqs.AssertExpectations(suite.T())

	// another call shouldn't call API
	queueURL, err = awsClient.getSQSQueueURL(ctx, queueName)
	suite.NoError(err)
	suite.Equal(expectedQueueURL, *queueURL)
	fakeSqs.AssertExpectations(suite.T())
}

func (suite *AWSClientTestSuite) TestAWSClient_messageHandler() {
	ctx := context.Background()
	assertions := assert.New(suite.T())

	fakeCallback := suite.fakeCallback
	fakePreDeserializeHook := &FakePreDeserializeHook{}
	suite.settings.PreDeserializeHook = fakePreDeserializeHook.PreDeserializeHook
	awsClient := awsClient{}

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
	message.ID = ""
	suite.Require().NoError(err)
	// Override time so comparison does not fail due to precision
	message.Metadata.Timestamp = JSONTime(time.Unix(0, int64(time.Hour)))
	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	receipt := uuid.Must(uuid.NewV4()).String()
	message.Metadata.Receipt = receipt

	fakeCallback.On("Callback", ctx, mock.Anything).Return(nil)

	var messageBodyMap map[string]interface{}
	err = json.Unmarshal([]byte(msgJSON), &messageBodyMap)
	suite.Require().NoError(err)
	fakePreDeserializeHook.On("PreDeserializeHook", &ctx, &msgJSON).Return(nil)

	err = awsClient.messageHandler(ctx, suite.settings, msgJSON, receipt)
	assertions.Nil(err)

	fakeCallback.AssertExpectations(suite.T())
	fakePreDeserializeHook.AssertExpectations(suite.T())

	expectedMsg := message
	msg := fakeCallback.Calls[0].Arguments.Get(1).(*Message)

	// Spot check all fields except Callback. Go does not support function pointer comparison
	suite.Require().NoError(err)
	suite.Equal(*expectedMsg.Data.(*FakeHedwigDataField), *msg.Data.(*FakeHedwigDataField))
	suite.Equal(expectedMsg.Metadata, msg.Metadata)
	suite.Equal(expectedMsg.ID, msg.ID)
	suite.Equal(expectedMsg.Schema, msg.Schema)
	suite.Equal(expectedMsg.FormatVersion, msg.FormatVersion)
}

func (suite *AWSClientTestSuite) TestAWSClient_messageHandlerHookError() {
	ctx := context.Background()
	assertions := assert.New(suite.T())

	fakeCallback := suite.fakeCallback
	fakePreDeserializeHook := &FakePreDeserializeHook{}
	suite.settings.PreDeserializeHook = fakePreDeserializeHook.PreDeserializeHook
	awsClient := awsClient{}

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
	suite.Require().NoError(err)
	message.ID = ""
	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	expectedError := errors.Errorf("Fake error!")
	fakePreDeserializeHook.On("PreDeserializeHook", &ctx, &msgJSON).Return(expectedError)

	receipt := uuid.Must(uuid.NewV4()).String()
	err = awsClient.messageHandler(ctx, suite.settings, msgJSON, receipt)
	assertions.EqualError(errors.Cause(err), "Fake error!")

	fakeCallback.AssertExpectations(suite.T())
	fakePreDeserializeHook.AssertExpectations(suite.T())
}

func (suite *AWSClientTestSuite) TestAWSClient_messageHandlerNoHook() {
	ctx := context.Background()
	assertions := assert.New(suite.T())

	fakeCallback := suite.fakeCallback
	awsClient := awsClient{}

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
	message.ID = ""
	suite.Require().NoError(err)
	// Override time so comparison does not fail due to precision
	message.Metadata.Timestamp = JSONTime(time.Unix(0, int64(time.Hour)))
	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	receipt := uuid.Must(uuid.NewV4()).String()
	message.Metadata.Receipt = receipt

	fakeCallback.On("Callback", ctx, mock.Anything).Return(nil)

	err = awsClient.messageHandler(ctx, suite.settings, msgJSON, receipt)
	assertions.Nil(err)

	fakeCallback.AssertExpectations(suite.T())
}

func (suite *AWSClientTestSuite) TestAWSClient_messageHandlerNoCallbackRegistry() {
	ctx := context.Background()
	assertions := assert.New(suite.T())

	fakeCallback := suite.fakeCallback
	awsClient := awsClient{}

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
	message.ID = ""
	suite.Require().NoError(err)
	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	// Set to nil here so message can be created with no error
	suite.settings.CallbackRegistry = nil

	receipt := uuid.Must(uuid.NewV4()).String()
	message.Metadata.Receipt = receipt

	err = awsClient.messageHandler(ctx, suite.settings, msgJSON, receipt)
	assertions.Contains(err.Error(), "callbackRegistry is required")

	fakeCallback.AssertExpectations(suite.T())
}

func (suite *AWSClientTestSuite) TestAWSClient_messageHandlerFailsOnValidationFailure() {
	ctx := context.Background()
	fakeCallback := suite.fakeCallback
	awsClient := awsClient{}

	data := FakeHedwigDataField{
		VehicleID: "P_1234567890123456",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
	message.ID = ""
	suite.Require().NoError(err)
	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	receipt := uuid.Must(uuid.NewV4()).String()

	err = awsClient.messageHandler(ctx, suite.settings, msgJSON, receipt)
	suite.Contains(err.Error(), "validate")

	suite.True(fakeCallback.AssertNotCalled(suite.T(), "Callback"))
}

func (suite *AWSClientTestSuite) TestAWSClient_messageHandlerFailsOnCallbackFailure() {
	ctx := context.Background()
	awsClient := awsClient{}

	fakeCallback := suite.fakeCallback

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(suite.settings, "vehicle_created", "1.0", nil, &data)
	suite.Require().NoError(err)

	// Override time so comparison does not fail due to precision
	message.Metadata.Timestamp = JSONTime(
		time.Unix(0, int64(1)*int64(time.Hour)))
	message.validate()
	message.validateCallback(suite.settings)

	fakeCallback.On("Callback", ctx, mock.Anything).Return(errors.New("my bad"))

	msgJSON, err := message.JSONString()
	suite.Require().NoError(err)

	receipt := uuid.Must(uuid.NewV4()).String()
	message.Metadata.Receipt = receipt

	err = awsClient.messageHandler(ctx, suite.settings, msgJSON, receipt)
	suite.EqualError(err, "my bad")

	fakeCallback.AssertExpectations(suite.T())

	// Validate callback argument
	suite.Equal(len(fakeCallback.Calls), 1)

	expectedMsg := message
	msg := fakeCallback.Calls[0].Arguments.Get(1).(*Message)

	// Spot check all fields except Callback. Go does not support function pointer comparison
	suite.Equal(*expectedMsg.Data.(*FakeHedwigDataField), *msg.Data.(*FakeHedwigDataField))
	suite.Equal(expectedMsg.Metadata, msg.Metadata)
	suite.Equal(expectedMsg.ID, msg.ID)
	suite.Equal(expectedMsg.Schema, msg.Schema)
	suite.Equal(expectedMsg.FormatVersion, msg.FormatVersion)

	// Validate callback is set
	suite.NotNil(msg.callback)
}

func (suite *AWSClientTestSuite) TestAWSClient_messageHandlerFailsOnBadJSON() {
	ctx := context.Background()
	awsClient := awsClient{}
	receipt := uuid.Must(uuid.NewV4()).String()
	messageJSON := "bad json-"
	err := awsClient.messageHandler(ctx, suite.settings, string(messageJSON), receipt)
	suite.NotNil(err)
}

func (suite *AWSClientTestSuite) TestNewAWSClient() {
	sessionCache := &AWSSessionsCache{}

	iaws := newAWSClient(sessionCache, suite.settings)
	suite.NotNil(iaws)
}

func TestAWSClientTestSuite(t *testing.T) {
	suite.Run(t, new(AWSClientTestSuite))
}
