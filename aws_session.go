/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

func createSession(
	region string, awsAccessKey string, awsSecretAccessKey string, awsSessionToken string) *session.Session {
	return session.Must(session.NewSessionWithOptions(
		session.Options{
			Config: aws.Config{
				Credentials: credentials.NewStaticCredentialsFromCreds(
					credentials.Value{
						AccessKeyID:     awsAccessKey,
						SecretAccessKey: awsSecretAccessKey,
						SessionToken:    awsSessionToken,
					},
				),
				Region:     aws.String(region),
				DisableSSL: aws.Bool(false),
			},
		}))
}

type sessionKey struct {
	awsRegion       string
	awsAccessKeyID  string
	awsSessionToken string
}

// AWSSessionsCache is a cache that holds sessions
type AWSSessionsCache struct {
	sessionMap sync.Map
}

// NewAWSSessionsCache creates a new session cache
func NewAWSSessionsCache() *AWSSessionsCache {
	return &AWSSessionsCache{
		sessionMap: sync.Map{},
	}
}

func (c *AWSSessionsCache) getOrCreateSession(settings *Settings) *session.Session {
	region := settings.AWSRegion
	awsAccessKey := settings.AWSAccessKey
	awsSecretAccessKey := settings.AWSSecretKey
	awsSessionToken := settings.AWSSessionToken

	key := sessionKey{awsRegion: region, awsAccessKeyID: awsAccessKey, awsSessionToken: awsSessionToken}
	s, ok := c.sessionMap.Load(key)
	if !ok {
		s = createSession(region, awsAccessKey, awsSecretAccessKey, awsSessionToken)
		c.sessionMap.Store(key, s)
	}
	return s.(*session.Session)
}

// GetSession retrieves a session if it is cached, otherwise creates one
func (c *AWSSessionsCache) GetSession(settings *Settings) *session.Session {
	return c.getOrCreateSession(settings)
}
