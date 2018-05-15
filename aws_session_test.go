/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper to count number of items in map
func numItems(m *sync.Map) int {
	count := 0
	m.Range(func(k, v interface{}) bool {
		count++
		return true
	})
	return count
}

func TestCreateSession(t *testing.T) {
	assertions := assert.New(t)

	awsAccessKey := "fakeAccess"
	awsSecretAccessKey := "fakeSecretKey"
	awsRegion := "us-east-1"
	awsSessionToken := "awsToken"
	s := createSession(awsRegion, awsAccessKey, awsSecretAccessKey, awsSessionToken)

	assertions.Equal(awsRegion, *s.Config.Region)

	value, err := s.Config.Credentials.Get()
	require.NoError(t, err)

	assertions.Equal(awsAccessKey, value.AccessKeyID)
	assertions.Equal(awsSecretAccessKey, value.SecretAccessKey)
}

func TestGetSessionCached(t *testing.T) {
	assertions := assert.New(t)

	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access",
		AWSSecretKey: "fake_secret",
	}

	sessionCache := NewAWSSessionsCache()
	assertions.Equal(0, numItems(&sessionCache.sessionMap))

	s := sessionCache.GetSession(settings)
	assertions.Equal(1, numItems(&sessionCache.sessionMap))

	settingsCopy := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access",
		AWSSecretKey: "fake_secret",
	}

	sCopy := sessionCache.GetSession(settingsCopy)
	assertions.Equal(1, numItems(&sessionCache.sessionMap))
	assertions.Equal(s, sCopy)
}

func TestGetSessionDifferent(t *testing.T) {
	assertions := assert.New(t)

	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
	}

	sessionCache := NewAWSSessionsCache()
	assertions.Equal(0, numItems(&sessionCache.sessionMap))

	s := sessionCache.GetSession(settings)
	assertions.Equal(1, numItems(&sessionCache.sessionMap))

	settingsCopy := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access_2",
		AWSSecretKey: "fake_secret_2",
	}

	sCopy := sessionCache.GetSession(settingsCopy)
	assertions.Equal(2, numItems(&sessionCache.sessionMap))
	assertions.NotEqual(s, sCopy)
}
