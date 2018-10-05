/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCompiledSchema(inputSchema string) (*jsonschema.Schema, error) {
	addJSONSchemaCustomFormats()
	url := "schema.json"
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft4
	if err := compiler.AddResource(url, strings.NewReader(inputSchema)); err != nil {
		return nil, err
	}
	schema, err := compiler.Compile(url)
	return schema, err
}

func TestJSONSchemaFormatHumanUUIDValid(t *testing.T) {
	testSchema := `{"type": "string", "format": "human-uuid"}`
	schema, err := newTestCompiledSchema(testSchema)
	require.NoError(t, err)

	cases := []*strings.Reader{
		strings.NewReader(`"6cac5588-24cc-4b4f-bbf9-7dc0ce93f96e"`),
		strings.NewReader(`"eb0a0a70-3c37-4898-a73f-1b120c0eaf03"`),
	}

	for _, c := range cases {
		err = schema.Validate(c)
		assert.Nil(t, err)
	}
}

func TestJSONSchemaFormatHumanUUIDInvalid(t *testing.T) {
	testSchema := `{"type": "string", "format": "human-uuid"}`
	schema, err := newTestCompiledSchema(testSchema)
	require.NoError(t, err)

	cases := []*strings.Reader{
		strings.NewReader(`"abcd"`),
		strings.NewReader(`"yyyyyyyy-tttt-416a-92ed-420e62b33eb5"`),
	}

	for _, c := range cases {
		err = schema.Validate(c)
		assert.NotNil(t, err)
	}
}

func TestSchemaKeyFromSchemaValid(t *testing.T) {
	schema := "hedwig.automatic.com/schema#/schemas/vehicle_created/1.0"
	schemaKey, err := schemaKeyFromSchema(schema)
	require.NoError(t, err)

	expectedSchemaKey := "vehicle_created/1.*"
	assert.Equal(t, expectedSchemaKey, schemaKey)
}

func TestSchemaKeyFromSchemaInvalidSchema(t *testing.T) {
	assertions := assert.New(t)

	schema := "hedwig.automatic.com/schema#/schemas/vehicle_created/1.a"
	schemaKey, err := schemaKeyFromSchema(schema)
	assertions.NotNil(err)
	assertions.Empty(schemaKey)
}

func TestValidateValid(t *testing.T) {
	settings := createTestSettings()
	validator := settings.Validator
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	require.NoError(t, err)

	err = validator.Validate(message)
	assert.Nil(t, err)
}

func TestInvalidateInvalidMinorVersion(t *testing.T) {
	settings := createTestSettings()
	validator := settings.Validator
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.9"
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	require.NoError(t, err)

	err = validator.Validate(message)
	assert.NotNil(t, err)
}

func TestInvalidNoXVersions(t *testing.T) {
	assertions := assert.New(t)
	schemaMissingXversions := `
	{
		"id": "https://hedwig.automatic.com/schema",
		"$schema": "http://json-schema.org/draft-04/schema#",
		"description": "Test Schema for Hedwig messages",
		"schemas": {
			"trip_created": {
				"1": {
					"description": "This is a message type",
					"type": "object",
					"required": [
						"vehicle_id",
						"user_id"
					],
					"properties": {
						"vehicle_id": {
							"type": "string"
						},
						"user_id": {
							"type": "string"
						},
						"vin": {
							"type": "string"
						}
					}
				}
			}
		}
	}
	`
	v, err := NewMessageValidatorFromBytes([]byte(schemaMissingXversions))
	assertions.Nil(v)
	assertions.NotNil(err)
	assertions.Equal("x-versions not defined for message for schemaURL: https://hedwig.automatic.com/schema/schemas/trip_created/1", err.Error())
}

func TestValidateInvalidSchema(t *testing.T) {
	settings := createTestSettings()
	validator := settings.Validator
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "100.0"
	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	require.NoError(t, err)

	err = validator.Validate(message)
	assert.NotNil(t, err)
}

func TestValidateInvalidData(t *testing.T) {
	settings := createTestSettings()
	validator := settings.Validator
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "100.0"
	data := FakeHedwigDataField{
		VehicleID: "123",
	}
	message, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, map[string]string{}, &data)
	require.NoError(t, err)

	err = validator.Validate(message)
	assert.NotNil(t, err)
}
