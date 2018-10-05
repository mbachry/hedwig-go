/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/santhosh-tekuri/jsonschema"
	"github.com/santhosh-tekuri/jsonschema/formats"
)

var schemaKeyRegex *regexp.Regexp

const xVersionsKey = "x-versions"

// Add custom JSON schema formats
func init() {
	schemaKeyRegex = regexp.MustCompile(`([^/]+)/(\d+)\.(\d+)$`)
}

func addJSONSchemaCustomFormats() {
	humanUUIDRegex := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

	// Validates this is a human readable uuid (uuid separated by hyphens)
	formats.Register("human-uuid", func(in string) bool {
		return humanUUIDRegex.MatchString(in)
	})
}

// Convert full schema name to schema key to look up in schema.json
//   hedwig.automatic.com/schema#/schemas/vehicle_created/1.0 => vehicle_created/1.0
func schemaKeyFromSchema(schema string) (string, error) {
	m := schemaKeyRegex.FindStringSubmatch(schema)
	if len(m) == 0 {
		return "", errors.New("No schema key found")
	}

	return fmt.Sprintf("%s/%s.*", m[1], m[2]), nil
}

// IMessageValidator handles validating Hedwig messages
type IMessageValidator interface {
	SchemaRoot() string
	Validate(message *Message) error
}

func extractXVersions(schemaByte []byte, schemaURL string) ([]string, error) {
	schemaJSONDecoded := map[string]interface{}{}
	err := json.Unmarshal(schemaByte, &schemaJSONDecoded)
	if err != nil {
		return nil, err
	}
	xVersions, ok := schemaJSONDecoded[xVersionsKey]
	if !ok {
		return nil, errors.Errorf("x-versions not defined for message for schemaURL: %s", schemaURL)
	}
	typeConvertedXVersions := []string{}
	for _, version := range xVersions.([]interface{}) {
		typeConvertedXVersions = append(typeConvertedXVersions, version.(string))
	}
	return typeConvertedXVersions, nil
}

// NewMessageValidatorFromBytes from an byte encoded schema file
func NewMessageValidatorFromBytes(schemaFile []byte) (IMessageValidator, error) {
	addJSONSchemaCustomFormats()

	validator := messageValidator{
		compiledSchemaMap: make(map[string]*jsonschema.Schema),
		schemaVersionsMap: make(map[string]map[string]bool),
	}

	var parsedSchema map[string]interface{}
	err := json.Unmarshal(schemaFile, &parsedSchema)
	if err != nil {
		return nil, err
	}

	// Extract base url from schema id
	validator.schemaID = parsedSchema["id"].(string)

	schemaMap := parsedSchema["schemas"].(map[string]interface{})
	for schemaName, schemaVersionObj := range schemaMap {
		schemaVersionMap := schemaVersionObj.(map[string]interface{})
		for version, schema := range schemaVersionMap {
			schemaByte, err := json.Marshal(schema)
			if err != nil {
				return nil, err
			}

			compiler := jsonschema.NewCompiler()

			// Force to draft version 4
			compiler.Draft = jsonschema.Draft4

			schemaURL := fmt.Sprintf("%s/schemas/%s/%s", validator.schemaID, schemaName, version)
			schemaJSONDecoded := map[string]interface{}{}
			err = json.Unmarshal(schemaByte, &schemaJSONDecoded)
			if err != nil {
				return nil, err
			}

			xVersions, err := extractXVersions(schemaByte, schemaURL)
			if err != nil {
				return nil, err
			}

			err = compiler.AddResource(schemaURL, strings.NewReader(string(schemaByte)))
			if err != nil {
				return nil, err
			}

			err = compiler.AddResource(validator.schemaID, strings.NewReader(string(schemaFile)))
			if err != nil {
				return nil, err
			}

			schema, err := compiler.Compile(schemaURL)
			if err != nil {
				return nil, err
			}

			schemaKey := fmt.Sprintf("%s/%s", schemaName, version)
			validator.compiledSchemaMap[schemaKey] = schema
			versionsForThisSchema := map[string]bool{}
			for _, version := range xVersions {
				versionsForThisSchema[version] = true
			}
			validator.schemaVersionsMap[schemaKey] = versionsForThisSchema
		}
	}

	return &validator, nil
}

// NewMessageValidator creates a new validator from the given file
func NewMessageValidator(schemaFilePath string) (IMessageValidator, error) {
	rawSchema, err := ioutil.ReadFile(schemaFilePath)
	if err != nil {
		return nil, err
	}

	return NewMessageValidatorFromBytes(rawSchema)
}

// messageValidator is an implementation of MessageValidator
type messageValidator struct {
	// Format: schemakey("schema name/schema major version") => schema
	//   parking.created/3 => schema
	compiledSchemaMap map[string]*jsonschema.Schema
	// Format: schemakey => {version_string: bool}
	// 		   parking.created/3 => {"3.0": true, "3.1": true}
	schemaVersionsMap map[string]map[string]bool

	schemaID string
}

func (mv *messageValidator) SchemaRoot() string {
	return mv.schemaID
}

// Validate checks whether the Hedwig message is valid
func (mv *messageValidator) Validate(message *Message) error {
	if message == nil {
		return errors.New("No message given")
	}

	msgDataJSONStr, err := message.DataJSONString()
	if err != nil {
		// Unable to convert to JSON
		return err
	}

	schemaKey, err := schemaKeyFromSchema(message.Schema)
	if err != nil {
		return errors.Wrapf(err, "Invalid schema, no schema key found: %s", message.Schema)
	}

	if schema, ok := mv.compiledSchemaMap[schemaKey]; ok {
		if xVersions, ok := mv.schemaVersionsMap[schemaKey]; ok {
			msgSchema := message.dataSchemaVersion.Original()
			if _, ok := xVersions[msgSchema]; !ok {
				return errors.Errorf("version %s not in valid versions for %s", msgSchema, schemaKey)
			}
		}

		if err := schema.Validate(strings.NewReader(msgDataJSONStr)); err != nil {
			return errors.Wrapf(err, "message failed json-schema validation")
		}
		return nil
	}
	return errors.Errorf("No schema found for %s", schemaKey)
}
