// Copyright © 2023 Meroxa, Inc. & Yalantis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package config defines configurable values shared between Source and Destination
// and implements a method to parse them.
package config

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"
)

// defaultConnectionURI is a default MongoDB connection URI string.
var defaultConnectionURI = &url.URL{Scheme: "mongodb", Host: "localhost:27017"}

const (
	// defaultServerSelectionTimeout is a default value for the ServerSelectionTimeout option.
	defaultServerSelectionTimeout = time.Second * 5

	// awsSessionTokenPropertyName is a name of a AWS session token property
	// for the auth mechanism properties.
	awsSessionTokenPropertyName = "AWS_SESSION_TOKEN" //nolint:gosec // it's not hardcoded credentials
	// tlsCAFile is a URL query name for a TLS CA file.
	tlsCAFileQueryName = "tlsCAFile"
	// tlsCertificateKeyFileQueryName is a URL query name for a TLS certificate key file.
	tlsCertificateKeyFileQueryName = "tlsCertificateKeyFile"
)

// AuthMechanism defines a MongoDB authentication mechanism.
type AuthMechanism string

// The list of available authentication mechanisms is listed below.
const (
	SCRAMSHA256 AuthMechanism = "SCRAM-SHA-256"
	SCRAMSHA1   AuthMechanism = "SCRAM-SHA-1"
	MongoDBCR   AuthMechanism = "MONGODB-CR"
	MongoDBAWS  AuthMechanism = "MONGODB-AWS"
	MongoDBX509 AuthMechanism = "MONGODB-X509"
)

// IsValid checks if the underlying AuthMechanism is valid.
func (am AuthMechanism) IsValid() bool {
	switch am {
	case SCRAMSHA256, SCRAMSHA1, MongoDBCR, MongoDBAWS, MongoDBX509:
		return true
	}

	return false
}

// Config contains configurable values shared between
// source and destination MongoDB connector.
type Config struct {
	// URI is the connection string.
	// The URI can contain host names, IPv4/IPv6 literals, or an SRV record.
	URIStr string `json:"uri" default:"mongodb://localhost:27017"`
	uri    *url.URL

	// DB is the name of a database the connector must work with.
	DB string `json:"db" validate:"required"`
	// Collection is the name of a collection the connector must
	// write to (destination) or read from (source).
	Collection string `json:"collection" validate:"required"`

	Auth AuthConfig
}

// AuthConfig contains authentication-specific configurable values.
type AuthConfig struct {
	// Username is the username.
	Username string `json:"auth.username"`
	// Password is the user's password.
	Password string `json:"auth.password"`
	// DB is the name of a database that contains
	// the user's authentication data.
	DB string `json:"auth.db"`
	// Mechanism is the authentication mechanism.
	Mechanism AuthMechanism `json:"auth.mechanism"`
	// TLSCAFile is the path to either a single or a bundle of
	// certificate authorities to trust when making a TLS connection.
	TLSCAFile string `json:"auth.tls.caFile,omitempty"`
	// TLSCertificateKeyFile is the path to the client certificate
	// file or the client private key file.
	TLSCertificateKeyFile string `json:"auth.tls.certificateKeyFile,omitempty"`
	// AWSSessionToken is an AWS session token.
	AWSSessionToken string `json:"auth.awsSessionToken"`
}

func (c *Config) Validate(ctx context.Context) error {
	var errs []error
	uri, err := url.Parse(c.URIStr)
	if err != nil {
		errs = append(errs, err)
	} else {
		c.uri = uri
	}

	err = c.validatePath("auth.tls.caFile", c.Auth.TLSCAFile)
	if err != nil {
		errs = append(errs, err)
	}

	err = c.validatePath("auth.tls.certificateKeyFile", c.Auth.TLSCertificateKeyFile)
	if err != nil {
		errs = append(errs, err)
	}

	// validate auth mechanism if it's not empty
	if c.Auth.Mechanism != "" && !c.Auth.Mechanism.IsValid() {
		errs = append(errs, fmt.Errorf("invalid auth mechanism %q", c.Auth.Mechanism))
	}

	return errors.Join(errs...)
}

// GetClientOptions returns generated options for mongo connection depending on mechanism.
func (c *Config) GetClientOptions() *options.ClientOptions {
	uri, properties := c.getURIAndPropertiesByMechanism()
	opts := options.Client().ApplyURI(uri).SetServerSelectionTimeout(defaultServerSelectionTimeout)

	// If we don't have any custom auth options, we should skip adding credential options
	if c.Auth == (AuthConfig{}) {
		return opts
	}

	cred := options.Credential{
		AuthMechanism:           string(c.Auth.Mechanism),
		AuthMechanismProperties: properties,
		AuthSource:              c.Auth.DB,
		Username:                c.Auth.Username,
		Password:                c.Auth.Password,
	}

	return opts.SetAuth(cred)
}

// getURIAndPropertiesByMechanism generates uri and options depending on auth mechanism.
func (c *Config) getURIAndPropertiesByMechanism() (string, map[string]string) {
	//nolint:exhaustive // because most of the mechanisms using same options
	switch c.Auth.Mechanism {
	case MongoDBX509:
		uri := *c.uri

		values := uri.Query()

		if c.Auth.TLSCAFile != "" {
			values.Add(tlsCAFileQueryName, c.Auth.TLSCAFile)
		}

		if c.Auth.TLSCertificateKeyFile != "" {
			values.Add(tlsCertificateKeyFileQueryName, c.Auth.TLSCertificateKeyFile)
		}

		uri.RawQuery = values.Encode()

		return uri.String(), nil

	case MongoDBAWS:
		var properties map[string]string
		if c.Auth.AWSSessionToken != "" {
			properties = map[string]string{
				awsSessionTokenPropertyName: c.Auth.AWSSessionToken,
			}
		}

		return c.uri.String(), properties

	default:
		return c.uri.String(), nil
	}
}

func (c *Config) validatePath(paramName, path string) error {
	if path == "" {
		return nil
	}

	_, err := os.Stat(c.Auth.TLSCAFile)
	if err != nil {
		return fmt.Errorf("path for %s %q not valid: %w", paramName, path, err)
	}

	return nil
}
