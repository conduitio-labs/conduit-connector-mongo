// Copyright © 2022 Meroxa, Inc. & Yalantis
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
	"fmt"
	"net/url"
	"strings"

	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/conduitio-labs/conduit-connector-mongo/validator"
)

// AuthMechanism defines a MongoDB authentication mechanism.
type AuthMechanism string

// The list of available authentication mechanisms is listed below.
const (
	SCRAMSHA256 AuthMechanism = "SCRAM-SHA-256"
	SCRAMSHA1   AuthMechanism = "SCRAM-SHA-1"
	MongoDBCR   AuthMechanism = "MONGODB-CR"
	MongoDBAWS  AuthMechanism = "MONGODB-AWS"
	X509        AuthMechanism = "X.509"
)

// IsValid checks if the underlying AuthMechanism is valid.
func (am AuthMechanism) IsValid() bool {
	switch am {
	case SCRAMSHA256, SCRAMSHA1, MongoDBCR, MongoDBAWS, X509:
		return true
	}

	return false
}

const (
	// KeyURI is a config name for a connection string.
	KeyURI = "uri"
	// KeyDB is a config name for a database.
	KeyDB = "db"
	// KeyCollection is a config name for a collection.
	KeyCollection = "collection"
	// KeyAuthUsername is a config name for a username.
	KeyAuthUsername = "auth.username"
	// KeyAuthPassword is a config name for a password.
	KeyAuthPassword = "auth.password"
	// KeyAuthDB is a config name for an authentication database.
	KeyAuthDB = "auth.db"
	// KeyAuthMechanism is a config name for an authentication mechanism.
	KeyAuthMechanism = "auth.mechanism"
	// KeyAuthTLSCAFile is a config name for a TLS CA file.
	KeyAuthTLSCAFile = "auth.tls.caFile"
	// KeyAuthTLSCertificateKeyFile is a config name for a TLS certificate key file.
	KeyAuthTLSCertificateKeyFile = "auth.tls.certificateKeyFile"
	// KeyAuthAWSSessionToken is a config name for an AWS session token.
	//
	//nolint:gosec // it's not hardcoded credentials
	KeyAuthAWSSessionToken = "auth.awsSessionToken"
)

// Config contains configurable values shared between
// source and destination MongoDB connector.
type Config struct {
	// URI is the connection string.
	// The URI can contain host names, IPv4/IPv6 literals, or an SRV record.
	URI string `key:"uri" validate:"required,uri"`
	// DB is the name of a database the connector must work with.
	DB string `key:"db" validate:"required,max=64"`
	// Collection is the name of a collection the connector must
	// write to (destination) or read from (source).
	Collection string `key:"collection" validate:"required"`

	Auth AuthConfig
}

// AuthConfig contains authentication-specific configurable values.
type AuthConfig struct {
	// Username is the username.
	Username string `key:"auth.username"`
	// Password is the user's password.
	Password string `key:"auth.password"`
	// DB is the name of a database that contains
	// the user's authentication data.
	DB string `key:"auth.db"`
	// Mechanism is the authentication mechanism.
	Mechanism AuthMechanism `key:"auth.mechanism"`
	// TLSCAFile is the path to either a single or a bundle of
	// certificate authorities to trust when making a TLS connection.
	TLSCAFile string `key:"auth.tls.caFile" validate:"omitempty,file"`
	// TLSCertificateKeyFile is the path to the client certificate
	// file or the client private key file.
	TLSCertificateKeyFile string `key:"auth.tls.certificateKeyFile" validate:"omitempty,file"`
	// AWSSessionToken is an AWS session token.
	AWSSessionToken string `key:"auth.awsSessionToken"`
}

// Parse maps the incoming map to the [Config] and validates it.
func Parse(raw map[string]string) (Config, error) {
	config := Config{
		URI:        raw[KeyURI],
		DB:         raw[KeyDB],
		Collection: raw[KeyCollection],
		Auth: AuthConfig{
			Username:              raw[KeyAuthUsername],
			Password:              raw[KeyAuthPassword],
			DB:                    raw[KeyAuthDB],
			Mechanism:             AuthMechanism(strings.ToUpper(raw[KeyAuthMechanism])),
			TLSCAFile:             raw[KeyAuthTLSCAFile],
			TLSCertificateKeyFile: raw[KeyAuthTLSCertificateKeyFile],
			AWSSessionToken:       raw[KeyAuthAWSSessionToken],
		},
	}

	// validate auth mechanism if it's not empty
	if config.Auth.Mechanism != "" && !config.Auth.Mechanism.IsValid() {
		return Config{}, &InvalidAuthMechanismError{
			AuthMechanism: config.Auth.Mechanism,
		}
	}

	if err := validator.ValidateStruct(&config); err != nil {
		return Config{}, fmt.Errorf("validate struct: %w", err)
	}

	return config, nil
}

const (
	// awsSessionTokenPropertyName is a name of a AWS session token property
	// for the auth mechanism properties.
	//
	//nolint:gosec // it's not hardcoded credentials
	awsSessionTokenPropertyName = "AWS_SESSION_TOKEN"
	// tlsCAFile is a URL query name for a TLS CA file.
	tlsCAFileQueryName = "tlsCAFile"
	// tlsCertificateKeyFileQueryName is a URL query name for a TLS certificate key file.
	tlsCertificateKeyFileQueryName = "tlsCertificateKeyFile"
)

// GetOptions returns generated options for mongo connection depending on mechanism.
func (d *Config) GetOptions() *options.ClientOptions {
	uri, properties := d.getURIAndPropertiesByMechanism()
	opts := options.Client().ApplyURI(uri)
	if d.Auth == (AuthConfig{}) {
		return opts
	}

	cred := options.Credential{
		AuthMechanism:           string(d.Auth.Mechanism),
		AuthMechanismProperties: properties,
		AuthSource:              d.Auth.DB,
		Username:                d.Auth.Username,
		Password:                d.Auth.Password,
	}

	return opts.SetAuth(cred)
}

// getURIAndPropertiesByMechanism generates uri and options depending on auth mechanism.
func (d *Config) getURIAndPropertiesByMechanism() (string, map[string]string) {
	//nolint:exhaustive // because most of the mechanisms using same options
	switch d.Auth.Mechanism {
	case X509:
		parsedURI, _ := url.Parse(d.URI)

		values := parsedURI.Query()
		values.Add(tlsCAFileQueryName, d.Auth.TLSCAFile)
		values.Add(tlsCertificateKeyFileQueryName, d.Auth.TLSCertificateKeyFile)

		parsedURI.RawQuery = values.Encode()

		return parsedURI.String(), nil
	case MongoDBAWS:
		return d.URI, map[string]string{
			awsSessionTokenPropertyName: d.Auth.AWSSessionToken,
		}
	default:
		return d.URI, nil
	}
}
