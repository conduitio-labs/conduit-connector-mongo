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

package destination

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/conduitio-labs/conduit-connector-mongo/config"
)

// Writer defines a writer interface needed for the [Destination].
type Writer interface {
	InsertRecord(ctx context.Context, record sdk.Record) error
	Close(ctx context.Context) error
}

// Destination Vitess Connector persists records to a MySQL database via VTgate instance.
type Destination struct {
	sdk.UnimplementedDestination

	writer Writer
	config config.Config
}

// NewDestination creates new instance of the Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyURI: {
			Default:  "",
			Required: true,
			Description: "The connection string. " +
				"The URI can contain host names, IPv4/IPv6 literals, or an SRV record.",
		},
		config.KeyDB: {
			Default:     "",
			Required:    true,
			Description: "The name of a database the connector must work with.",
		},
		config.KeyCollection: {
			Default:     "",
			Required:    true,
			Description: "The name of a collection the connector must read from.",
		},
		config.KeyAuthUsername: {
			Default:     "",
			Required:    false,
			Description: "The username.",
		},
		config.KeyAuthPassword: {
			Default:     "",
			Required:    false,
			Description: "The user's password.",
		},
		config.KeyAuthDB: {
			Default:     "admin",
			Required:    false,
			Description: "The name of a database that contains the user's authentication data.",
		},
		config.KeyAuthMechanism: {
			Default:     "The default mechanism that defined depending on your MongoDB server version.",
			Required:    false,
			Description: "The authentication mechanism. ",
		},
		config.KeyAuthTLSCAFile: {
			Default:  "",
			Required: false,
			Description: "The path to either a single or a bundle of certificate authorities" +
				" to trust when making a TLS connection.",
		},
		config.KeyAuthTLSCertificateKeyFile: {
			Default:     "",
			Required:    false,
			Description: "The path to the client certificate file or the client private key file.",
		},
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	db, err := mongo.Connect(ctx, d.config.GetOptions())
	if err != nil {
		return fmt.Errorf("connect to mongo: %w", err)
	}

	err = db.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("ping to mongo: %w", err)
	}

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, record := range records {
		if err := d.writer.InsertRecord(ctx, record); err != nil {
			return i, fmt.Errorf("insert record: %w", err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.writer != nil {
		if err := d.writer.Close(ctx); err != nil {
			return fmt.Errorf("close writer: %w", err)
		}
	}

	return nil
}
