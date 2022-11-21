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

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	InsertRecord(ctx context.Context, record sdk.Record) error
	Close(ctx context.Context) error
}

// Destination Vitess Connector persists records to a MySQL database via VTgate instance.
type Destination struct {
	sdk.UnimplementedDestination

	writer Writer
	config Config
}

// NewDestination creates new instance of the Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyURI: {
			Default:     "",
			Required:    true,
			Description: "An URI of MongoDB server",
		},
		config.KeyAuthDB: {
			Default:     "",
			Required:    true,
			Description: "A database that contains credentials to auth",
		},
		config.KeyDB: {
			Default:     "",
			Required:    true,
			Description: "A database for connector to work with",
		},
		config.KeyCollection: {
			Default:     "",
			Required:    true,
			Description: "A collection for connector to work with",
		},
		config.KeyAuthUsername: {
			Default:     "",
			Required:    true,
			Description: "Username part of credentials",
		},
		config.KeyAuthPassword: {
			Default:     "",
			Required:    true,
			Description: "Password part of credentials",
		},
		config.KeyAuthMechanism: {
			Default:     "",
			Required:    false,
			Description: "A name of method which will be used to connect to MongoDB, if not set, using Mongo default",
		},
		config.KeyAuthTLSCAFile: {
			Default:     "",
			Required:    false,
			Description: "TLSCA filename to connect to MongoDB via X.509 Mechanism",
		},
		config.KeyAuthTLSCertificateKeyFile: {
			Default:     "",
			Required:    false,
			Description: "TLS Certificate Key filename to connect to MongoDB via X.509 Mechanism",
		},
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := ParseConfig(cfg)
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
		return fmt.Errorf("teardown: %w", d.writer.Close(ctx))
	}

	return nil
}
