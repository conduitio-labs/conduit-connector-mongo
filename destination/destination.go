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

//go:generate mockgen -destination mock/destination.go -package mock . Writer

package destination

import (
	"context"
	"fmt"
	"reflect"

	"github.com/conduitio-labs/conduit-connector-mongo/codec"
	"github.com/conduitio-labs/conduit-connector-mongo/common"
	mconfig "github.com/conduitio-labs/conduit-connector-mongo/config"
	"github.com/conduitio-labs/conduit-connector-mongo/destination/writer"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/mongo"
)

// Writer defines a writer interface needed for the [Destination].
type Writer interface {
	Write(ctx context.Context, record opencdc.Record) error
}

// Destination Mongo Connector persists records to a MongoDB.
type Destination struct {
	sdk.UnimplementedDestination

	writer Writer
	client *mongo.Client
	config mconfig.Config
}

// NewDestination creates new instance of the Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() config.Parameters {
	return map[string]config.Parameter{
		mconfig.KeyURI: {
			Default: "mongodb://localhost:27017",
			Description: "The connection string. " +
				"The URI can contain host names, IPv4/IPv6 literals, or an SRV record.",
		},
		mconfig.KeyDB: {
			Default:     "",
			Description: "The name of a database the connector must work with.",
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		mconfig.KeyCollection: {
			Default:     "",
			Description: "The name of a collection the connector must read from.",
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		mconfig.KeyAuthUsername: {
			Default:     "",
			Description: "The username.",
		},
		mconfig.KeyAuthPassword: {
			Default:     "",
			Description: "The user's password.",
		},
		mconfig.KeyAuthDB: {
			Default:     "admin",
			Description: "The name of a database that contains the user's authentication data.",
		},
		mconfig.KeyAuthMechanism: {
			Default: "",
			Description: "The authentication mechanism. " +
				"The default mechanism, which is defined depending on the version of your MongoDB server.",
		},
		mconfig.KeyAuthTLSCAFile: {
			Default: "",
			Description: "The path to either a single or a bundle of certificate authorities" +
				" to trust when making a TLS connection.",
		},
		mconfig.KeyAuthTLSCertificateKeyFile: {
			Default:     "",
			Description: "The path to the client certificate file or the client private key file.",
		},
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(_ context.Context, cfg config.Config) error {
	configuration, err := mconfig.Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	var err error
	d.client, err = mongo.Connect(ctx, d.config.GetClientOptions().SetRegistry(newBSONCodecRegistry()))
	if err != nil {
		return fmt.Errorf("connect to mongo: %w", err)
	}

	if err = d.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping to mongo: %w", err)
	}

	collection, err := common.GetMongoCollection(ctx, d.client, d.config.DB, d.config.Collection)
	if err != nil {
		return fmt.Errorf("get mongo collection: %w", err)
	}

	d.writer = writer.NewWriter(collection)

	return nil
}

func newBSONCodecRegistry() *bsoncodec.Registry {
	registry := bson.NewRegistry()
	registry.RegisterKindEncoder(reflect.String, codec.StringObjectIDCodec{})

	return registry
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		if err := d.writer.Write(ctx, record); err != nil {
			return i, fmt.Errorf("write record: %w", err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.client != nil {
		if err := d.client.Disconnect(ctx); err != nil {
			return fmt.Errorf("client disconnect: %w", err)
		}
	}

	return nil
}
