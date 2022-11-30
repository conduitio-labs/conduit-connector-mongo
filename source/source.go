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

// Package source implements the source logic of the MongoDB connector.
package source

import (
	"context"
	"fmt"
	"reflect"

	"github.com/conduitio-labs/conduit-connector-mongo/config"
	"github.com/conduitio-labs/conduit-connector-mongo/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// bsoncodec.RegistryBuilder allows us to specify the logic of
// decoding/encoding certain BSON types, that will be performed
// inside the MongoDB Go driver.
//
// In this particular case we convert bson.ObjectID to string
// when unmarshaling a raw BSON element to map[string]any.
var registry = bson.NewRegistryBuilder().
	RegisterTypeMapEntry(bsontype.ObjectID, reflect.TypeOf(string(""))).
	Build()

// Iterator defines an Iterator interface needed for the [Source].
type Iterator interface {
	HasNext(context.Context) (bool, error)
	Next(context.Context) (sdk.Record, error)
	Stop(context.Context) error
}

// Source implements the source logic of the MongoDB connector.
type Source struct {
	sdk.UnimplementedSource

	config   Config
	client   *mongo.Client
	iterator Iterator
}

// NewSource creates a new instance of the [Source].
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the [Source].
//
//nolint:funlen // yeah, this function can become long at some point.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyURI: {
			Default:  "mongodb://localhost:27017",
			Required: false,
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
		ConfigKeyBatchSize: {
			Default:     "1000",
			Required:    false,
			Description: "The size of a document batch.",
		},
		ConfigKeySnapshot: {
			Default:  "true",
			Required: false,
			Description: "The field determines whether or not the connector " +
				"will take a snapshot of the entire collection before starting CDC mode.",
		},
		ConfigKeyOrderingField: {
			Default:  "_id",
			Required: false,
			Description: "The name of a field that is used for ordering " +
				"collection elements when capturing a snapshot.",
		},
	}
}

// Configure provides the connector with the configuration that is validated and stored.
// In case the configuration is not valid it returns an error.
func (s *Source) Configure(ctx context.Context, raw map[string]string) error {
	sourceConfig, err := ParseConfig(raw)
	if err != nil {
		return fmt.Errorf("parse source config: %w", err)
	}

	s.config = sourceConfig

	return nil
}

// Open opens needed connections and prepares to start producing records.
func (s *Source) Open(ctx context.Context, sdkPosition sdk.Position) error {
	opts := options.Client().ApplyURI(s.config.URI.String()).SetRegistry(registry)

	var err error
	s.client, err = mongo.Connect(ctx, opts)
	if err != nil {
		return fmt.Errorf("connect to mongo: %w", err)
	}

	if err = s.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping mongo server: %w", err)
	}

	collection := s.client.Database(s.config.DB).Collection(s.config.Collection)

	s.iterator, err = iterator.NewCombined(ctx, iterator.CombinedParams{
		Collection:    collection,
		BatchSize:     s.config.BatchSize,
		Snapshot:      s.config.Snapshot,
		OrderingField: s.config.OrderingField,
		SDKPosition:   sdkPosition,
	})
	if err != nil {
		return fmt.Errorf("create combined iterator: %w", err)
	}

	return nil
}

// Read returns a new [sdk.Record].
// It can return the error [sdk.ErrBackoffRetry] to signal to the SDK
// it should call Read again with a backoff retry.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("get next record: %w", err)
	}

	return record, nil
}

// Ack just logs a provided position.
func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")

	return nil
}

// Teardown closes connections, stops iterators and prepares for a graceful shutdown.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		if err := s.iterator.Stop(ctx); err != nil {
			return fmt.Errorf("stop iterator: %w", err)
		}
	}

	if s.client != nil {
		if err := s.client.Disconnect(ctx); err != nil {
			return fmt.Errorf("client disconnect: %w", err)
		}
	}

	return nil
}
