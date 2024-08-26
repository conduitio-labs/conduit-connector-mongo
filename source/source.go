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

//go:generate mockgen -destination mock/source.go -package mock . Iterator

// Package source implements the source logic of the MongoDB connector.
package source

import (
	"context"
	"fmt"
	"reflect"

	"github.com/conduitio-labs/conduit-connector-mongo/codec"
	"github.com/conduitio-labs/conduit-connector-mongo/common"
	mconfig "github.com/conduitio-labs/conduit-connector-mongo/config"
	"github.com/conduitio-labs/conduit-connector-mongo/source/iterator"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/mongo"
)

// Iterator defines an Iterator interface needed for the [Source].
type Iterator interface {
	HasNext(context.Context) (bool, error)
	Next(context.Context) (opencdc.Record, error)
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
	return sdk.SourceWithMiddleware(
		&Source{},
		sdk.DefaultSourceMiddleware(
			// disable schema extraction by default, because the source produces raw data
			sdk.SourceWithSchemaExtractionConfig{
				PayloadEnabled: lang.Ptr(false),
				KeyEnabled:     lang.Ptr(false),
			},
		)...,
	)
}

// Parameters is a map of named Parameters that describe how to configure the [Source].
//
//nolint:funlen,nolintlint // yeah, this function can become long at some point.
func (s *Source) Parameters() config.Parameters {
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
			Default:     "",
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
		ConfigKeyBatchSize: {
			Default:     "1000",
			Description: "The size of a document batch.",
		},
		ConfigKeySnapshot: {
			Default: "true",
			Description: "The field determines whether or not the connector " +
				"will take a snapshot of the entire collection before starting CDC mode.",
		},
		ConfigKeyOrderingField: {
			Default: "_id",
			Description: "The name of a field that is used for ordering " +
				"collection documents when capturing a snapshot.",
		},
	}
}

// Configure provides the connector with the configuration that is validated and stored.
// In case the configuration is not valid it returns an error.
func (s *Source) Configure(_ context.Context, raw config.Config) error {
	sourceConfig, err := ParseConfig(raw)
	if err != nil {
		return fmt.Errorf("parse source config: %w", err)
	}

	s.config = sourceConfig

	return nil
}

// Open opens needed connections and prepares to start producing records.
func (s *Source) Open(ctx context.Context, sdkPosition opencdc.Position) error {
	opts := s.config.GetClientOptions().SetRegistry(newBSONCodecRegistry())

	var err error
	s.client, err = mongo.Connect(ctx, opts)
	if err != nil {
		return fmt.Errorf("connect to mongo: %w", err)
	}

	if err = s.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping mongo server: %w", err)
	}

	collection, err := common.GetMongoCollection(ctx, s.client, s.config.DB, s.config.Collection)
	if err != nil {
		return fmt.Errorf("get mongo collection: %w", err)
	}

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

func newBSONCodecRegistry() *bsoncodec.Registry {
	registry := bson.NewRegistry()

	registry.RegisterTypeMapEntry(bson.TypeObjectID, reflect.TypeOf(""))
	registry.RegisterTypeMapEntry(bson.TypeArray, reflect.TypeOf([]any{}))
	registry.RegisterKindEncoder(reflect.String, codec.StringObjectIDCodec{})

	return registry
}

// Read returns a new [opencdc.Record].
// It can return the error [sdk.ErrBackoffRetry] to signal to the SDK
// it should call Read again with a backoff retry.
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("get next record: %w", err)
	}

	return record, nil
}

// Ack just logs a provided position.
func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
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
