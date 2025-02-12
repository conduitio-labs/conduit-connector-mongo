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
	"github.com/conduitio-labs/conduit-connector-mongo/source/iterator"
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

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

// NewSource creates a new instance of the [Source].
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(
		&Source{},
	)
}

// Parameters is a map of named Parameters that describe how to configure the [Source].
//
//nolint:funlen,nolintlint // yeah, this function can become long at some point.

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
