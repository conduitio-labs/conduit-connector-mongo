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
	"github.com/conduitio-labs/conduit-connector-mongo/destination/writer"
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
	config Config
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

// NewDestination creates new instance of the Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
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
