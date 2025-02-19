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

package iterator

import (
	"context"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// The supported Change Stream event operation types are listed below.
const (
	operationTypeInsert = "insert"
	operationTypeUpdate = "update"
	operationTypeDelete = "delete"
)

// changeStreamMatchPipeline is a MongoDB Change Stream pipeline that
// filters and returns only insert, update and delete events.
var changeStreamMatchPipeline = bson.D{
	{
		Key: "$match", Value: bson.M{
			"operationType": bson.M{"$in": []string{
				operationTypeInsert,
				operationTypeUpdate,
				operationTypeDelete,
			}},
		},
	},
}

// changeStreamEvent defines a Change Stream event type.
// It consists of all fields sufficient to process inserts, updates, and deletes.
type changeStreamEvent struct {
	// ID is a BSON object which serves as an identifier for the Change Stream event.
	// This value is used as the resumeToken.
	ID bson.Raw `bson:"_id"`
	// DocumentKey contains the _id field of a document.
	DocumentKey map[string]any `bson:"documentKey"`
	// OperationType is the type of operation that the Change Stream reports.
	OperationType string `bson:"operationType"`
	// WallTime is the server date and time of the database operation.
	WallTime time.Time `bson:"wallTime"`
	// FullDocument contains all fields of a document.
	FullDocument map[string]any `bson:"fullDocument"`
	// Namespace is a namespace affected by the event.
	Namespace struct {
		// Collection is the name of a collection where the event occurred.
		Collection string `bson:"coll"`
	} `bson:"ns"`
}

// toRecord converts the underlying [changeStreamEvent] to an [opencdc.Record].
func (e changeStreamEvent) toRecord() (opencdc.Record, error) {
	position := &position{
		Mode:        modeCDC,
		ResumeToken: e.ID,
	}

	sdkPosition, err := position.marshalSDKPosition()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position into opencdc.Position: %w", err)
	}

	// set the record metadata
	metadata := make(opencdc.Metadata)
	metadata.SetCollection(e.Namespace.Collection)
	metadata.SetCreatedAt(e.WallTime)

	switch e.OperationType {
	case operationTypeInsert:
		return sdk.Util.Source.NewRecordCreate(
			sdkPosition, metadata, opencdc.StructuredData(e.DocumentKey), opencdc.StructuredData(e.FullDocument),
		), nil

	case operationTypeUpdate:
		return sdk.Util.Source.NewRecordUpdate(
			sdkPosition, metadata, opencdc.StructuredData(e.DocumentKey), nil, opencdc.StructuredData(e.FullDocument),
		), nil

	case operationTypeDelete:
		return sdk.SourceUtil{}.NewRecordDelete(
			sdkPosition, metadata, opencdc.StructuredData(e.DocumentKey), nil,
		), nil

	default:
		// this shouldn't happen as we filter Change Stream events by operation type,
		// and get only insert, update, and delete
		return opencdc.Record{}, errUnsupportedOperationType
	}
}

// cdc implements a Change Data Capture iterator for the MongoDB.
// It works by creating and listening to a MongoDB [Change Stream].
//
// [Change Stream]: https://www.mongodb.com/docs/manual/changeStreams/.
type cdc struct {
	changeStream *mongo.ChangeStream
}

// newCDC creates a new instance of the [cdc].
func newCDC(ctx context.Context, collection *mongo.Collection, position *position) (*cdc, error) {
	changeStream, err := createChangeStream(ctx, collection, position)
	if err != nil {
		return nil, fmt.Errorf("create change stream: %w", err)
	}

	return &cdc{
		changeStream: changeStream,
	}, nil
}

// hasNext checks whether the [cdc] iterator has records to return or not.
func (c *cdc) hasNext(ctx context.Context) (bool, error) {
	return c.changeStream.TryNext(ctx), c.changeStream.Err()
}

// next returns the next record.
func (c *cdc) next(_ context.Context) (opencdc.Record, error) {
	var event changeStreamEvent
	if err := c.changeStream.Decode(&event); err != nil {
		return opencdc.Record{}, fmt.Errorf("decode change stream event: %w", err)
	}

	record, err := event.toRecord()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("convert event to opencdc.Record: %w", err)
	}

	return record, nil
}

// stop stops the iterator.
func (c *cdc) stop(ctx context.Context) error {
	if c.changeStream != nil {
		if err := c.changeStream.Close(ctx); err != nil {
			return fmt.Errorf("close change stream: %w", err)
		}
	}

	return nil
}

// createChangeStream creates a MongoDB Change Stream for a provided collection.
// The resulting Change Stream will listen to events only with the following
// operation types: insert, update and delete.
//
// If a provided [position] is not empty and it has a resumeToken, the Change Stream
// will start listening to events from that particular position.
func createChangeStream(
	ctx context.Context,
	collection *mongo.Collection,
	position *position,
) (*mongo.ChangeStream, error) {
	// the UpdateLookup option includes a delta describing the changes to the document
	// and a copy of the entire document that was changed
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// if a position is not nil and its resumeToken is not empty,
	// we'll start listening to the Change Stream from that particular position
	if position != nil && position.ResumeToken != nil {
		opts = opts.SetResumeAfter(position.ResumeToken)
	}

	changeStream, err := collection.Watch(ctx, mongo.Pipeline{changeStreamMatchPipeline}, opts)
	if err != nil {
		return nil, fmt.Errorf("create change stream on the %q collection: %w", collection.Name(), err)
	}

	return changeStream, nil
}
