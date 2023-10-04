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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// idFieldName is a reserved name for use as a primary key in MongoDB.
const idFieldName = "_id"

// snapshot is a snapshot iterator for the MongoDB source connector.
type snapshot struct {
	collection    *mongo.Collection
	orderingField string
	batchSize     int
	cursor        *mongo.Cursor
	position      *position
	// orderingFieldMaxValue is a max value of an ordering field
	// at the start of the snapshot. The snapshot iterator will only
	// grab fields with ordering field value less than or equal to this value.
	orderingFieldMaxValue any
	// resumeToken is needed for resuming the connector (particularly the CDC iterator)
	// after a pause that occurs just after the snapshot is completed.
	// That's why this value is stored in a snapshot position.
	resumeToken bson.Raw
	// polling defines if the snapshot is used to detect insertions
	// by polling for new documents in case CDC is not possible.
	polling bool
}

// snapshotParams is an incoming params for the [newSnapshot] function.
type snapshotParams struct {
	collection    *mongo.Collection
	orderingField string
	batchSize     int
	position      *position
	resumeToken   bson.Raw
}

// newSnapshot creates a new instance of the [snapshot] iterator.
func newSnapshot(ctx context.Context, params snapshotParams) (*snapshot, error) {
	var orderingFieldMaxValue any

	switch pos := params.position; {
	case pos != nil && params.position.MaxElement != nil:
		orderingFieldMaxValue = params.position.MaxElement

	default:
		var err error
		orderingFieldMaxValue, err = getMaxFieldValue(ctx, params.collection, params.orderingField)
		if err != nil && !errors.Is(err, errNoDocuments) {
			return nil, fmt.Errorf("get ordering field max value: %w", err)
		}
	}

	return &snapshot{
		collection:            params.collection,
		orderingField:         params.orderingField,
		batchSize:             params.batchSize,
		position:              params.position,
		orderingFieldMaxValue: orderingFieldMaxValue,
		resumeToken:           params.resumeToken,
	}, nil
}

// newPollingSnapshot creates a new instance of the [snapshot] iterator prepared for polling.
func newPollingSnapshot(ctx context.Context, params snapshotParams) (*snapshot, error) {
	pos := params.position
	if pos == nil || pos.Mode == modeSnapshot {
		orderingFieldMaxValue, err := getMaxFieldValue(ctx, params.collection, params.orderingField)
		if err != nil && !errors.Is(err, errNoDocuments) {
			return nil, fmt.Errorf("get ordering field max value: %w", err)
		}

		pos = &position{
			Mode:    modeCDC,
			Element: orderingFieldMaxValue,
		}
	}

	return &snapshot{
		collection:    params.collection,
		orderingField: params.orderingField,
		batchSize:     params.batchSize,
		position:      pos,
		polling:       true,
	}, nil
}

// hasNext checks whether the snapshot iterator has records to return or not.
func (s *snapshot) hasNext(ctx context.Context) (bool, error) {
	if s.cursor != nil && s.cursor.TryNext(ctx) {
		return true, nil
	}

	if err := s.loadBatch(ctx); err != nil {
		return false, fmt.Errorf("load batch: %w", err)
	}

	return s.cursor.TryNext(ctx), s.cursor.Err()
}

// next returns the next record.
func (s *snapshot) next(_ context.Context) (sdk.Record, error) {
	var element map[string]any
	if err := s.cursor.Decode(&element); err != nil {
		return sdk.Record{}, fmt.Errorf("decode element: %w", err)
	}

	// if the snapshot is polling new items,
	// we mark its position as CDC to identify it during pauses correctly
	mode := modeSnapshot
	if s.polling {
		mode = modeCDC
	}

	// try to create and marshal the record position
	position := &position{
		Mode:        mode,
		Element:     element[s.orderingField],
		MaxElement:  s.orderingFieldMaxValue,
		ResumeToken: s.resumeToken,
	}

	sdkPosition, err := position.marshalSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal sdk position: %w", err)
	}

	s.position = position

	// set the record metadata
	metadata := make(sdk.Metadata)
	metadata[metadataFieldCollection] = s.collection.Name()
	metadata.SetCreatedAt(time.Now())

	elementBytes, err := json.Marshal(element)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed marshalling record into JSON: %w", err)
	}

	if s.polling {
		return sdk.Util.Source.NewRecordCreate(
			sdkPosition,
			metadata,
			sdk.StructuredData{idFieldName: element[idFieldName]},
			sdk.RawData(elementBytes),
		), nil
	}

	return sdk.Util.Source.NewRecordSnapshot(
		sdkPosition,
		metadata,
		sdk.StructuredData{idFieldName: element[idFieldName]},
		sdk.RawData(elementBytes),
	), nil
}

// stop stops the iterator.
func (s *snapshot) stop(ctx context.Context) error {
	if s.cursor != nil {
		if err := s.cursor.Close(ctx); err != nil {
			return fmt.Errorf("close cursor: %w", err)
		}
	}

	return nil
}

// loadBatch finds a batch of documents in a MongoDB collection, based on the snapshot's
// collection, orderingField, batchSize, and the current position.
func (s *snapshot) loadBatch(ctx context.Context) error {
	opts := options.Find().
		SetSort(bson.M{s.orderingField: 1}).
		SetLimit(int64(s.batchSize))

	orderingFieldFilter := bson.M{}
	// if the snapshot ordering field max value is not nil,
	// we'll ask for documents that are less or equal to that value
	if s.orderingFieldMaxValue != nil {
		orderingFieldFilter["$lte"] = s.orderingFieldMaxValue
	}
	// if the snapshot position is not nil and its element is not empty,
	// we'll do cursor-based pagination and ask for documents that are greater
	// than the element
	if s.position != nil && s.position.Element != nil {
		orderingFieldFilter["$gt"] = s.position.Element
	}

	cursor, err := s.collection.Find(ctx, bson.M{s.orderingField: orderingFieldFilter}, opts)
	if err != nil {
		return fmt.Errorf("execute find: %w", err)
	}

	s.cursor = cursor

	return nil
}

// getMaxFieldValue returns the maximum field value that can be found in a MongoDB collection.
func getMaxFieldValue(ctx context.Context, collection *mongo.Collection, fieldName string) (any, error) {
	documentCount, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("count collection documents: %w", err)
	}

	// if a collection doesn't have any documents,
	// we'll return the errNoDocuments error and just skip the snapshot step.
	if documentCount == 0 {
		return nil, errNoDocuments
	}

	// this is the way we can get the maximum value of a specific field
	opts := options.Find().SetSort(bson.M{fieldName: -1}).SetLimit(1)

	cursor, err := collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("execute find: %w", err)
	}

	if !cursor.TryNext(ctx) {
		if cursor.Err() != nil {
			return nil, fmt.Errorf("cursor: %w", cursor.Err())
		}

		return nil, errMaxFieldValueNotFound
	}

	var element map[string]any
	if err := cursor.Decode(&element); err != nil {
		return nil, fmt.Errorf("decode cursor element: %w", err)
	}

	return element[fieldName], nil
}
