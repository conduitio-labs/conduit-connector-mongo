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

package iterator

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// idFieldName is a reserved name for use as a primary key in MongoDB.
const idFieldName = "_id"

// snapshot is a snapshot iterator for the MongoDB source connector.
type snapshot struct {
	collection     *mongo.Collection
	orderingColumn string
	batchSize      int
	cursor         *mongo.Cursor
	position       *position
}

// newSnapshot creates a new instance of the [snapshot] iterator.
func newSnapshot(collection *mongo.Collection, orderingColumn string, batchSize int, position *position) *snapshot {
	return &snapshot{
		collection:     collection,
		orderingColumn: orderingColumn,
		batchSize:      batchSize,
		position:       position,
	}
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

	// try to create and marshal the record position
	position := &position{
		Mode:    modeSnapshot,
		Element: element[s.orderingColumn],
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

	return sdk.Util.Source.NewRecordSnapshot(
		sdkPosition, metadata,
		sdk.StructuredData{idFieldName: element[idFieldName]}, sdk.StructuredData(element),
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

// loadBatch finds a batch of elements in a MongoDB collection, based on the snapshot's
// collection, orderingColumn, batchSize, and the current position.
func (s *snapshot) loadBatch(ctx context.Context) error {
	opts := options.Find().
		SetSort(bson.M{s.orderingColumn: 1}).
		SetLimit(int64(s.batchSize))

	filter := bson.M{}
	// if the snapshot position is not nil and its element is not empty,
	// we'll do cursor-based pagination and ask for elements that are greater
	// than the element
	if s.position != nil && s.position.Element != nil {
		positionElement := s.processPositionElement(s.position.Element)

		filter[s.orderingColumn] = bson.M{
			"$gt": positionElement,
		}
	}

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("execute find: %w", err)
	}

	s.cursor = cursor

	return nil
}

// processPositionElement tries to parse the positionElement as a [primitive.ObjectID].
//   - If the positionElement is a valid hex representation of the MongoDB ObjectID
//     the method returns it as a [primitive.ObjectID].
//   - If the positionElement is not a valid hex representation of the MongoDB ObjectID
//     the method returns the provided value without any modifications.
func (s *snapshot) processPositionElement(positionElement any) any {
	if positionElementStr, ok := positionElement.(string); ok {
		positionElementObjectID, err := primitive.ObjectIDFromHex(positionElementStr)
		if err == nil {
			return positionElementObjectID
		}
	}

	return positionElement
}
