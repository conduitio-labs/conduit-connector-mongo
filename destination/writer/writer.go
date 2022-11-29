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

package writer

import (
	"context"
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// idFieldName contains default reserved primary key from MongoDB.
	idFieldName = "_id"

	// setCommand contains command, that used during Update query.
	setCommand = "$set"
)

// Writer implements a writer logic for Mongo destination.
type Writer struct {
	collection *mongo.Collection
}

// NewWriter creates new instance of the Writer.
func NewWriter(collection *mongo.Collection) *Writer {
	writer := &Writer{
		collection: collection,
	}

	return writer
}

// Write inserts a sdk.Record into a Destination.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	if err := sdk.Util.Destination.Route(ctx, record,
		w.insert,
		w.update,
		w.delete,
		w.insert,
	); err != nil {
		return fmt.Errorf("route %s: %w", record.Operation, err)
	}

	return nil
}

func (w *Writer) insert(ctx context.Context, record sdk.Record) error {
	payload := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	if _, err := w.collection.InsertOne(ctx, bson.M(payload)); err != nil {
		return fmt.Errorf("insert one: %w", err)
	}

	return nil
}

func (w *Writer) update(ctx context.Context, record sdk.Record) error {
	payload := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return fmt.Errorf("parse payload: %w", err)
	}

	delete(payload, idFieldName) // deleting key from payload arguments

	keys := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Key.Bytes(), &keys); err != nil {
		return fmt.Errorf("parse keys: %w", err)
	}

	if _, err := w.collection.UpdateOne(ctx, bson.M(keys), bson.M{setCommand: bson.M(payload)}); err != nil {
		return fmt.Errorf("update one: %w", err)
	}

	return nil
}

func (w *Writer) delete(ctx context.Context, record sdk.Record) error {
	keys := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Key.Bytes(), &keys); err != nil {
		return fmt.Errorf("parse keys: %w", err)
	}

	if _, err := w.collection.DeleteOne(ctx, bson.M(keys)); err != nil {
		return fmt.Errorf("delete one: %w", err)
	}

	return nil
}
