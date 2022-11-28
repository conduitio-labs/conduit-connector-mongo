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
	// defaultIDFieldName contains default reserved primary key from MongoDB.
	defaultIDFieldName = "_id"

	// setCommand contains command, that used during Update query.
	setCommand = "$set"
)

// Writer implements a writer logic for Mongo destination.
type Writer struct {
	db    *mongo.Collection
	table string
}

// Params is an incoming params for the NewWriter function.
type Params struct {
	DB    *mongo.Collection
	Table string
}

// NewWriter creates new instance of the Writer.
func NewWriter(ctx context.Context, params Params) (*Writer, error) {
	writer := &Writer{
		db:    params.DB,
		table: params.Table,
	}

	return writer, nil
}

// Write inserts a sdk.Record into a Destination.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	if err := sdk.Util.Destination.Route(ctx,
		record,
		w.insert,
		w.update,
		w.delete,
		w.insert,
	); err != nil {
		return fmt.Errorf("route %s: %w", record.Operation.String(), err)
	}

	return nil
}

func (w *Writer) Close(ctx context.Context) error {
	err := w.db.Database().Client().Disconnect(ctx)
	if err != nil {
		return fmt.Errorf("close db: %w", err)
	}

	return nil
}

func (w *Writer) insert(ctx context.Context, record sdk.Record) error {
	payload := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	if _, err := w.db.InsertOne(ctx, payload); err != nil {
		return fmt.Errorf("insert data: %w", err)
	}

	return nil
}

func (w *Writer) update(ctx context.Context, record sdk.Record) error {
	payload, err := parsePayload(record)
	if err != nil {
		return fmt.Errorf("parse payload: %w", err)
	}

	ids, err := parseKeys(record)
	if err != nil {
		return fmt.Errorf("parse keys: %w", err)
	}

	filter := generateBsonFromMap(ids)
	body := generateBsonFromMap(payload)

	if _, err := w.db.UpdateOne(ctx,
		filter,
		bson.D{{
			Key:   setCommand,
			Value: body,
		}},
	); err != nil {
		return fmt.Errorf("update payload into destination: %w", err)
	}

	return nil
}

func (w *Writer) delete(ctx context.Context, record sdk.Record) error {
	ids, err := parseKeys(record)
	if err != nil {
		return fmt.Errorf("parse keys: %w", err)
	}

	filter := generateBsonFromMap(ids)

	if _, err := w.db.DeleteOne(ctx,
		filter,
	); err != nil {
		return fmt.Errorf("delete data from destination: %w", err)
	}

	return nil
}

// parsePayload is parsing Payload.After from record and deleting key from payload (we using it from Key field).
func parsePayload(data sdk.Record) (map[string]any, error) {
	parsed := make(sdk.StructuredData)
	if err := json.Unmarshal(data.Payload.After.Bytes(), &parsed); err != nil {
		return nil, fmt.Errorf("parse payload: %w", err)
	}

	delete(parsed, defaultIDFieldName) // deleting key from payload arguments

	return parsed, nil
}

// parseKeys extracts key fields from record trying to convert them to ObjectID type.
func parseKeys(data sdk.Record) (sdk.StructuredData, error) {
	parsed := make(sdk.StructuredData)
	if err := json.Unmarshal(data.Key.Bytes(), &parsed); err != nil {
		return nil, fmt.Errorf("parse payload: %w", err)
	}

	return parsed, nil
}

// generateBsonFromMap generates bson.D object from map[string]any data.
func generateBsonFromMap(m map[string]any) bson.D {
	res := make(bson.D, 0, len(m))
	for i, v := range m {
		res = append(res, bson.E{
			Key:   i,
			Value: v,
		})
	}

	return res
}
