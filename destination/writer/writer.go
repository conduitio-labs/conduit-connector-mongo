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
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

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

// InsertRecord inserts a sdk.Record into a Destination.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	if err := sdk.Util.Destination.Route(ctx,
		record,
		w.insert,
		w.update,
		w.delete,
		w.insert,
	); err != nil {
		return fmt.Errorf("insert record error: %w", err)
	}

	return nil
}

func (w *Writer) Close(ctx context.Context) error {
	err := w.db.Database().Client().Disconnect(ctx)
	if err != nil {
		return fmt.Errorf("unable to close: %w", err)
	}

	return nil
}

const (
	// defaultIDFieldName contains default reserved primary key from MongoDB.
	defaultIDFieldName = "_id"

	// setCommand contains command, that used during Update query.
	setCommand = "$set"
)

func (w *Writer) insert(ctx context.Context, record sdk.Record) error {
	// we are not using method parsePayload, because we need to save _id in payload during insert
	// (it probably not appear in Key, and we need to handle that).
	parsed := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &parsed); err != nil {
		return fmt.Errorf("parse payload: %w", err)
	}

	keys, err := parseKeys(record)
	if err != nil {
		return fmt.Errorf("unable to parse key in update: %w", err)
	}

	// we should change keys into ObjectID type if possible for future queries.
	for i, v := range keys {
		if _, ok := parsed[i]; ok {
			parsed[i] = v
		}
	}

	if _, err := w.db.InsertOne(ctx, parsed); err != nil {
		return fmt.Errorf("insert data into destination: %w", err)
	}

	return nil
}

func (w *Writer) update(ctx context.Context, record sdk.Record) error {
	data, err := parsePayload(record)
	if err != nil {
		return fmt.Errorf("unable to parse payload in update: %w", err)
	}

	ids, err := parseKeys(record)
	if err != nil {
		return fmt.Errorf("unable to parse key in update: %w", err)
	}

	filter := generateBsonFromMap(ids)
	body := generateBsonFromMap(data)

	if _, err := w.db.UpdateOne(ctx,
		filter,
		bson.D{{
			Key:   setCommand,
			Value: body,
		}},
	); err != nil {
		return fmt.Errorf("upsert data into destination: %w", err)
	}

	return nil
}

func (w *Writer) delete(ctx context.Context, record sdk.Record) error {
	ids, err := parseKeys(record)
	if err != nil {
		return fmt.Errorf("unable to parse key in update: %w", err)
	}

	filter := generateBsonFromMap(ids)

	if _, err := w.db.DeleteOne(ctx,
		filter,
	); err != nil {
		return fmt.Errorf("upsert data into destination: %w", err)
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

	// trying to parse each Key into ObjectID for optimization.
	for key, keyValue := range parsed {
		parsed[key] = tryParseToObjectID(keyValue)
	}

	return parsed, nil
}

func tryParseToObjectID(data any) any {
	str, ok := data.(string)
	if !ok {
		return data
	}

	hex, err := primitive.ObjectIDFromHex(str)
	if err != nil {
		return data
	}

	return hex
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
