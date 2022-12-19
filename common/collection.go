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

// Package common provides functions shared between different parts of the connector.
package common

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// GetMongoCollection checks if the provided database and collection
// exist in a Mongo instance the client is connected to, and returns the [mongo.Collection] if they exist.
// By default, the Go Mongo driver creates a database and collection if they don't exist,
// so this function may come in handy when it comes to validations.
func GetMongoCollection(ctx context.Context, client *mongo.Client, db, collection string) (*mongo.Collection, error) {
	databaseNames, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list database names: %w", err)
	}

	databaseExist := false
	for _, databaseName := range databaseNames {
		if databaseName == db {
			databaseExist = true
		}
	}

	if !databaseExist {
		return nil, fmt.Errorf("database %q doesn't exist", db)
	}

	collectionNames, err := client.Database(db).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list collection names: %w", err)
	}

	collectionExist := false
	for _, collectionName := range collectionNames {
		if collectionName == collection {
			collectionExist = true
		}
	}

	if !collectionExist {
		return nil, fmt.Errorf("collection %q doesn't exist", collection)
	}

	return client.Database(db).Collection(collection), nil
}
