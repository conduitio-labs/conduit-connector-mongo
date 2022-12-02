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

package mongo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/conduitio-labs/conduit-connector-mongo/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// set the directConnection to true in order to avoid the known hostname problem.
	testURI              = "mongodb://localhost:27017/?directConnection=true"
	testDB               = "test"
	testCollectionPrefix = "test_acceptance_coll"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

// GenerateRecord overrides the [sdk.ConfigurableAcceptanceTestDriver] GenerateRecord method.
// It generates a MongoDB-specific payload and a random bson.ObjectID key converted to a string.
func (d driver) GenerateRecord(t *testing.T, operation sdk.Operation) sdk.Record {
	t.Helper()

	id := primitive.NewObjectID().String()

	return sdk.Record{
		Operation: operation,
		Key: sdk.StructuredData{
			"_id": id,
		},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"_id":        id,
				"name":       gofakeit.Name(),
				"email":      gofakeit.Email(),
				"created_at": time.Now().Format(time.RFC3339),
			},
		},
	}
}

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		config.KeyURI: testURI,
		config.KeyDB:  testDB,
	}

	sdk.AcceptanceTest(t, driver{
		sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest:        beforeTest(cfg),
				AfterTest:         afterTest(cfg),
			},
		},
	})
}

// beforeTest set the config collection field to a unique name prefixed with the testCollectionPrefix.
func beforeTest(cfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		cfg[config.KeyCollection] = fmt.Sprintf("%s_%d", testCollectionPrefix, time.Now().UnixNano())
	}
}

// afterTest connects to a MongoDB instance and drops a test collection.
func afterTest(cfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		is := is.New(t)

		// create a test mongo client
		mongoClient, err := createTestMongoClient(context.Background(), cfg[config.KeyURI])
		is.NoErr(err)
		defer func() {
			err = mongoClient.Disconnect(context.Background())
			is.NoErr(err)
		}()

		// connect to the test database and collection
		testDatabase := mongoClient.Database(cfg[config.KeyDB])
		testCollection := testDatabase.Collection(cfg[config.KeyCollection])

		// drop the test collection
		err = testCollection.Drop(context.Background())
		is.NoErr(err)
	}
}

// createTestMongoClient connects to a MongoDB by a provided URI.
func createTestMongoClient(ctx context.Context, uri string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(uri)

	mongoClient, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	return mongoClient, nil
}
