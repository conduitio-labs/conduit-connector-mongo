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
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	testEnvNameURI       = "CONNECTION_URI"
	testDB               = "test_acceptance"
	testCollectionPrefix = "test_acceptance_coll"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

// GenerateRecord overrides the [sdk.ConfigurableAcceptanceTestDriver] GenerateRecord method.
// It generates a MongoDB-specific payload and a random bson.ObjectID key converted to a string.
func (d driver) GenerateRecord(t *testing.T, operation opencdc.Operation) opencdc.Record {
	t.Helper()

	id := primitive.NewObjectID().String()

	return opencdc.Record{
		Operation: operation,
		Key: opencdc.StructuredData{
			"_id": id,
		},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"_id":        id,
				"name":       gofakeit.Name(),
				"email":      gofakeit.Email(),
				"created_at": time.Now().Format(time.RFC3339),
				"float64":    gofakeit.Float64(),
				"map":        map[string]any{"key1": gofakeit.Name(), "key2": gofakeit.Float64()},
				"slice":      []any{gofakeit.Name(), gofakeit.Float64()},
			},
		},
	}
}

func TestAcceptance(t *testing.T) {
	uri := os.Getenv(testEnvNameURI)
	if uri == "" {
		t.Skipf("%s env var must be set", testEnvNameURI)
	}

	cfg := map[string]string{
		"uri": uri,
		"db":  testDB,
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

		is := is.New(t)

		// create a test mongo client
		mongoClient, err := createTestMongoClient(context.Background(), cfg["uri"])
		is.NoErr(err)
		defer func() {
			err = mongoClient.Disconnect(context.Background())
			is.NoErr(err)
		}()

		cfg["collection"] = fmt.Sprintf("%s_%d", testCollectionPrefix, time.Now().UnixNano())

		// connect to the test database and create a collection
		testDatabase := mongoClient.Database(cfg["db"])
		is.NoErr(testDatabase.CreateCollection(context.Background(), cfg["collection"]))
	}
}

// afterTest connects to a MongoDB instance and drops a test collection.
func afterTest(cfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		is := is.New(t)

		// create a test mongo client
		mongoClient, err := createTestMongoClient(context.Background(), cfg["uri"])
		is.NoErr(err)
		defer func() {
			err = mongoClient.Disconnect(context.Background())
			is.NoErr(err)
		}()

		// connect to the test database and collection
		testDatabase := mongoClient.Database(cfg["db"])
		testCollection := testDatabase.Collection(cfg["collection"])

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
