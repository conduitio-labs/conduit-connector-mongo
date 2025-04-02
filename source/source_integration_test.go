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

package source_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	mongoConn "github.com/conduitio-labs/conduit-connector-mongo"
	"github.com/conduitio-labs/conduit-connector-mongo/codec"
	"github.com/conduitio-labs/conduit-connector-mongo/source"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	testEnvNameURI       = "CONNECTION_URI"
	testDB               = "test_source"
	testCollectionPrefix = "test_coll"
)

func TestSource_Open_failDatabaseNotExist(t *testing.T) {
	is := is.New(t)

	underTest := source.NewSource()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	//nolint:forcetypeassert // we know it's *Config
	cfg := underTest.Config().(*source.Config)
	prepareConfig(t, cfg)

	err := underTest.Open(ctx, nil)
	is.True(err != nil)
	is.Equal(
		err.Error(),
		fmt.Sprintf(`get mongo collection: database "%s" doesn't exist`, cfg.DB),
	)
}

func TestSource_Open_failCollectionNotExist(t *testing.T) {
	is := is.New(t)

	underTest := source.NewSource()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	//nolint:forcetypeassert // we know it's *Config
	cfg := underTest.Config().(*source.Config)
	prepareConfig(t, cfg)

	mongoClient, err := createTestMongoClient(ctx, cfg.URIStr)
	is.NoErr(err)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = mongoClient.Disconnect(cleanupCtx)
		is.NoErr(err)
	})

	// connect to a test database (this will create it automatically)
	testDatabase := mongoClient.Database(cfg.DB)
	// create a test collection with a wrong name
	wrongName := cfg.Collection + "s"
	is.NoErr(testDatabase.CreateCollection(ctx, wrongName))
	testCollection := testDatabase.Collection(wrongName)
	// drop the created test collection after the test
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = testCollection.Drop(cleanupCtx)
		is.NoErr(err)
	})

	err = underTest.Open(ctx, nil)
	is.True(err != nil)
	is.Equal(err.Error(), fmt.Sprintf(
		`get mongo collection: collection "%s" doesn't exist`, cfg.Collection),
	)
}

func TestSource_Read_successSnapshot(t *testing.T) {
	is := is.New(t)

	underTest := source.NewSource()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	//nolint:forcetypeassert // we know it's *Config
	cfg := underTest.Config().(*source.Config)
	prepareConfig(t, cfg)

	mongoClient, err := createTestMongoClient(ctx, cfg.URIStr)
	is.NoErr(err)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = mongoClient.Disconnect(cleanupCtx)
		is.NoErr(err)
	})

	// connect to the test database and create the test collection
	testDatabase := mongoClient.Database(cfg.DB)
	is.NoErr(testDatabase.CreateCollection(ctx, cfg.Collection))
	testCollection := testDatabase.Collection(cfg.Collection)
	// drop the created test collection after the test
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = testCollection.Drop(cleanupCtx)
		is.NoErr(err)
	})

	// insert a test item to the test collection
	testItem, err := createTestItem(ctx, testCollection)
	is.NoErr(err)

	err = underTest.Open(ctx, nil)
	is.NoErr(err)

	record, err := underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationSnapshot)
	is.Equal(record.Payload.After, testItem)
}

func TestSource_Read_continueSnapshot(t *testing.T) {
	is := is.New(t)

	underTest := source.NewSource()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	//nolint:forcetypeassert // we know it's *Config
	cfg := underTest.Config().(*source.Config)
	prepareConfig(t, cfg)

	mongoClient, err := createTestMongoClient(ctx, cfg.URIStr)
	is.NoErr(err)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = mongoClient.Disconnect(cleanupCtx)
		is.NoErr(err)
	})

	// connect to the test database and create the test collection
	testDatabase := mongoClient.Database(cfg.DB)
	is.NoErr(testDatabase.CreateCollection(ctx, cfg.Collection))
	testCollection := testDatabase.Collection(cfg.Collection)
	// drop the created test collection after the test
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = testCollection.Drop(cleanupCtx)
		is.NoErr(err)
	})

	// insert two test items to the test collection
	firstTestItem, err := createTestItem(ctx, testCollection)
	is.NoErr(err)

	secondTestItem, err := createTestItem(ctx, testCollection)
	is.NoErr(err)

	err = underTest.Open(ctx, nil)
	is.NoErr(err)

	// check the first item
	record, err := underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationSnapshot)
	is.Equal(record.Payload.After, firstTestItem)

	cancel()
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()

	err = underTest.Teardown(ctx)
	is.NoErr(err)

	// restart the source after the pause,
	// with the last record's position
	err = underTest.Open(ctx, record.Position)
	is.NoErr(err)

	// check that the connector can still see the second item
	// after the pause
	record, err = underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationSnapshot)
	is.Equal(record.Payload.After, secondTestItem)
}

func TestSource_Read_successCDC(t *testing.T) {
	is := is.New(t)

	underTest := source.NewSource()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	//nolint:forcetypeassert // we know it's *Config
	cfg := underTest.Config().(*source.Config)
	prepareConfig(t, cfg)

	mongoClient, err := createTestMongoClient(ctx, cfg.URIStr)
	is.NoErr(err)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = mongoClient.Disconnect(cleanupCtx)
		is.NoErr(err)
	})

	// connect to the test database and create the test collection
	testDatabase := mongoClient.Database(cfg.DB)
	is.NoErr(testDatabase.CreateCollection(ctx, cfg.Collection))
	testCollection := testDatabase.Collection(cfg.Collection)
	// drop the created test collection after the test
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = testCollection.Drop(cleanupCtx)
		is.NoErr(err)
	})

	err = underTest.Open(ctx, nil)
	is.NoErr(err)

	// we expect backoff retry and switch to CDC mode here
	_, err = underTest.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// insert a test item to the test collection
	testItem, err := createTestItem(ctx, testCollection)
	is.NoErr(err)

	// compare the record operation and its payload
	record, err := underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationCreate)
	is.Equal(record.Payload.After, testItem)

	// update the test item
	updatedTestItem, err := updateTestItem(ctx, testCollection, testItem)
	is.NoErr(err)

	// compare the record operation and its payload
	record, err = underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationUpdate)
	is.Equal(record.Payload.After, updatedTestItem)

	// delete the test item
	err = deleteTestItem(ctx, testCollection, updatedTestItem)
	is.NoErr(err)

	// compare the record operation, we expect it to be delete
	record, err = underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationDelete)
}

func TestSource_Read_successCDCAfterSnapshotPause(t *testing.T) {
	is := is.New(t)

	underTest := source.NewSource()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	//nolint:forcetypeassert // we know it's *Config
	cfg := underTest.Config().(*source.Config)
	prepareConfig(t, cfg)

	mongoClient, err := createTestMongoClient(ctx, cfg.URIStr)
	is.NoErr(err)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = mongoClient.Disconnect(cleanupCtx)
		is.NoErr(err)
	})

	// connect to the test database and create the test collection
	testDatabase := mongoClient.Database(cfg.DB)
	is.NoErr(testDatabase.CreateCollection(ctx, cfg.Collection))
	testCollection := testDatabase.Collection(cfg.Collection)
	// drop the created test collection after the test
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = testCollection.Drop(cleanupCtx)
		is.NoErr(err)
	})

	// insert a test item to the test collection
	snapshotItem, err := createTestItem(ctx, testCollection)
	is.NoErr(err)

	err = underTest.Open(ctx, nil)
	is.NoErr(err)

	// we expect a snapshot record
	record, err := underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationSnapshot)
	is.Equal(record.Payload.After, snapshotItem)

	// stop the source
	cancel()
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()

	err = underTest.Teardown(ctx)
	is.NoErr(err)

	// insert a test item to the test collection while the source is stopped
	cdcCreateItem, err := createTestItem(ctx, testCollection)
	is.NoErr(err)

	// update a test item to the test collection while the source is stopped
	cdcUpdateItem, err := updateTestItem(ctx, testCollection, cdcCreateItem)
	is.NoErr(err)

	// resume the source
	err = underTest.Open(ctx, record.Position)
	is.NoErr(err)

	// compare the record operation and its payload
	record, err = underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationCreate)
	is.Equal(record.Payload.After, cdcCreateItem)

	// compare the record operation and its payload
	record, err = underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationUpdate)
	is.Equal(record.Payload.After, cdcUpdateItem)
}

func TestSource_Read_continueCDC(t *testing.T) {
	is := is.New(t)

	underTest := source.NewSource()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	//nolint:forcetypeassert // we know it's *Config
	cfg := underTest.Config().(*source.Config)
	prepareConfig(t, cfg)

	mongoClient, err := createTestMongoClient(ctx, cfg.URIStr)
	is.NoErr(err)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = mongoClient.Disconnect(cleanupCtx)
		is.NoErr(err)
	})

	// connect to the test database and create the test collection
	testDatabase := mongoClient.Database(cfg.DB)
	is.NoErr(testDatabase.CreateCollection(ctx, cfg.Collection))
	testCollection := testDatabase.Collection(cfg.Collection)
	// drop the created test collection after the test
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		err = testCollection.Drop(cleanupCtx)
		is.NoErr(err)
	})

	err = underTest.Open(ctx, nil)
	is.NoErr(err)

	// we expect backoff retry and switch to CDC mode here
	_, err = underTest.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// insert a test item to the test collection
	firstTestItem, err := createTestItem(ctx, testCollection)
	is.NoErr(err)

	// compare the record operation and its payload
	record, err := underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationCreate)
	is.Equal(record.Payload.After, firstTestItem)

	// stop the source
	cancel()
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()

	err = underTest.Teardown(ctx)
	is.NoErr(err)

	// create another test item
	secondTestItem, err := createTestItem(ctx, testCollection)
	is.NoErr(err)

	// update the first test item
	updatedFirstItem, err := updateTestItem(ctx, testCollection, firstTestItem)
	is.NoErr(err)

	// restart the source after the pause,
	// with the last record's position
	err = underTest.Open(ctx, record.Position)
	is.NoErr(err)

	// check that the second item has been inserted
	record, err = underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationCreate)
	is.Equal(record.Payload.After, secondTestItem)

	// check that the first item has been updated
	record, err = underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationUpdate)
	is.Equal(record.Payload.After, updatedFirstItem)

	// stop the source one more time
	cancel()
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()

	err = underTest.Teardown(ctx)
	is.NoErr(err)

	// delete both test items
	err = deleteTestItem(ctx, testCollection, firstTestItem)
	is.NoErr(err)

	err = deleteTestItem(ctx, testCollection, secondTestItem)
	is.NoErr(err)

	// restart the source one more time,
	// with the last record's position
	err = underTest.Open(ctx, record.Position)
	is.NoErr(err)

	// check that both items have been deleted
	record, err = underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationDelete)
	is.Equal(record.Key, opencdc.StructuredData{"_id": firstTestItem["_id"]})

	record, err = underTest.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, opencdc.OperationDelete)
	is.Equal(record.Key, opencdc.StructuredData{"_id": secondTestItem["_id"]})
}

// prepareConfig prepares a config with the required fields.
func prepareConfig(t *testing.T, cfg *source.Config) {
	t.Helper()

	uri := os.Getenv(testEnvNameURI)
	if uri == "" {
		t.Skipf("%s env var must be set", testEnvNameURI)
	}

	cfgMap := map[string]string{
		"uri":                                uri,
		"db":                                 testDB,
		"collection":                         fmt.Sprintf("%s_%d", testCollectionPrefix, time.Now().UnixNano()),
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
	err := sdk.Util.ParseConfig(t.Context(), cfgMap, cfg, mongoConn.Connector.NewSpecification().SourceParams)
	if err != nil {
		t.Logf("parse configuration error: %v", err)
	}

	err = cfg.Validate(t.Context())
	if err != nil {
		t.Logf("config validation error: %v", err)
	}
}

// createTestMongoClient connects to a MongoDB by a provided URI.
func createTestMongoClient(ctx context.Context, uri string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(uri).SetRegistry(newBSONCodecRegistry())

	mongoClient, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	return mongoClient, nil
}

func newBSONCodecRegistry() *bsoncodec.Registry {
	registry := bson.NewRegistry()
	registry.RegisterKindEncoder(reflect.String, codec.StringObjectIDCodec{})

	return registry
}

// createTestItem writes a random item to a collection and returns it.
func createTestItem(ctx context.Context, collection *mongo.Collection) (opencdc.StructuredData, error) {
	testItem := map[string]any{
		"email":     gofakeit.Email(),
		"firstName": gofakeit.FirstName(),
		"lastName":  gofakeit.LastName(),
	}

	insertOneResult, err := collection.InsertOne(ctx, testItem)
	if err != nil {
		return nil, fmt.Errorf("insert one: %w", err)
	}

	id, ok := insertOneResult.InsertedID.(primitive.ObjectID)
	if !ok {
		return nil, fmt.Errorf("expected ID to be an ObjectID, but got %T", insertOneResult.InsertedID)
	}
	testItem["_id"] = id.Hex()

	return testItem, nil
}

// updateTestItem updates a provided test item
// with a random email and firstName.
func updateTestItem(
	ctx context.Context,
	collection *mongo.Collection,
	testItem opencdc.StructuredData,
) (opencdc.StructuredData, error) {
	newEmail := gofakeit.Email()
	newFirstName := gofakeit.FirstName()

	// copy the testItem into the new updatedTestItem,
	// in order not to modify the original testItem
	updatedTestItem := make(opencdc.StructuredData)
	for key, value := range testItem {
		updatedTestItem[key] = value
	}

	updatedTestItemID, ok := updatedTestItem["_id"].(string)
	if !ok {
		return nil, errors.New("cannot convert _id to string")
	}

	parsedTestItemID, err := primitive.ObjectIDFromHex(updatedTestItemID)
	if err != nil {
		return nil, fmt.Errorf("object id from hex: %w", err)
	}

	_, err = collection.UpdateOne(ctx,
		bson.M{"_id": parsedTestItemID},
		bson.M{"$set": bson.M{"email": newEmail, "firstName": newFirstName}},
	)
	if err != nil {
		return nil, fmt.Errorf("update one: %w", err)
	}

	// set the updated fields to the updatedTestItem in order
	// to compare this with a resulting record payload
	updatedTestItem["email"] = newEmail
	updatedTestItem["firstName"] = newFirstName

	return updatedTestItem, nil
}

// deleteTestItem deletes a test item by a provided id.
func deleteTestItem(
	ctx context.Context,
	collection *mongo.Collection,
	testItem opencdc.StructuredData,
) error {
	testItemID, ok := testItem["_id"].(string)
	if !ok {
		return errors.New("cannot convert _id to string")
	}

	parsedTestItemID, err := primitive.ObjectIDFromHex(testItemID)
	if err != nil {
		return fmt.Errorf("object id from hex: %w", err)
	}

	_, err = collection.DeleteOne(ctx, bson.M{"_id": parsedTestItemID})
	if err != nil {
		return fmt.Errorf("delete one: %w", err)
	}

	return nil
}
