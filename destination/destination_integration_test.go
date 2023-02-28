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

package destination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/conduitio-labs/conduit-connector-mongo/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/conduitio-labs/conduit-connector-mongo/config"
	"github.com/conduitio-labs/conduit-connector-mongo/destination/writer"
)

const (
	// set the directConnection to true in order to avoid the known hostname problem.
	testDB               = "test_destination"
	testCollectionPrefix = "test_coll"

	// next consts will be used for test models as field names.
	testIDFieldName    = "_id"
	testEmailFieldName = "email"
	testNameFieldName  = "name"
)

func TestDestination_Write_snapshotSuccess(t *testing.T) {
	is := is.New(t)

	cfg := prepareConfig(t)

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, cfg)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyURI], cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = col.Drop(context.Background())
		is.NoErr(err)

		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	err = destination.Open(ctx)
	is.NoErr(err)

	testItem := createTestItem(t)

	n, err := destination.Write(ctx, []sdk.Record{sdk.Util.Source.NewRecordSnapshot(
		nil, nil,
		// in insert keys are not used, so we can omit it
		nil,
		sdk.StructuredData(testItem),
	)})
	is.NoErr(err)
	is.Equal(n, 1)

	compareTestPayload(ctx, t, is, col, testItem)

	_, err = col.DeleteMany(ctx, bson.M{})
	is.NoErr(err)
}

func TestDestination_Write_insertSuccess(t *testing.T) {
	is := is.New(t)

	cfg := prepareConfig(t)

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, cfg)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyURI], cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = col.Drop(context.Background())
		is.NoErr(err)

		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	err = destination.Open(ctx)
	is.NoErr(err)

	testItem := createTestItem(t)

	n, err := destination.Write(ctx,
		[]sdk.Record{sdk.Util.Source.NewRecordCreate(
			nil,
			nil,
			nil,
			sdk.StructuredData(testItem))})
	is.NoErr(err)
	is.Equal(n, 1)

	compareTestPayload(ctx, t, is, col, testItem)

	_, err = col.DeleteMany(ctx, bson.M{})
	is.NoErr(err)
}

func TestDestination_Write_updateSuccess(t *testing.T) {
	is := is.New(t)

	cfg := prepareConfig(t)

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, cfg)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyURI], cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = col.Drop(context.Background())
		is.NoErr(err)

		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	err = destination.Open(ctx)
	is.NoErr(err)

	testItem := createTestItem(t)

	n, err := destination.Write(ctx, []sdk.Record{sdk.Util.Source.NewRecordCreate(
		nil,
		nil,
		nil,
		sdk.StructuredData(testItem))})
	is.NoErr(err)
	is.Equal(n, 1)

	testItem[testNameFieldName] = gofakeit.LastName()
	n, err = destination.Write(ctx, []sdk.Record{sdk.Util.Source.NewRecordUpdate(
		nil, nil,
		sdk.StructuredData{testIDFieldName: testItem[testIDFieldName]},
		sdk.StructuredData{}, // in update we are not using this field, so we can omit it
		sdk.StructuredData{testNameFieldName: testItem[testNameFieldName]},
	)})
	is.NoErr(err)
	is.Equal(n, 1)

	compareTestPayload(ctx, t, is, col, testItem)

	_, err = col.DeleteMany(ctx, bson.M{})
	is.NoErr(err)
}

func TestDestination_Write_updateFailureNoKeys(t *testing.T) {
	is := is.New(t)

	cfg := prepareConfig(t)

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, cfg)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyURI], cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = col.Drop(context.Background())
		is.NoErr(err)

		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	err = destination.Open(ctx)
	is.NoErr(err)

	testItem := createTestItem(t)

	n, err := destination.Write(ctx, []sdk.Record{sdk.Util.Source.NewRecordCreate(
		nil,
		nil,
		nil,
		sdk.StructuredData(testItem))})
	is.NoErr(err)
	is.Equal(n, 1)

	_, err = destination.Write(ctx, []sdk.Record{sdk.Util.Source.NewRecordUpdate(
		nil, nil,
		sdk.StructuredData{},
		sdk.StructuredData{}, // in update we are not using this field, so we can omit it
		sdk.StructuredData{testNameFieldName: gofakeit.LastName()},
	)})
	is.True(errors.Is(err, writer.ErrEmptyKey))

	_, err = col.DeleteMany(ctx, bson.M{})
	is.NoErr(err)
}

func TestDestination_Write_deleteSuccess(t *testing.T) {
	is := is.New(t)

	cfg := prepareConfig(t)

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, cfg)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyURI], cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = col.Drop(context.Background())
		is.NoErr(err)

		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	err = destination.Open(ctx)
	is.NoErr(err)

	testItem := createTestItem(t)

	n, err := destination.Write(ctx, []sdk.Record{sdk.Util.Source.NewRecordCreate(
		nil,
		nil,
		nil,
		sdk.StructuredData(testItem))})
	is.NoErr(err)
	is.Equal(n, 1)

	n, err = destination.Write(ctx, []sdk.Record{sdk.Util.Source.NewRecordDelete(
		nil, nil,
		sdk.StructuredData{testIDFieldName: testItem[testIDFieldName]},
	)})
	is.NoErr(err)
	is.Equal(n, 1)

	res, err := col.Find(ctx, bson.M{})
	is.NoErr(err)
	is.True(!res.Next(ctx))

	_, err = col.DeleteMany(ctx, bson.M{})
	is.NoErr(err)
}

func compareTestPayload(
	ctx context.Context,
	t *testing.T,
	is *is.I,
	col *mongo.Collection,
	testRecordPayload sdk.StructuredData,
) {
	t.Helper()

	c, err := col.CountDocuments(ctx, bson.D{})
	is.NoErr(err)
	is.Equal(c, int64(1))

	res, err := col.Find(ctx, bson.M{})
	is.NoErr(err)
	is.True(res.TryNext(ctx))
	var result map[string]any
	err = res.Decode(&result)
	is.NoErr(err)

	is.Equal(sdk.StructuredData(result), testRecordPayload)
}

func getTestCollection(ctx context.Context, uri, collection string) (*mongo.Collection, error) {
	conn, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connect to mongo: %w", err)
	}

	database := conn.Database(testDB)
	if err = database.CreateCollection(ctx, collection); err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	return database.Collection(collection), nil
}

func createTestItem(t *testing.T) map[string]any {
	t.Helper()

	return map[string]any{
		// testIDFieldName is declared as string for testing codec
		testIDFieldName:    primitive.NewObjectIDFromTimestamp(time.Now()).String(),
		testEmailFieldName: gofakeit.Email(),
		testNameFieldName:  gofakeit.Name(),
	}
}

func prepareConfig(t *testing.T) map[string]string {
	t.Helper()

	uri := os.Getenv(common.TestEnvNameURI)
	if uri == "" {
		t.Skipf("%s env var must be set", common.TestEnvNameURI)
	}

	return map[string]string{
		config.KeyURI:        uri,
		config.KeyDB:         testDB,
		config.KeyCollection: fmt.Sprintf("%s_%d", testCollectionPrefix, time.Now().UnixNano()),
	}
}
