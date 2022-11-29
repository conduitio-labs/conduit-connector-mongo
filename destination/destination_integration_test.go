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
	"testing"
	"time"

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
	testURI              = "mongodb://localhost:27017/?directConnection=true"
	testDB               = "test"
	testCollectionPrefix = "test_coll"

	// next consts will be used for test models.
	testIDFieldName = "_id"
	testID          = "6384c3e48740eb54f858bde5"

	testTextFieldName = "textfield"
	testTextData      = "some text data"

	testNumberFieldName  = "numberfield"
	testNumberData       = 1234
	testNumberDataChange = 5678
)

func TestDestination_Write_snapshotSuccess(t *testing.T) {
	is := is.New(t)

	cfg := prepareConfig(t)

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, cfg)
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	n, err := destination.Write(ctx, []sdk.Record{getTestSnapshot(t)})
	is.NoErr(err)
	is.Equal(n, 1)

	c, err := col.CountDocuments(ctx, bson.D{})
	is.NoErr(err)
	is.Equal(c, int64(1))

	res, err := col.Find(ctx, bson.M{})
	res.Next(ctx)
	var result map[string]any
	err = res.Decode(&result)
	is.NoErr(err)

	id, ok := result[testIDFieldName].(primitive.ObjectID)
	is.Equal(ok, true)

	hex, err := primitive.ObjectIDFromHex(testID)
	is.NoErr(err)

	is.Equal(id, hex)

	text, ok := result[testTextFieldName].(string)
	is.Equal(ok, true)
	is.Equal(text, testTextData)

	num, ok := result[testNumberFieldName].(float64)
	is.Equal(ok, true)
	is.Equal(int(num), testNumberData)

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

	err = destination.Open(ctx)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	n, err := destination.Write(ctx, []sdk.Record{getTestCreateRecord(t)})
	is.NoErr(err)
	is.Equal(n, 1)

	c, err := col.CountDocuments(ctx, bson.D{})
	is.NoErr(err)
	is.Equal(c, int64(1))

	res, err := col.Find(ctx, bson.M{})
	res.Next(ctx)
	var result map[string]any
	err = res.Decode(&result)
	is.NoErr(err)

	id, ok := result[testIDFieldName].(primitive.ObjectID)
	is.Equal(ok, true)

	hex, err := primitive.ObjectIDFromHex(testID)
	is.NoErr(err)

	is.Equal(id, hex)

	text, ok := result[testTextFieldName].(string)
	is.Equal(ok, true)
	is.Equal(text, testTextData)

	num, ok := result[testNumberFieldName].(float64)
	is.Equal(ok, true)
	is.Equal(int(num), testNumberData)

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

	err = destination.Open(ctx)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	n, err := destination.Write(ctx, []sdk.Record{getTestCreateRecord(t)})
	is.NoErr(err)
	is.Equal(n, 1)

	n, err = destination.Write(ctx, []sdk.Record{getTestUpdateRecord(t)})
	is.NoErr(err)
	is.Equal(n, 1)

	res, err := col.Find(ctx, bson.M{})
	res.Next(ctx)
	var result map[string]any
	err = res.Decode(&result)
	is.NoErr(err)

	id, ok := result[testIDFieldName].(primitive.ObjectID)
	is.Equal(ok, true)

	hex, err := primitive.ObjectIDFromHex(testID)
	is.NoErr(err)

	is.Equal(id, hex)

	text, ok := result[testTextFieldName].(string)
	is.Equal(ok, true)
	is.Equal(text, testTextData)

	num, ok := result[testNumberFieldName].(float64)
	is.Equal(ok, true)
	is.Equal(int(num), testNumberDataChange)

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

	err = destination.Open(ctx)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	n, err := destination.Write(ctx, []sdk.Record{getTestCreateRecord(t)})
	is.NoErr(err)
	is.Equal(n, 1)

	_, err = destination.Write(ctx, []sdk.Record{getTestUpdateRecordNoKeys(t)})
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

	err = destination.Open(ctx)
	is.NoErr(err)

	col, err := getTestCollection(ctx, cfg[config.KeyCollection])
	is.NoErr(err)

	t.Cleanup(func() {
		err = destination.Teardown(ctx)
		is.NoErr(err)
	})

	n, err := destination.Write(ctx, []sdk.Record{getTestCreateRecord(t)})
	is.NoErr(err)
	is.Equal(n, 1)

	n, err = destination.Write(ctx, []sdk.Record{getTestDeleteRecord(t)})
	is.NoErr(err)
	is.Equal(n, 1)

	res, err := col.Find(ctx, bson.M{})
	is.NoErr(err)
	is.True(!res.Next(ctx))

	_, err = col.DeleteMany(ctx, bson.M{})
	is.NoErr(err)
}

func getTestCollection(ctx context.Context, collection string) (*mongo.Collection, error) {
	conn, err := mongo.Connect(ctx, options.Client().ApplyURI(testURI))
	if err != nil {
		return nil, fmt.Errorf("connect to mongo: %w", err)
	}

	return conn.Database(testDB).Collection(collection), nil
}

func getTestCreateRecord(t *testing.T) sdk.Record {
	t.Helper()

	return sdk.Util.Source.NewRecordCreate(
		nil, nil,
		// in insert keys are not used, so we can omit it
		nil,
		sdk.StructuredData{
			testIDFieldName:     testID, // we put it as string here, codec will translate it into ObjectID
			testTextFieldName:   testTextData,
			testNumberFieldName: testNumberData,
		},
	)
}

func getTestSnapshot(t *testing.T) sdk.Record {
	t.Helper()

	return sdk.Util.Source.NewRecordSnapshot(
		nil, nil,
		// in insert keys are not used, so we can omit it
		nil,
		sdk.StructuredData{
			testIDFieldName:     testID, // we put it as string here, codec will translate it into ObjectID
			testTextFieldName:   testTextData,
			testNumberFieldName: testNumberData,
		},
	)
}

func getTestUpdateRecord(t *testing.T) sdk.Record {
	t.Helper()

	return sdk.Util.Source.NewRecordUpdate(
		nil, nil,
		sdk.StructuredData{testIDFieldName: testID},
		sdk.StructuredData{}, // in update we are not using this field, so we can omit it
		sdk.StructuredData{testNumberFieldName: testNumberDataChange},
	)
}

func getTestUpdateRecordNoKeys(t *testing.T) sdk.Record {
	t.Helper()

	return sdk.Util.Source.NewRecordUpdate(
		nil, nil,
		sdk.StructuredData{},
		sdk.StructuredData{}, // in update we are not using this field, so we can omit it
		sdk.StructuredData{testNumberFieldName: testNumberDataChange},
	)
}

func getTestDeleteRecord(t *testing.T) sdk.Record {
	t.Helper()

	return sdk.Util.Source.NewRecordDelete(
		nil, nil,
		sdk.StructuredData{testIDFieldName: testID},
	)
}

func prepareConfig(t *testing.T) map[string]string {
	t.Helper()

	return map[string]string{
		config.KeyURI:        testURI,
		config.KeyDB:         testDB,
		config.KeyCollection: fmt.Sprintf("%s_%d", testCollectionPrefix, time.Now().UnixNano()),
	}
}
