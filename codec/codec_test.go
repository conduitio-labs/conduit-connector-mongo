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

package codec

import (
	"errors"
	"reflect"
	"testing"

	"github.com/brianvoe/gofakeit"
	"github.com/matryer/is"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw/bsonrwtest"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// valueReaderWriter implements the [bsonrw.ValueWriter]
// and overrides its WriteString and WriteObjectID methods
// to use them within the StringObjectIDCodec.EncodeValue method.
type valueReaderWriter struct {
	bsonrwtest.ValueReaderWriter

	value any
}

func newValueReaderWriter() *valueReaderWriter {
	return &valueReaderWriter{}
}

func (vrw *valueReaderWriter) WriteString(s string) error {
	vrw.value = s

	return nil
}

func (vrw *valueReaderWriter) WriteObjectID(oid primitive.ObjectID) error {
	vrw.value = oid

	return nil
}

func TestStringObjectIDCodec_EncodeValue_ValidObjectID(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	codec := StringObjectIDCodec{}

	// create a valid primitive.ObjectID and its hex string representation
	hexString := primitive.NewObjectID().Hex()
	expectedObjectID, _ := primitive.ObjectIDFromHex(hexString)

	// setup a test valueReaderWriter
	vw := newValueReaderWriter()

	// execute the EncodeValue with a hex string representation of a valid primitive.ObjectID,
	// we expect to get it back as a primitive.ObjectID
	err := codec.EncodeValue(bsoncodec.EncodeContext{}, vw, reflect.ValueOf(hexString))
	is.NoErr(err)
	is.Equal(vw.value, expectedObjectID)
}

func TestStringObjectIDCodec_EncodeValue_RandomString(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	codec := StringObjectIDCodec{}

	// setup a test valueReaderWriter
	vw := newValueReaderWriter()

	// generate a random string
	randomString := gofakeit.Zip()

	// execute the EncodeValue with the random string, we expect to get it back as a string
	err := codec.EncodeValue(bsoncodec.EncodeContext{}, vw, reflect.ValueOf(randomString))
	is.NoErr(err)
	is.Equal(vw.value, randomString)
}

func TestStringObjectIDCodec_EncodeValue_InvalidParameter(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	codec := StringObjectIDCodec{}

	// setup a test valueReaderWriter
	vw := newValueReaderWriter()

	// generate a random integer
	randomInt := gofakeit.Int8()

	// execute the EncodeValue with the random integer, we expect to get a ValueEncoderError
	err := codec.EncodeValue(bsoncodec.EncodeContext{}, vw, reflect.ValueOf(randomInt))
	is.True(err != nil)
	var valueEncoderError bsoncodec.ValueEncoderError
	is.True(errors.As(err, &valueEncoderError))
	is.Equal(valueEncoderError.Name, "StringEncodeValue")
}
