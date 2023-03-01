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
	"reflect"
	"testing"

	"github.com/matryer/is"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw/bsonrwtest"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestStringObjectIDCodec_EncodeValue(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	expectedObjectID := primitive.NewObjectID()

	vrw := &bsonrwtest.ValueReaderWriter{BSONType: bsontype.ObjectID, Return: expectedObjectID}

	err := StringObjectIDCodec{}.EncodeValue(
		bsoncodec.EncodeContext{},
		vrw,
		reflect.ValueOf(expectedObjectID.Hex()),
	)
	is.NoErr(err)

	actualObjectID, err := vrw.ReadObjectID()
	is.NoErr(err)
	is.Equal(expectedObjectID, actualObjectID)
}
