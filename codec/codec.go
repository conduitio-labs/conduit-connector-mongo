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

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// StringObjectIDCodec is the Codec used for string values.
// It tries to convert strings into [bson.ObjectID].
type StringObjectIDCodec struct{}

// EncodeValue implements the [bsoncodec.ValueEncoder] interface.
// The method tries to convert a string value into [bson.ObjectID].
//
//   - If a string can be converted into [bson.ObjectID] it returns it as a [bson.ObjectID].
//   - If a string cannot be converted into [bson.ObjectID] it returns it as a string, without any transformations.
func (sc StringObjectIDCodec) EncodeValue(
	_ bsoncodec.EncodeContext,
	vw bsonrw.ValueWriter,
	val reflect.Value,
) error {
	if val.Kind() != reflect.String {
		return bsoncodec.ValueEncoderError{
			Name:     "StringEncodeValue",
			Kinds:    []reflect.Kind{reflect.String},
			Received: val,
		}
	}

	objectID, err := primitive.ObjectIDFromHex(val.String())
	if err != nil {
		return vw.WriteString(val.String())
	}

	return vw.WriteObjectID(objectID)
}
