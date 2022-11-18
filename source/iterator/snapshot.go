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

package iterator

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/mongo"
)

// Snapshot is a snapshot iterator for the MongoDB source connector.
type Snapshot struct {
	collection *mongo.Collection
}

// NewSnapshot creates a new instance of the [Snapshot] iterator.
func NewSnapshot(collection *mongo.Collection) *Snapshot {
	return &Snapshot{
		collection: collection,
	}
}

// HasNext checks whether the snapshot iterator has records to return or not.
func (s *Snapshot) HasNext(ctx context.Context) (bool, error) {
	return false, nil
}

// Next returns the next record.
func (s *Snapshot) Next(ctx context.Context) (sdk.Record, error) {
	return sdk.Record{}, nil
}

// Stop stops the iterator.
func (s *Snapshot) Stop(ctx context.Context) error {
	return nil
}
