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

// Package iterator implements the CDC and Snapshot iterators for MongoDB.
// Working with them is carried out through a combined iterator.
package iterator

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/mongo"
)

// metadataFieldCollection is a name of a record metadata field that stores a MongoDB collection name.
const metadataFieldCollection = "mongo.collection"

// Combined is a combined iterator for MongoDB.
// It consists of the cdc and snapshot iterators.
// A snapshot is captured only if the snapshot is set to true.
type Combined struct {
	snapshot   *snapshot
	cdc        *cdc
	collection *mongo.Collection
}

// CombinedParams is an incoming params for the [NewCombined] function.
type CombinedParams struct {
	Collection     *mongo.Collection
	BatchSize      int
	Snapshot       bool
	OrderingColumn string
	SDKPosition    sdk.Position
}

// NewCombined creates a new instance of the [Combined].
func NewCombined(ctx context.Context, params CombinedParams) (*Combined, error) {
	combined := &Combined{
		collection: params.Collection,
	}

	position, err := parsePosition(params.SDKPosition)
	if err != nil && !errors.Is(err, errNilSDKPosition) {
		return nil, fmt.Errorf("parse sdk position: %w", err)
	}

	switch pos := position; {
	case params.Snapshot && (pos == nil || pos.Mode == modeSnapshot):
		combined.snapshot = newSnapshot(params.Collection, params.OrderingColumn, params.BatchSize, pos)

	case !params.Snapshot || (pos != nil && pos.Mode == modeCDC):
		combined.cdc, err = newCDC(ctx, params.Collection, pos)
		if err != nil {
			return nil, fmt.Errorf("init cdc iterator: %w", err)
		}

	default:
		return nil, fmt.Errorf("invalid position mode %q", pos.Mode)
	}

	return combined, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
// If the underlying snapshot iterator returns false, the combined iterator will try to switch to the cdc iterator.
func (c *Combined) HasNext(ctx context.Context) (bool, error) {
	switch {
	case c.snapshot != nil:
		hasNext, err := c.snapshot.hasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot has next: %w", err)
		}

		if !hasNext {
			if err := c.switchToCDC(ctx); err != nil {
				return false, fmt.Errorf("switch to cdc mode: %w", err)
			}

			return c.cdc.hasNext(ctx)
		}

		return true, nil

	case c.cdc != nil:
		return c.cdc.hasNext(ctx)

	default:
		// this shouldn't happen
		return false, ErrNoIterator
	}
}

// Next returns the next record.
func (c *Combined) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case c.snapshot != nil:
		return c.snapshot.next(ctx)

	case c.cdc != nil:
		return c.cdc.next(ctx)

	default:
		// this shouldn't happen
		return sdk.Record{}, ErrNoIterator
	}
}

// Stop stops the underlying iterators.
func (c *Combined) Stop(ctx context.Context) error {
	if c.snapshot != nil {
		if err := c.snapshot.stop(ctx); err != nil {
			return fmt.Errorf("stop snapshot: %w", err)
		}
	}

	if c.cdc != nil {
		if err := c.cdc.stop(ctx); err != nil {
			return fmt.Errorf("stop cdc: %w", err)
		}
	}

	return nil
}

// switchToCDCIterator initializes the cdc iterator, and set the snapshot to nil.
func (c *Combined) switchToCDC(ctx context.Context) error {
	// start CDC with nil position in order to listen to new changes
	cdc, err := newCDC(ctx, c.collection, nil)
	if err != nil {
		return fmt.Errorf("init cdc iterator: %w", err)
	}

	c.cdc = cdc

	if err := c.snapshot.stop(ctx); err != nil {
		return fmt.Errorf("stop snapshot iterator: %w", err)
	}

	c.snapshot = nil

	return nil
}