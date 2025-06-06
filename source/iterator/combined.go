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

// Package iterator implements the CDC and Snapshot iterators for MongoDB.
// Working with them is carried out through a combined iterator.
package iterator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/conduitio/conduit-commons/opencdc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Combined is a combined iterator for MongoDB.
// It consists of the cdc and snapshot iterators.
// A snapshot is captured only if the snapshot is set to true.
type Combined struct {
	snapshot *snapshot
	// pollingSnapshot is used when CDC is not available.
	// It supports insert operations only.
	pollingSnapshot *snapshot
	cdc             *cdc
}

// CombinedParams is an incoming params for the [NewCombined] function.
type CombinedParams struct {
	Collection    *mongo.Collection
	BatchSize     int
	Snapshot      bool
	OrderingField string
	SDKPosition   opencdc.Position
}

// NewCombined creates a new instance of the [Combined].
func NewCombined(ctx context.Context, params CombinedParams) (*Combined, error) {
	combined := &Combined{}

	position, err := parsePosition(params.SDKPosition)
	if err != nil && !errors.Is(err, errNilSDKPosition) {
		return nil, fmt.Errorf("parse sdk position: %w", err)
	}

	// create the CDC iterator in any case in order to properly
	// switch after the snapshot and start consuming events starting from the current time
	combined.cdc, err = newCDC(ctx, params.Collection, position)
	if err != nil {
		if !strings.Contains(err.Error(), matchProjectStageErrMessage) {
			return nil, fmt.Errorf("init cdc iterator: %w", err)
		}

		combined.pollingSnapshot, err = newPollingSnapshot(ctx, snapshotParams{
			collection:    params.Collection,
			orderingField: params.OrderingField,
			batchSize:     params.BatchSize,
			position:      position,
		})
		if err != nil {
			return nil, fmt.Errorf("init polling snapshot: %w", err)
		}
	}

	// initialize the object only if the user has determined that it is required
	// and if there is no position or the position mode is a snapshot
	if params.Snapshot && (position == nil || position.Mode == modeSnapshot) {
		var resumeToken bson.Raw
		if combined.cdc != nil {
			resumeToken = combined.cdc.changeStream.ResumeToken()
		}

		combined.snapshot, err = newSnapshot(ctx, snapshotParams{
			collection:    params.Collection,
			orderingField: params.OrderingField,
			batchSize:     params.BatchSize,
			position:      position,
			resumeToken:   resumeToken,
		})
		if err != nil {
			return nil, fmt.Errorf("init snapshot iterator: %w", err)
		}
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
			if err := c.snapshot.stop(ctx); err != nil {
				return false, fmt.Errorf("stop snapshot iterator: %w", err)
			}
			c.snapshot = nil

			if c.pollingSnapshot != nil {
				return c.pollingSnapshot.hasNext(ctx)
			}

			return c.cdc.hasNext(ctx)
		}

		return true, nil

	case c.pollingSnapshot != nil:
		return c.pollingSnapshot.hasNext(ctx)

	case c.cdc != nil:
		return c.cdc.hasNext(ctx)

	default:
		// this shouldn't happen
		return false, ErrNoIterator
	}
}

// Next returns the next record.
func (c *Combined) Next(ctx context.Context) (opencdc.Record, error) {
	switch {
	case c.snapshot != nil:
		return c.snapshot.next(ctx)

	case c.pollingSnapshot != nil:
		return c.pollingSnapshot.next(ctx)

	case c.cdc != nil:
		return c.cdc.next(ctx)

	default:
		// this shouldn't happen
		return opencdc.Record{}, ErrNoIterator
	}
}

// Stop stops the underlying iterators.
func (c *Combined) Stop(ctx context.Context) error {
	if c.snapshot != nil {
		if err := c.snapshot.stop(ctx); err != nil {
			return fmt.Errorf("stop snapshot: %w", err)
		}
	}

	if c.pollingSnapshot != nil {
		if err := c.pollingSnapshot.stop(ctx); err != nil {
			return fmt.Errorf("stop polling snapshot: %w", err)
		}
	}

	if c.cdc != nil {
		if err := c.cdc.stop(ctx); err != nil {
			return fmt.Errorf("stop cdc: %w", err)
		}
	}

	return nil
}
