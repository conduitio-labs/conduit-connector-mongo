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

package source

import (
	"context"
	"errors"

	"github.com/conduitio-labs/conduit-connector-mongo/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Config contains source-specific configurable values.
type Config struct {
	sdk.DefaultSourceMiddleware
	config.Config

	// BatchSize is the size of a document batch.
	BatchSize int `json:"batchSize" default:"1000" validate:"gt=0,lt=100000"`
	// Snapshot determines whether the connector will take a snapshot
	// of the entire collection before starting CDC mode.
	Snapshot bool `json:"snapshot" default:"true"`
	// OrderingField is the name of a field that is used for ordering
	// collection documents when capturing a snapshot.
	OrderingField string `json:"orderingField" default:"_id"`
}

func (c *Config) Validate(ctx context.Context) error {
	var errs []error
	if err := c.Config.Validate(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := c.DefaultSourceMiddleware.Validate(ctx); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
