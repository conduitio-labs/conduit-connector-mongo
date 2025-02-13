// Copyright © 2025 Meroxa, Inc. & Yalantis
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

	"github.com/conduitio-labs/conduit-connector-mongo/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Config struct {
	sdk.DefaultDestinationMiddleware
	config.Config
}

func (c *Config) Validate(ctx context.Context) error {
	var errs []error
	if err := c.Config.Validate(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := c.DefaultDestinationMiddleware.Validate(ctx); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
