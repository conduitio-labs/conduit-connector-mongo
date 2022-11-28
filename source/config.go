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

package source

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/conduitio-labs/conduit-connector-mongo/config"
	"github.com/conduitio-labs/conduit-connector-mongo/validator"
)

const (
	// defaultBatchSize is the default value for the batchSize field.
	defaultBatchSize = 1000
	// defaultSnapshotMode is the default value for the snapshotMode field.
	defaultSnapshotMode = SnapshotModeInitial
	// defaultOrderingColumn is the default value for the orderingColumn field.
	defaultOrderingColumn = "_id"
)

const (
	// ConfigKeyBatchSize is a config name for a batch size.
	ConfigKeyBatchSize = "batchSize"
	// ConfigKeySnapshotMode is a config name for a snapshotMode field.
	ConfigKeySnapshotMode = "snapshotMode"
	// ConfigKeyOrderingColumn is a config name for a orderingColumn field.
	ConfigKeyOrderingColumn = "orderingColumn"
)

// SnapshotMode defines a snapshot mode.
type SnapshotMode string

// The available snapshot modes are listed below.
const (
	SnapshotModeInitial SnapshotMode = "initial"
	SnapshotModeNever   SnapshotMode = "never"
)

// IsValid checks if the underlying [SnapshotMode] is valid.
func (sm SnapshotMode) IsValid() bool {
	switch sm {
	case SnapshotModeInitial, SnapshotModeNever:
		return true
	}

	return false
}

// Config contains source-specific configurable values.
type Config struct {
	config.Config

	// BatchSize is the size of a document batch.
	BatchSize int `key:"batchSize" validate:"gte=1,lte=100000"`
	// SnapshotMode determines whether or not the connector will take a snapshot
	// of the entire collection before starting CDC mode.
	SnapshotMode SnapshotMode `key:"snapshotMode"`
	// OrderingColumn is the name of a field that is used for ordering
	// collection elements when capturing a snapshot.
	OrderingColumn string `key:"orderingColumn"`
}

// ParseConfig maps the incoming map to the [Config] and validates it.
func ParseConfig(raw map[string]string) (Config, error) {
	commonConfig, err := config.Parse(raw)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	sourceConfig := Config{
		Config:         commonConfig,
		BatchSize:      defaultBatchSize,
		SnapshotMode:   defaultSnapshotMode,
		OrderingColumn: defaultOrderingColumn,
	}

	// parse batch size if it's not empty
	if batchSizeStr := raw[ConfigKeyBatchSize]; batchSizeStr != "" {
		batchSize, err := strconv.Atoi(batchSizeStr)
		if err != nil {
			return Config{}, fmt.Errorf("parse %q: %w", ConfigKeyBatchSize, err)
		}

		sourceConfig.BatchSize = batchSize
	}

	// parse snapshotMode if it's not empty
	if raw[ConfigKeySnapshotMode] != "" {
		snapshotMode := SnapshotMode(strings.ToLower(raw[ConfigKeySnapshotMode]))

		if !snapshotMode.IsValid() {
			return Config{}, fmt.Errorf("invalid snapshot mode %q", snapshotMode)
		}

		sourceConfig.SnapshotMode = snapshotMode
	}

	// set the orderingColumn if it's not empty
	if orderingColumn := raw[ConfigKeyOrderingColumn]; orderingColumn != "" {
		sourceConfig.OrderingColumn = orderingColumn
	}

	if err := validator.ValidateStruct(&sourceConfig); err != nil {
		return Config{}, fmt.Errorf("validate source config: %w", err)
	}

	return sourceConfig, nil
}
