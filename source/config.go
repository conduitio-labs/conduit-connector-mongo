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
	"fmt"
	"strconv"

	"github.com/conduitio-labs/conduit-connector-mongo/config"
	"github.com/conduitio-labs/conduit-connector-mongo/validator"
)

const (
	// defaultBatchSize is the default value for the batchSize field.
	defaultBatchSize = 1000
	// defaultSnapshot is the default value for the snapshot field.
	defaultSnapshot = true
	// defaultOrderingField is the default value for the orderingField field.
	defaultOrderingField = "_id"
)

const (
	// ConfigKeyBatchSize is a config name for a batch size.
	ConfigKeyBatchSize = "batchSize"
	// ConfigKeySnapshot is a config name for a snapshot field.
	ConfigKeySnapshot = "snapshot"
	// ConfigKeyOrderingField is a config name for a orderingField field.
	ConfigKeyOrderingField = "orderingField"
)

// Config contains source-specific configurable values.
type Config struct {
	config.Config

	// BatchSize is the size of a document batch.
	BatchSize int `key:"batchSize" validate:"gte=1,lte=100000"`
	// Snapshot determines whether or not the connector will take a snapshot
	// of the entire collection before starting CDC mode.
	Snapshot bool `key:"snapshot"`
	// OrderingField is the name of a field that is used for ordering
	// collection documents when capturing a snapshot.
	OrderingField string `key:"orderingField"`
}

// ParseConfig maps the incoming map to the [Config] and validates it.
func ParseConfig(raw map[string]string) (Config, error) {
	commonConfig, err := config.Parse(raw)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	sourceConfig := Config{
		Config:        commonConfig,
		BatchSize:     defaultBatchSize,
		Snapshot:      defaultSnapshot,
		OrderingField: defaultOrderingField,
	}

	// parse batch size if it's not empty
	if batchSizeStr := raw[ConfigKeyBatchSize]; batchSizeStr != "" {
		batchSize, err := strconv.Atoi(batchSizeStr)
		if err != nil {
			return Config{}, fmt.Errorf("parse %q: %w", ConfigKeyBatchSize, err)
		}

		sourceConfig.BatchSize = batchSize
	}

	// parse snapshot if it's not empty
	if snapshotStr := raw[ConfigKeySnapshot]; snapshotStr != "" {
		snapshot, err := strconv.ParseBool(snapshotStr)
		if err != nil {
			return Config{}, fmt.Errorf("parse %q: %w", ConfigKeySnapshot, err)
		}

		sourceConfig.Snapshot = snapshot
	}

	// set the orderingField if it's not empty
	if orderingField := raw[ConfigKeyOrderingField]; orderingField != "" {
		sourceConfig.OrderingField = orderingField
	}

	if err := validator.ValidateStruct(&sourceConfig); err != nil {
		return Config{}, fmt.Errorf("validate source config: %w", err)
	}

	return sourceConfig, nil
}
