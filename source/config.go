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

	"github.com/conduitio-labs/conduit-connector-mongo/config"
	"github.com/conduitio-labs/conduit-connector-mongo/validator"
)

const (
	// defaultBatchSize is the default value for the batchSize field.
	defaultBatchSize = 1000
	// defaultCopyExistingData is the default value for the copyExistingData field.
	defaultCopyExistingData = true
	// defaultOrderingColumn is the default value for the orderingColumn field.
	defaultOrderingColumn = "_id"
)

const (
	// ConfigKeyBatchSize is a config name for a batch size.
	ConfigKeyBatchSize = "batchSize"
	// ConfigKeyCopyExistingData is a config name for a copyExistingData field.
	ConfigKeyCopyExistingData = "copyExistingData"
	// ConfigKeyOrderingColumn is a config name for a orderingColumn field.
	ConfigKeyOrderingColumn = "orderingColumn"
)

// Config contains source-specific configurable values.
type Config struct {
	config.Config

	// BatchSize is the size of a document batch.
	BatchSize int `key:"batchSize" validate:"gte=1,lte=100000"`
	// CopyExistingData determines whether or not the connector will take a snapshot
	// of the entire collection before starting CDC mode.
	CopyExistingData bool `key:"copyExistingData"`
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
		Config:           commonConfig,
		BatchSize:        defaultBatchSize,
		CopyExistingData: defaultCopyExistingData,
		OrderingColumn:   defaultOrderingColumn,
	}

	// parse batch size if it's not empty
	if batchSizeStr := raw[ConfigKeyBatchSize]; batchSizeStr != "" {
		batchSize, err := strconv.Atoi(batchSizeStr)
		if err != nil {
			return Config{}, fmt.Errorf("parse %q: %w", ConfigKeyBatchSize, err)
		}

		sourceConfig.BatchSize = batchSize
	}

	// parse copyExistingData if it's not empty
	if copyExistingDataStr := raw[ConfigKeyCopyExistingData]; copyExistingDataStr != "" {
		copyExisting, err := strconv.ParseBool(copyExistingDataStr)
		if err != nil {
			return Config{}, fmt.Errorf("parse %q: %w", ConfigKeyCopyExistingData, err)
		}

		sourceConfig.CopyExistingData = copyExisting
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
