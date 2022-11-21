// Copyright © 2022 Meroxa, Inc.
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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-mongo/config"
)

type Config struct {
	config.Config
}

func ParseConfig(cfg map[string]string) (Config, error) {
	common, err := config.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	destinationConfig := Config{
		Config: common,
	}

	return destinationConfig, nil
}
