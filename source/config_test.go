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
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mongo/config"
)

func TestParseConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     map[string]string
		want    Config
		wantErr bool
	}{
		{
			name: "success_required_only",
			raw: map[string]string{
				config.KeyURI:        "mongodb://localhost:27017",
				config.KeyDB:         "test",
				config.KeyCollection: "users",
			},
			want: Config{
				Config: config.Config{
					URI:        "mongodb://localhost:27017",
					DB:         "test",
					Collection: "users",
				},
				BatchSize: defaultBatchSize,
			},
			wantErr: false,
		},
		{
			name: "success_custom_batch_size",
			raw: map[string]string{
				config.KeyURI:        "mongodb://localhost:27017",
				config.KeyDB:         "test",
				config.KeyCollection: "users",
				ConfigKeyBatchSize:   "100",
			},
			want: Config{
				Config: config.Config{
					URI:        "mongodb://localhost:27017",
					DB:         "test",
					Collection: "users",
				},
				BatchSize: 100,
			},
			wantErr: false,
		},
		{
			name: "fail_invalid_common_config_missing_required",
			raw: map[string]string{
				config.KeyDB:         "test",
				config.KeyCollection: "users",
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_batch_size_gte",
			raw: map[string]string{
				config.KeyURI:        "mongodb://localhost:27017",
				config.KeyDB:         "test",
				config.KeyCollection: "users",
				ConfigKeyBatchSize:   "-1",
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_batch_size_lte",
			raw: map[string]string{
				config.KeyURI:        "mongodb://localhost:27017",
				config.KeyDB:         "test",
				config.KeyCollection: "users",
				ConfigKeyBatchSize:   "1000000000",
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_batch_size_nan",
			raw: map[string]string{
				config.KeyURI:        "mongodb://localhost:27017",
				config.KeyDB:         "test",
				config.KeyCollection: "users",
				ConfigKeyBatchSize:   "two",
			},
			want:    Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseConfig(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseConfig() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
