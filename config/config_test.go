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

package config

import (
	"reflect"
	"testing"
)

func TestAuthMechanism_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		am   AuthMechanism
		want bool
	}{
		{
			name: "success_SCRAM_SHA_256",
			am:   "SCRAM-SHA-256",
			want: true,
		},
		{
			name: "success_SCRAM_SHA_1",
			am:   "SCRAM-SHA-1",
			want: true,
		},
		{
			name: "success_MONGODB_CR",
			am:   "MONGODB-CR",
			want: true,
		},
		{
			name: "success_MONGODB_AWS",
			am:   "MONGODB-AWS",
			want: true,
		},
		{
			name: "success_X.509",
			am:   "X.509",
			want: true,
		},
		{
			name: "fail_unsupported",
			am:   "SASL",
			want: false,
		},
		{
			name: "fail_empty_string",
			am:   "",
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.am.IsValid(); got != tt.want {
				t.Errorf("AuthMechanism.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	t.Parallel()

	type args struct {
		raw map[string]string
	}

	tests := []struct {
		name    string
		args    args
		want    Config
		wantErr bool
	}{
		{
			name: "success_only_required_fields",
			args: args{
				raw: map[string]string{
					KeyURI:        "mongodb://localhost:27017",
					KeyDB:         "test",
					KeyCollection: "users",
				},
			},
			want: Config{
				URI:        "mongodb://localhost:27017",
				DB:         "test",
				Collection: "users",
			},
			wantErr: false,
		},
		{
			name: "success_with_auth_mechanism",
			args: args{
				raw: map[string]string{
					KeyURI:           "mongodb://localhost:27017",
					KeyDB:            "test",
					KeyCollection:    "users",
					KeyAuthMechanism: "SCRAM-SHA-256",
				},
			},
			want: Config{
				URI:        "mongodb://localhost:27017",
				DB:         "test",
				Collection: "users",
				Auth: AuthConfig{
					Mechanism: SCRAMSHA256,
				},
			},
			wantErr: false,
		},
		{
			name: "success_with_auth_mechanism_lowercase",
			args: args{
				raw: map[string]string{
					KeyURI:           "mongodb://localhost:27017",
					KeyDB:            "test",
					KeyCollection:    "users",
					KeyAuthMechanism: "scram-sha-256",
				},
			},
			want: Config{
				URI:        "mongodb://localhost:27017",
				DB:         "test",
				Collection: "users",
				Auth: AuthConfig{
					Mechanism: SCRAMSHA256,
				},
			},
			wantErr: false,
		},
		{
			name: "success_with_tls_configs",
			args: args{
				raw: map[string]string{
					KeyURI:                       "mongodb://localhost:27017",
					KeyDB:                        "test",
					KeyCollection:                "users",
					KeyAuthMechanism:             "SCRAM-SHA-256",
					KeyAuthTLSCAFile:             "config.go", // pointed to the existing file
					KeyAuthTLSCertificateKeyFile: "config.go", // pointed to the existing file
				},
			},
			want: Config{
				URI:        "mongodb://localhost:27017",
				DB:         "test",
				Collection: "users",
				Auth: AuthConfig{
					Mechanism:             SCRAMSHA256,
					TLSCAFile:             "config.go",
					TLSCertificateKeyFile: "config.go",
				},
			},
			wantErr: false,
		},
		{
			name: "fail_missing_required_field",
			args: args{
				raw: map[string]string{
					KeyURI: "mongodb://localhost:27017",
					KeyDB:  "test",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_uri",
			args: args{
				raw: map[string]string{
					KeyURI:        "mong\\'odb://localhost:27017",
					KeyDB:         "test",
					KeyCollection: "users",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_auth_mechanism",
			args: args{
				raw: map[string]string{
					KeyURI:           "mongodb://localhost:27017",
					KeyDB:            "test",
					KeyCollection:    "users",
					KeyAuthMechanism: "WRONG_AUTH",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_tls_config_files_do_not_exist",
			args: args{
				raw: map[string]string{
					KeyURI:                       "mongodb://localhost:27017",
					KeyDB:                        "test",
					KeyCollection:                "users",
					KeyAuthTLSCAFile:             "wrong.ca",        // non-existent file
					KeyAuthTLSCertificateKeyFile: "certificate.txt", // non-existent file
				},
			},
			want:    Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
