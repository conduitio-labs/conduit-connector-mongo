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

package config

import (
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
			am:   "MONGODB-X509",
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

	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "success_with_auth_mechanism",
			cfg: Config{
				URIStr:     "mongodb://localhost:27017",
				DB:         "test",
				Collection: "users",
				Auth: AuthConfig{
					Mechanism: AuthMechanism("SCRAM-SHA-256"),
				},
			},
			wantErr: false,
		},
		{
			name: "success_with_auth_mechanism_lowercase",
			cfg: Config{
				URIStr:     "mongodb://localhost:27017",
				DB:         "test",
				Collection: "users",
				Auth: AuthConfig{
					Mechanism: AuthMechanism("scram-sha-256"),
				},
			},

			wantErr: false,
		},
		{
			name: "success_with_tls_configs",
			cfg: Config{
				URIStr:     "mongodb://localhost:27017",
				DB:         "test",
				Collection: "users",
				Auth: AuthConfig{
					Mechanism:             AuthMechanism("SCRAM-SHA-256"),
					TLSCAFile:             "config.go",
					TLSCertificateKeyFile: "config.go",
				},
			},
			wantErr: false,
		},

		{
			name: "fail_invalid_uri",
			cfg: Config{
				URIStr:     "mong\\'odb://localhost:27017",
				DB:         "test",
				Collection: "users",
			},
			wantErr: true,
		},
		{
			name: "fail_invalid_auth_mechanism",
			cfg: Config{
				URIStr:     "mongodb://localhost:27017",
				DB:         "test",
				Collection: "users",
				Auth: AuthConfig{
					Mechanism: AuthMechanism("WRONG_AUTH"),
				},
			},
			wantErr: true,
		},
		{
			name: "fail_tls_config_files_do_not_exist",
			cfg: Config{
				URIStr:     "mongodb://localhost:27017",
				DB:         "test",
				Collection: "users",
				Auth: AuthConfig{
					TLSCAFile:             "wrong.ca",
					TLSCertificateKeyFile: "certificate.txt",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate(t.Context())
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
