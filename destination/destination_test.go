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

package destination

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mongo/config"
	"github.com/conduitio-labs/conduit-connector-mongo/destination/mock"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestDestination_Configure_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{}
	err := d.Configure(context.Background(), map[string]string{
		config.KeyURI:        "mongodb://localhost:27017",
		config.KeyDB:         "test",
		config.KeyCollection: "users",
	})

	is.NoErr(err)

	is.Equal(d.config, config.Config{
		URI: &url.URL{
			Scheme: "mongodb",
			Host:   "localhost:27017",
		},
		DB:         "test",
		Collection: "users",
	})
}

func TestDestination_Configure_mechanismFailure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{}
	err := d.Configure(context.Background(), map[string]string{
		config.KeyURI:           "mongodb://localhost:27017",
		config.KeyDB:            "test",
		config.KeyCollection:    "users",
		config.KeyAuthMechanism: "not existing mechanism",
	})

	is.Equal(err.Error(), "parse config: invalid auth mechanism \"NOT EXISTING MECHANISM\"")
}

func TestDestination_Configure_structValidateFailure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{}
	err := d.Configure(context.Background(), map[string]string{
		config.KeyURI:        "mong\\'odb://localhost:27017",
		config.KeyDB:         "test",
		config.KeyCollection: "users",
	})

	is.True(err != nil)
}

func TestDestination_Write_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockWriter(ctrl)
	it.EXPECT().Write(ctx, opencdc.Record{}).Return(nil)

	d := Destination{
		writer: it,
	}

	count, err := d.Write(ctx, []opencdc.Record{{}})
	is.NoErr(err)

	is.Equal(count, 1)
}

func TestDestination_Write_failInsertRecord(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockWriter(ctrl)
	it.EXPECT().Write(ctx, opencdc.Record{}).Return(errors.New("insert record: fail"))

	d := Destination{
		writer: it,
	}

	_, err := d.Write(ctx, []opencdc.Record{{}})
	is.True(err != nil)
	is.Equal(err.Error(), "write record: insert record: fail")
}

func TestDestination_Teardown_successWriterIsNil(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	d := Destination{
		writer: nil,
	}

	err := d.Teardown(ctx)
	is.NoErr(err)
}
