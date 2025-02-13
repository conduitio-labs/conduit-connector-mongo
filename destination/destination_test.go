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
	"testing"

	"github.com/conduitio-labs/conduit-connector-mongo/destination/mock"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

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
