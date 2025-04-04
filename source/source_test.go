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
	"errors"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mongo/source/mock"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestSource_Read_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := t.Context()

	key := make(opencdc.StructuredData)
	key["id"] = 1

	metadata := make(opencdc.Metadata)
	metadata.SetCreatedAt(time.Time{})

	record := opencdc.Record{
		Position: opencdc.Position(`{"lastId": 1}`),
		Metadata: metadata,
		Key:      key,
		Payload: opencdc.Change{
			After: key,
		},
	}

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(record, nil)

	s := Source{
		iterator: it,
	}

	r, err := s.Read(ctx)
	is.NoErr(err)

	is.Equal(r, record)
}

func TestSource_Read_failHasNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := t.Context()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, errors.New("get data: fail"))

	s := Source{
		iterator: it,
	}

	record, err := s.Read(ctx)
	is.Equal(record, opencdc.Record{})
	is.True(err != nil)
}

func TestSource_Read_failNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := t.Context()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(opencdc.Record{}, errors.New("key is not exist"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.True(err != nil)
}

func TestSource_Teardown_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := t.Context()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop(ctx).Return(nil)

	s := Source{
		iterator: it,
	}

	err := s.Teardown(t.Context())
	is.NoErr(err)
}

func TestSource_Teardown_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := t.Context()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop(ctx).Return(errors.New("some error"))

	s := Source{
		iterator: it,
	}

	err := s.Teardown(t.Context())
	is.Equal(err.Error(), "stop iterator: some error")
}
