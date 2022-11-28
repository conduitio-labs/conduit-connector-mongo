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

package iterator

import "errors"

var (
	// ErrNoIterator occurs when the [Combined] has no any underlying iterators.
	ErrNoIterator = errors.New("no iterator")

	// errUnsupportedOperationType occurs when we got an unsupported operation type.
	// This error shouldn't actually occur, as we filter Change Stream events by operation type.
	// It's just a sentinel error for the [changeStreamEvent.toRecord] method.
	errUnsupportedOperationType = errors.New("unsupported operation type")

	// errNilSDKPosition occurs when trying to parse a nil [sdk.Position].
	// It's just a sentinel error for the [parsePosition] function.
	errNilSDKPosition = errors.New("nil sdk position")
)
