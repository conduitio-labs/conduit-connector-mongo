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

package iterator

import "errors"

var (
	// ErrNoIterator occurs when the [Combined] has no any underlying iterators.
	ErrNoIterator = errors.New("no iterator")

	// ErrResumeTokenMissing occurs when the connector is asked to resume from a
	// persisted position that carries no Change Stream resume token, while the
	// Change Stream is the iterator that would actually be used.
	//
	// Refusing is the point. createChangeStream applies SetResumeAfter only when
	// a token is present; with no token the stream opens with no start point at
	// all, which MongoDB interprets as "from now". Everything that changed while
	// the connector was down is then never delivered — silently, with no error
	// and nothing routed to a DLQ. Verified against a real replica set: only
	// post-resume writes arrive.
	//
	// A position with no token means the token was never captured in the first
	// place — reachable when the Change Stream could not be opened at initial
	// construction (see matchProjectStageErrMessage, the Azure CosmosDB path) so
	// the snapshot recorded positions without one. If the Change Stream later
	// becomes usable, resuming would silently skip the whole gap.
	//
	// Failing loud costs a restart with an explicit, actionable error. Starting
	// from "now" costs data, invisibly.
	ErrResumeTokenMissing = errors.New(
		"cannot resume: persisted position carries no Change Stream resume token, " +
			"so resuming would silently start from now and skip every change made while " +
			"the connector was down; delete the pipeline's stored position to re-snapshot " +
			"from scratch, or restore a position captured with a resume token")

	// errUnsupportedOperationType occurs when we got an unsupported operation type.
	// This error shouldn't actually occur, as we filter Change Stream events by operation type.
	// It's just a sentinel error for the [changeStreamEvent.toRecord] method.
	errUnsupportedOperationType = errors.New("unsupported operation type")

	// errNilSDKPosition occurs when trying to parse a nil [opencdc.Position].
	// It's just a sentinel error for the [parsePosition] function.
	errNilSDKPosition = errors.New("nil sdk position")

	// errMaxFieldValueNotFound occurs when it's impossible to find the maximum value of a field.
	errMaxFieldValueNotFound = errors.New("max field value not found")

	// errNoDocuments occurs when there're no documents in a collection.
	errNoDocuments = errors.New("no documents in collection")

	// matchProjectStageErrMessage contains an error text that Azure CosmosDB for MongoDB returns
	// when you try to create a Change Stream.
	// We use it to determine whether we should do snapshot polling instead of CDC.
	matchProjectStageErrMessage = "Change stream must be followed by a match and then a project stage"
)
