// Copyright © 2026 Meroxa, Inc. & Yalantis
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

// Guards the silent "resume from now" path.
//
// createChangeStream applies SetResumeAfter only when the persisted position
// carries a resume token. With no token the stream opens with no start point,
// which MongoDB serves from now — so every change made while the connector was
// down is dropped, silently, with no error and nothing in a DLQ.
//
// Verified against a real replica set before this guard existed: resuming with
// no token delivered ONLY writes made after the resume. An insert and an update
// performed while the connector was down were both absent.
//
// This is the same class as Conduit's 20260729 snapshot-handoff postmortem,
// whose follow-up list asks whether MongoDB loses the CDC start position the
// way MySQL did in conduitio-labs/conduit-connector-mysql#180. The common path
// here is sound — on a healthy replica set ResumeToken() is populated
// immediately after Watch, so positions do carry it. The gap is the path where
// the Change Stream could not be opened at construction (the CosmosDB
// matchProjectStageErrMessage fallback): the snapshot then records positions
// with no token, and if the Change Stream later becomes usable, the next resume
// skips the entire gap.
//
// NOTE: this package had NO test files before this one. That is why the gap
// went unnoticed.

package iterator

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// TestResumeTokenGuard_RejectsTokenlessCDCResume is the regression test for the
// guard itself.
//
// It is deliberately a unit test over the guard's exact predicate rather than an
// integration test, for a reason worth stating: an integration test that merely
// resumes and counts records would pass whether or not the guard exists, because
// the loss only shows up when something changed during the downtime window. It
// would assert on the harness, not the connector. This asserts the decision.
func TestResumeTokenGuard_RejectsTokenlessCDCResume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cdcInUse bool
		position *position
		wantErr  bool
	}{
		{
			// The dangerous case: we have somewhere to resume from, the Change
			// Stream is what will run, and there is no token to resume at.
			name:     "cdc in use, position without token: refuse",
			cdcInUse: true,
			position: &position{Mode: modeCDC, Element: "x"},
			wantErr:  true,
		},
		{
			name:     "cdc in use, snapshot-mode position without token: refuse",
			cdcInUse: true,
			position: &position{Mode: modeSnapshot, Element: "x", MaxElement: "z"},
			wantErr:  true,
		},
		{
			// Normal resume. This is what a healthy replica set produces.
			name:     "cdc in use, position with token: allow",
			cdcInUse: true,
			position: &position{Mode: modeCDC, ResumeToken: bson.Raw{0x05, 0x00}},
			wantErr:  false,
		},
		{
			// A genuinely fresh start legitimately begins from now. Refusing
			// here would break every first run.
			name:     "cdc in use, no position at all: allow",
			cdcInUse: true,
			position: nil,
			wantErr:  false,
		},
		{
			// The CosmosDB fallback: the Change Stream could not be opened, the
			// polling snapshot runs instead, and no Change Stream is consulted.
			// Guarding here would break that path by pre-empting the
			// matchProjectStageErrMessage check.
			name:     "cdc NOT in use (polling fallback), no token: allow",
			cdcInUse: false,
			position: &position{Mode: modeCDC, Element: "x"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := checkResumeToken(tt.cdcInUse, tt.position)
			if tt.wantErr && !errors.Is(err, ErrResumeTokenMissing) {
				t.Fatalf("expected ErrResumeTokenMissing, got %v", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
