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

import (
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/bson"
)

// positionMode defines the [position] mode.
type positionMode string

// The available position modes are listed below.
const (
	modeSnapshot positionMode = "snapshot"
	modeCDC      positionMode = "cdc"
)

// position is an iterator position.
// It consists of a resumeToken token that allows us to resume a Change Stream
// or restart a snapshot process from a particular position.
type position struct {
	Mode positionMode `json:"mode"`
	// ResumeToken is a Change Stream resume token
	// that allows resuming a Change Stream.
	// This value is used if the mode is CDC.
	ResumeToken bson.Raw `json:"resumeToken,omitempty"`
	// Element is a value of the last processed element by the snapshot capture.
	// This value is used if the mode is snapshot.
	Element any `json:"string,omitempty"`
}

// marshalSDKPosition marshals the underlying [position] into a [sdk.Position] as JSON bytes.
func (p *position) marshalSDKPosition() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return sdk.Position(positionBytes), nil
}

// parsePosition converts an [sdk.Position] into a [position].
func parsePosition(sdkPosition sdk.Position) (*position, error) {
	if sdkPosition == nil {
		return nil, errNilSDKPosition
	}

	var pos position
	if err := json.Unmarshal(sdkPosition, &pos); err != nil {
		return nil, fmt.Errorf("unmarshal sdk.Position into position: %w", err)
	}

	return &pos, nil
}
