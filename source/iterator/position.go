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

// Position is an iterator position.
// It consists of a resumeToken token that allows us to resume a Change Stream
// or restart a snapshot process from a particular position.
type Position struct {
	ResumeToken bson.Raw `json:"resumeToken"`
}

// MarshalSDKPosition marshals the underlying [Position] into a [sdk.Position] as JSON bytes.
func (p *Position) MarshalSDKPosition() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return sdk.Position(positionBytes), nil
}

// ParsePosition converts an [sdk.Position] into a [Position].
func ParsePosition(sdkPosition sdk.Position) (*Position, error) {
	if sdkPosition == nil {
		return nil, ErrNilSDKPosition
	}

	var position Position
	if err := json.Unmarshal(sdkPosition, &position); err != nil {
		return nil, fmt.Errorf("unmarshal sdk.Position into Position: %w", err)
	}

	return &position, nil
}
