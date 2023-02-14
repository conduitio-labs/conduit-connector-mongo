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

package validator

import "testing"

func Test_getFieldKey(t *testing.T) {
	t.Parallel()

	type args struct {
		data      any
		fieldName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "struct_with_key_tag",
			args: args{
				data: &struct {
					Name string `key:"super_name"`
				}{
					Name: "void",
				},
				fieldName: "Name",
			},
			want: "super_name",
		},
		{
			name: "struct_without_any_key_tags",
			args: args{
				data: &struct {
					Name string
				}{
					Name: "void",
				},
				fieldName: "Name",
			},
			want: "Name",
		},
		{
			name: "struct_with_empty_key_tag",
			args: args{
				data: &struct {
					Name string `key:""`
				}{
					Name: "void",
				},
				fieldName: "Name",
			},
			want: "Name",
		},
		{
			name: "data_is_nil_should_return_a_field_name",
			args: args{
				data:      nil,
				fieldName: "Name",
			},
			want: "Name",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := getFieldKey(tt.args.data, tt.args.fieldName); got != tt.want {
				t.Errorf("getFieldKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
