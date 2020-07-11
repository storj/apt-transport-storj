// Copyright (C) 2020 Storj Labs, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package symlinks

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveSymlinks(t *testing.T) {
	symlinkMap := SymlinkMap{
		"a":                             "b",
		"toplevel":                      "b/c/d",
		"toplevel2":                     "a/c/d",
		"uses/parent-dir":               "../symlink",
		"multi/step/resolve":            "step2/subdirectory",
		"multi/step/step2/subdirectory": "../../../toplevel2/e",
		"too/many/parent-dirs":          "../../../../../omg-hax",
		"lookup/loop":                   "../uses/parent-dir/loop",
		"symlink/loop":                  "../../lookup/loop",
	}

	tests := []struct {
		name      string
		fileName  string
		want      string
		expectErr error
	}{
		{"basic", "a", "b", nil},
		{"parent-dirs", "uses/parent-dir/foo", "symlink/foo", nil},
		{"multi-step", "multi/step/resolve", "b/c/d/e", nil},
		{"non-commutative", "b", "b", nil},
		{"too-many-parent-dirs", "too/many/parent-dirs", "omg-hax", nil},
		{"recursive", "lookup/loop", "", ErrSymlinkSteps},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := symlinkMap.Resolve(tt.fileName)
			if tt.expectErr != nil {
				require.Error(t, err, tt.expectErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEncodeDecode(t *testing.T) {
	symlinkMap := SymlinkMap{
		"aquote":    "\"",
		"":          "emptiness",
		"noon/time": "../../bula vinaka",
	}

	var buf bytes.Buffer
	writeN, err := symlinkMap.WriteTo(&buf)
	require.NoError(t, err)
	bufSize := buf.Len()
	assert.Equal(t, int64(bufSize), writeN)

	var otherSymlinkMap SymlinkMap
	readN, err := otherSymlinkMap.ReadFrom(&buf)
	require.NoError(t, err)
	assert.Equal(t, writeN, readN)

	assert.Equal(t, symlinkMap, otherSymlinkMap)
}
