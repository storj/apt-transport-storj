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

package progress

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/apt-transport-storj/common"
)

func dummySyncState() *SyncState {
	return &SyncState{
		DownstreamMap: map[string]common.FileEntry{
			"abc/def": {Path: "abc/def", Size: 294},
			"ghi":     {Path: "ghi", Size: 12345},
		},
		LocalMap: map[string]common.FileEntry{
			"localfile": {Path: "localfile", Size: 999},
			"z/y/x/w":   {Path: "z/y/x/w", Size: 0},
		},
		SymlinkMap: map[string]string{
			"foo/bar/Packages": "foo/bar/by-hash/SHA256/NOTAREALHASHLOL",
		},
		ReferencedFilenames: []common.FileEntry{
			{Path: "jibber/jabber", Size: 1},
		},
		TransferFiles: []string{"transfer-me", "already-transferred-me", "transfer-me-too"},
		PushFiles:     []string{"push-me"},
		DeleteFiles:   []string{"delete-me"},
	}
}

func TestSaveAndLoadProgress(t *testing.T) {
	st := dummySyncState()

	localDir, err := ioutil.TempDir("", "apt-transport-storj-testing-*")
	require.NoError(t, err)

	err = st.SaveProgress(localDir)
	require.NoError(t, err)

	stat, err := os.Stat(st.progressUpdateFilename)
	require.NoError(t, err)
	require.Greater(t, stat.Size(), int64(0))

	err = st.UpdateProgress(ActionTransfer, "already-transferred-me")
	require.NoError(t, err)
	err = st.UpdateProgress(ActionSentSymlinkMap, "")
	require.NoError(t, err)
	err = st.UpdateProgress(ActionDelete, "not-present")
	require.NoError(t, err)

	stat2, err := os.Stat(st.progressUpdateFilename)
	require.NoError(t, err)
	require.Greater(t, stat2.Size(), stat.Size())

	require.NoError(t, st.Close())

	st2, err := ReadPriorState(localDir)
	require.NoError(t, err)

	assert.NotEqual(t, syncStateForCompare(st), syncStateForCompare(&SyncState{}))
	assert.NotEqual(t, syncStateForCompare(st), syncStateForCompare(st2))

	st.SymlinkMap = nil
	st.TransferFiles = []string{"transfer-me", "transfer-me-too"}
	assert.Equal(t, syncStateForCompare(st), syncStateForCompare(st2))

	stat3, err := os.Stat(st2.progressUpdateFilename)
	require.NoError(t, err)
	assert.Equal(t, stat2.Size(), stat3.Size())

	err = st2.UpdateProgress(ActionPush, "push-me")
	require.NoError(t, err)

	stat4, err := os.Stat(st2.progressUpdateFilename)
	require.NoError(t, err)
	assert.Greater(t, stat4.Size(), stat3.Size())

	require.NoError(t, st2.Clear())
	_, err = os.Stat(st2.progressUpdateFilename)
	assert.Error(t, err)

	require.NoError(t, st2.Close())
}

func syncStateForCompare(st *SyncState) SyncState {
	return SyncState{
		DownstreamMap:       st.DownstreamMap,
		LocalMap:            st.LocalMap,
		SymlinkMap:          st.SymlinkMap,
		ReferencedFilenames: st.ReferencedFilenames,
		TransferFiles:       st.TransferFiles,
		PushFiles:           st.PushFiles,
		DeleteFiles:         st.DeleteFiles,
	}
}
