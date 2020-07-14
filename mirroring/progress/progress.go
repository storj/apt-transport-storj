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
	"encoding/gob"
	"io"
	"os"
	"path"
	"sync"
	"syscall"

	"github.com/zeebo/errs"

	"storj.io/apt-transport-storj/common"
	"storj.io/apt-transport-storj/symlinks"
)

const (
	// SaveFilename is the name of the file that will be used to save a SyncState at the
	// top level of the localdir.
	SaveFilename = ".storj-sync-mirror.progress"
)

// SyncState represents the state of a mirror sync operation. It may be saved to disk and
// restored in order to resume a previously terminated sync process.
type SyncState struct {
	DownstreamMap       common.FileMap
	LocalMap            common.FileMap
	SymlinkMap          symlinks.SymlinkMap
	ReferencedFilenames []common.FileEntry

	TransferFiles []string
	PushFiles     []string
	DeleteFiles   []string

	progressUpdateStream   *os.File
	progressEncoder        *gob.Encoder
	progressUpdateFilename string
	progressLock           sync.Mutex
}

type progressStep struct {
	Action Action
	Value  string
}

// Action represents the type of a single completed step taken by a mirror sync operation.
type Action int16

const (
	// ActionTransfer marks a successful file transfer (from upstream direct to downstream).
	ActionTransfer Action = iota
	// ActionPush marks a successful file push (rsync'd from upstream, uploaded from local to downstream).
	ActionPush
	// ActionDelete marks a successful file delete on the downstream.
	ActionDelete
	// ActionSentSymlinkMap indicates that the symlink map has successfully been uploaded to the downstream.
	ActionSentSymlinkMap
)

// EncodingError is an error class wrapping errors encountered during encoding of progress.
var EncodingError = errs.Class("progress encoding")

// DecodingError is an error class wrapping errors encountered during decoding of progress.
var DecodingError = errs.Class("progress decoding")

func readStateFrom(r io.ReadSeeker, st *SyncState) (err error) {
	dec := gob.NewDecoder(r)
	if err := dec.Decode(st); err != nil {
		return DecodingError.Wrap(err)
	}

	didTransfer := make(map[string]struct{})
	didPush := make(map[string]struct{})
	didDelete := make(map[string]struct{})
	var pos int64
	for {
		pos, err = r.Seek(0, io.SeekCurrent)
		if err != nil {
			return DecodingError.New("could not get position of reader: %v", err)
		}

		var step progressStep
		if err := dec.Decode(&step); err != nil {
			if err == io.EOF {
				break
			}
			if err.Error() == "extra data in buffer" {
				// can happen when save was interrupted
				break
			}
			return DecodingError.Wrap(err)
		}
		switch step.Action {
		case ActionTransfer:
			didTransfer[step.Value] = struct{}{}
		case ActionPush:
			didPush[step.Value] = struct{}{}
		case ActionDelete:
			didDelete[step.Value] = struct{}{}
		case ActionSentSymlinkMap:
			st.SymlinkMap = nil
		default:
			return DecodingError.New("unrecognized Action %v", step.Action)
		}
	}
	st.TransferFiles = filterList(st.TransferFiles, didTransfer)
	st.PushFiles = filterList(st.PushFiles, didPush)
	st.DeleteFiles = filterList(st.DeleteFiles, didDelete)

	_, err = r.Seek(pos, io.SeekStart)
	if err != nil {
		return DecodingError.New("could not seek in reader: %v", err)
	}
	return nil
}

// ReadPriorState reads a previously saved progress file, including the initial upstream,
// downstream, and local state, and all subsequent records written by UpdateProgress
// afterward. It returns a new SyncState object, to which additional steps can be saved
// with UpdateProgress.
func ReadPriorState(localDir string) (st *SyncState, err error) {
	saveFileName := path.Join(localDir, SaveFilename)
	f, err := os.OpenFile(saveFileName, os.O_RDWR, 0)
	if err != nil {
		return nil, DecodingError.Wrap(err)
	}
	defer func() {
		if f != nil {
			err = errs.Combine(err, f.Close())
		}
	}()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return nil, DecodingError.New("could not lock state file: %v", err)
	}

	st = &SyncState{progressUpdateFilename: f.Name()}
	if err := readStateFrom(f, st); err != nil {
		return nil, err
	}

	pos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, DecodingError.New("could not get position in save file: %v", err)
	}
	if err := f.Truncate(pos); err != nil {
		return nil, DecodingError.New("could not truncate save file at %d: %v", pos, err)
	}

	st.progressUpdateStream = f
	st.progressEncoder = gob.NewEncoder(st.progressUpdateStream)
	f = nil
	return st, nil
}

func filterList(fileList []string, excludeSet map[string]struct{}) []string {
	if len(excludeSet) == 0 {
		return fileList
	}
	newList := make([]string, 0, len(fileList)-len(excludeSet))
	for _, item := range fileList {
		if _, ok := excludeSet[item]; !ok {
			newList = append(newList, item)
		}
	}
	return newList
}

// SaveProgress starts a new progress file or overwrites an existing one. It records
// all the collected upstream, downstream, and local state that needs to be taken into
// account when determining what to transfer, upload, or delete.
func (st *SyncState) SaveProgress(localDir string) error {
	f, err := os.OpenFile(path.Join(localDir, SaveFilename), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return EncodingError.Wrap(err)
	}
	defer func() {
		if f != nil {
			err = errs.Combine(err, f.Close())
		}
	}()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return EncodingError.New("could not lock progress save file: %v", err)
	}

	enc := gob.NewEncoder(f)
	if err := enc.Encode(st); err != nil {
		return EncodingError.Wrap(err)
	}
	st.progressUpdateStream = f
	st.progressEncoder = enc
	st.progressUpdateFilename = f.Name()
	f = nil
	return EncodingError.Wrap(st.progressUpdateStream.Sync())
}

// UpdateProgress records a successful step taken in syncing the mirror. If the process
// is canceled and resumed, this progress will be read and the step will not need to be
// taken again.
func (st *SyncState) UpdateProgress(action Action, value string) error {
	step := progressStep{Action: action, Value: value}
	st.progressLock.Lock()
	err := st.progressEncoder.Encode(&step)
	st.progressLock.Unlock()
	if err != nil {
		return EncodingError.Wrap(err)
	}
	return EncodingError.Wrap(st.progressUpdateStream.Sync())
}

// Close closes the SyncState handle and its associated file stream.
func (st *SyncState) Close() error {
	if st.progressUpdateStream == nil {
		return nil
	}
	err := st.progressUpdateStream.Close()
	st.progressUpdateStream = nil
	return EncodingError.Wrap(err)
}

// Clear removes the stored progress file entirely. It should be called once a whole
// mirroring sync operation is complete.
func (st *SyncState) Clear() error {
	return EncodingError.Wrap(os.Remove(st.progressUpdateFilename))
}
