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

package common

import "time"

// Version is the version number of apt-transport-storj.
const Version = "0.0.1"

// FileEntry represents a small amount of info about a locally or remotely stored file.
type FileEntry struct {
	// Path is the path to this file entry, relative to the mirror root.
	Path string
	// Timestamp is the timestamp of the file entry, if known, or time.Time{} otherwise. Timestamps
	// are not generally known for files referenced by indexes.
	Timestamp time.Time
	// Size is the size of the file, whether local or remote.
	Size int64
}

// FileMap maps filenames (relative to the mirror root) to FileEntry records. It may describe
// files on the local host, or on the upstream mirror, or in the downstream Storj bucket.
type FileMap map[string]FileEntry
