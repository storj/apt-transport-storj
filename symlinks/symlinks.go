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
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	"storj.io/uplink"

	"storj.io/apt-transport-storj/common"
)

const (
	// StorjSymlinkMapKey is the key under which an apt-transport-storj symlink map
	// is expected to be stored.
	StorjSymlinkMapKey = "storj~apt~symlink~map.gz"

	// MaxSymlinkResolveSteps is the maximum number of steps that can be taken when
	// resolving a symlink in a SymlinkMap. If this number of steps is exceeded, the
	// Resolve() method will return ErrSymlinkSteps.
	MaxSymlinkResolveSteps = 256
)

// ErrSymlinkSteps is returned when a symlink resolution operation exceeds the maximum number
// of steps (MaxSymlinkResolveSteps)
var ErrSymlinkSteps = fmt.Errorf("maximum number of symlink resolution steps exceeded")

// SymlinkMap represents a map of symlinks in a directory tree, with symlink locations
// stored relative to the root of the tree.
type SymlinkMap map[string]string

// NewSymlinkMap creates a new SymlinkMap.
func NewSymlinkMap() SymlinkMap {
	return make(SymlinkMap)
}

// Add adds a path and a target to the symlink map. The target path will be interpreted
// as relative to the directory indicated by key. It will not be allowed to point outside
// the top level of the symlink map (e.g. using `../../`).
func (m SymlinkMap) Add(key, target string) {
	m[path.Clean(key)] = path.Clean(target)
}

// Resolve resolves symlinks in the given filename, according to the symlink map. All
// paths in the symlink map are expected to be 'clean' already (as in path.Clean).
// Symlink resolution follows the normal rules for a posix filesystem: paths are resolved
// one path component at a time; each path component might be a symlink; symlinks might
// point at other symlinks, which must be resolved themselves; symlink destinations are
// relative to the directory in which the symlink is stored; symlink resolution fails if
// it takes more than MaxSymlinkResolveSteps steps (because of the possibility for loops).
func (m SymlinkMap) Resolve(fileName string) (string, error) {
	resolved, _, err := m.resolveWorker(path.Clean(fileName), 1)
	return resolved, err
}

func (m SymlinkMap) resolveWorker(fileName string, numSteps int) (resolved string, maxDepth int, err error) {
	if numSteps > MaxSymlinkResolveSteps {
		return "", numSteps, ErrSymlinkSteps
	}
	dir, file := path.Split(fileName)
	if dir != "" {
		dir, numSteps, err = m.resolveWorker(strings.TrimRight(dir, "/"), numSteps+1)
		if err != nil {
			return "", numSteps, err
		}
		dir += "/"
	}
	fileName = dir + file
	linkDest, ok := m[fileName]
	if !ok {
		return fileName, numSteps, nil
	}
	linkDest = cleanPathAsIfRoot(dir + linkDest)
	return m.resolveWorker(linkDest, numSteps+1)
}

func cleanPathAsIfRoot(filePath string) string {
	if len(filePath) > 0 && filePath[0] == '/' {
		return path.Clean(filePath)
	}
	return path.Clean("/" + filePath)[1:]
}

type countingWriter struct {
	w   io.Writer
	pos int64
}

type countingReader struct {
	r   io.Reader
	pos int64
}

func (cw *countingWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.pos += int64(n)
	return n, err
}

func (cr *countingReader) Read(p []byte) (n int, err error) {
	n, err = cr.r.Read(p)
	cr.pos += int64(n)
	return n, err
}

// WriteTo encodes a symlink map and writes it to the given writer, returning
// the number of bytes written and any error encountered (conforming to
// io.WriterTo).
func (m SymlinkMap) WriteTo(writer io.Writer) (n int64, err error) {
	symlinkJSON, err := json.Marshal(m)
	if err != nil {
		return 0, err
	}
	// gzip.Writer doesn't let us see how many bytes have actually been written
	// to the underlying writer, so we can't return an accurate n here without
	// some weirdness. countingWriter is that weirdness.
	cWriter := &countingWriter{w: writer}
	compressedWriter, err := gzip.NewWriterLevel(cWriter, gzip.BestCompression)
	if err != nil {
		return 0, err
	}
	// don't defer-close compressedWriter; we need the close to happen before
	// we take cWriter.pos
	_, err = io.Copy(compressedWriter, bytes.NewReader(symlinkJSON))
	closeErr := compressedWriter.Close()
	if err == nil {
		err = closeErr
	}
	return cWriter.pos, err
}

// ReadFrom reads an encoded symlink map from the given reader and decodes
// it into the receiver SymlinkMap. Returns the number of bytes read and
// any error encountered (conforming to io.ReaderFrom).
func (m *SymlinkMap) ReadFrom(reader io.Reader) (n int64, err error) {
	// gzip.Reader doesn't let us see how many bytes have actually been read
	// from the underlying reader, so we can't return an accurate n here without
	// some weirdness. countingReader is that weirdness.
	cReader := &countingReader{r: reader}
	compressedReader, err := gzip.NewReader(cReader)
	if err != nil {
		return 0, err
	}
	defer common.DeferClose(compressedReader, &err)
	decoder := json.NewDecoder(compressedReader)
	err = decoder.Decode(m)
	return cReader.pos, err
}

// UploadSymlinkMap uploads a symlink map to the given bucket, using the given
// open project information.
func UploadSymlinkMap(ctx context.Context, symlinkMap SymlinkMap, project *uplink.Project, bucket string) (err error) {
	uploadObj, err := project.UploadObject(ctx, bucket, StorjSymlinkMapKey, nil)
	if err != nil {
		return err
	}
	_, err = symlinkMap.WriteTo(uploadObj)
	if err != nil {
		_ = uploadObj.Abort()
		return err
	}
	return uploadObj.Commit()
}

// DownloadSymlinkMap downloads a symlink map from the given bucket, using the
// given open project information.
func DownloadSymlinkMap(ctx context.Context, project *uplink.Project, bucket string) (symlinkMap SymlinkMap, err error) {
	downloadObj, err := project.DownloadObject(ctx, bucket, StorjSymlinkMapKey, nil)
	if err != nil {
		return nil, err
	}
	defer common.DeferClose(downloadObj, &err)
	symlinkMap = NewSymlinkMap()
	_, err = symlinkMap.ReadFrom(downloadObj)
	if err != nil {
		return nil, err
	}
	return symlinkMap, nil
}
