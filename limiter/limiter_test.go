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

package limiter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	pausePeriod = time.Millisecond
)

func TestReaderWithContextWithPipeTimeout(t *testing.T) {
	performTimeoutTest(t)
}

func BenchmarkReaderWithContextWithPipeTimeout(b *testing.B) {
	for i := 0; i < b.N; i++ {
		performTimeoutTest(b)
	}
}

func performTimeoutTest(tb testing.TB) {
	rPipe, wPipe, err := os.Pipe()
	require.NoError(tb, err)
	defer func() { _ = wPipe.Close() }()
	defer func() { _ = rPipe.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	rwc := NewFileReaderWithContext(ctx, rPipe)

	var group errgroup.Group
	group.Go(func() error {
		time.Sleep(pausePeriod)
		//tb.Log("calling cancel")
		cancel()
		//tb.Log("done calling cancel")
		return nil
	})

	var readErr error
	var readN int
	group.Go(func() error {
		var buf [128]byte
		readN, readErr = rwc.Read(buf[:])
		if readErr == nil {
			return fmt.Errorf("read did not return error")
		}
		return readErr
	})

	err = group.Wait()
	require.Error(tb, err)
	require.Equal(tb, 0, readN)
	require.Equal(tb, err, readErr)
}

func TestReaderWithContextWithPipeSuccess(t *testing.T) {
	performSuccessfulReadTest(t)
}

func BenchmarkReaderWithContextWithPipeSuccess(b *testing.B) {
	for i := 0; i < b.N; i++ {
		performSuccessfulReadTest(b)
	}
}

func performSuccessfulReadTest(tb testing.TB) {
	rPipe, wPipe, err := os.Pipe()
	require.NoError(tb, err)
	defer func() { _ = wPipe.Close() }()
	defer func() { _ = rPipe.Close() }()

	writeN, err := wPipe.Write([]byte{1, 2, 3})
	require.NoError(tb, err)
	require.Equal(tb, 3, writeN)

	ctx, cancel := context.WithCancel(context.Background())
	rwc := NewFileReaderWithContext(ctx, rPipe)
	defer cancel()

	var buf [128]byte
	readN, err := rwc.Read(buf[:])
	require.NoError(tb, err)
	require.Equal(tb, 3, readN)
	require.Equal(tb, []byte{1, 2, 3}, buf[:readN])
}
