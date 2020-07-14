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
	"io"
	"syscall"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// JobLimiter runs jobs in parallel, up to a limit.
type JobLimiter struct {
	sem   *semaphore.Weighted
	ctx   context.Context
	group *errgroup.Group
}

// NewJobLimiter creates a new JobLimiter instance limited to the given maximum
// number of concurrent jobs.
func NewJobLimiter(ctx context.Context, n int64) (*JobLimiter, context.Context) {
	group, ctx := errgroup.WithContext(ctx)
	return &JobLimiter{
		sem:   semaphore.NewWeighted(n),
		ctx:   ctx,
		group: group,
	}, ctx
}

// AddJob waits until the number of running jobs is less than the specified limit,
// then starts the given job in a goroutine. The call to AddJob will block until
// the goroutine can be started, providing backpressure if desired.
//
// If the job returns non-nil, the context will be canceled. If the context is
// canceled (because of a failed job or otherwise) the job might not run.
func (jl *JobLimiter) AddJob(job func() error) {
	if err := jl.sem.Acquire(jl.ctx, 1); err != nil {
		return
	}
	jl.group.Go(func() error {
		defer jl.sem.Release(1)
		return job()
	})
}

// Wait waits until all submitted jobs are complete and returns the first error
// returned by a job (if any). AddJob should not be called again after Wait.
func (jl *JobLimiter) Wait() error {
	return jl.group.Wait()
}

// FDReader is an interface for a reader which also exposes an Fd method for
// getting a corresponding file descriptor. The file descriptor will be used
// to determine when the reader can be read from without blocking.
//
// Note that using File().Fd() on net.IPConn, net.TCPConn, net.UnixConn, etc
// will not work; that duplicates the file descriptor of the underlying file,
// and does not return the actual underlying file.
type FDReader interface {
	io.Reader
	Fd() uintptr
}

// NewFileReaderWithContext returns a reader which, if the given context is
// canceled, will return early from read calls. Both ongoing read calls and
// subsequent read calls (made after the context is closed) will return the
// context error instead of waiting for data to arrive.
func NewFileReaderWithContext(ctx context.Context, fr FDReader) *ReaderWithContext {
	return &ReaderWithContext{Ctx: ctx, FDReader: fr}
}

// ReaderWithContext is a reader that will return early from Read calls if
// the corresponding context is closed.
type ReaderWithContext struct {
	Ctx      context.Context
	FDReader FDReader
}

// Read reads from the reader. It will return early if the associated context
// is closed.
func (rwc *ReaderWithContext) Read(b []byte) (n int, err error) {
	// shortcut, in case context is already closed (it can't become unclosed
	// again, so no race here)
	if err := rwc.Ctx.Err(); err != nil {
		return 0, err
	}

	// create a pipe; we will close the write end when the sub-context is closed
	// (which will happen if rwc.Ctx is closed, or when this function returns).
	var pipeFDs [2]int
	err = syscall.Pipe(pipeFDs[:])
	if err != nil {
		return 0, err
	}
	pipeRead := pipeFDs[0]
	pipeWrite := pipeFDs[1]
	ctx, cancel := context.WithCancel(rwc.Ctx)
	defer func() {
		_ = syscall.Close(pipeRead)
	}()
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = syscall.Close(pipeWrite)
	}()

	n, canceled, err := readOrCancel(rwc.FDReader, pipeRead, b)
	if err != nil {
		return n, err
	}
	if canceled {
		return 0, rwc.Ctx.Err()
	}
	return n, nil
}
