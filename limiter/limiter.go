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
