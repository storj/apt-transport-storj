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

package mirroring

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"storj.io/uplink"

	"storj.io/apt-transport-storj/common"
	"storj.io/apt-transport-storj/limiter"
	"storj.io/apt-transport-storj/mirroring/lists"
	"storj.io/apt-transport-storj/mirroring/progress"
	"storj.io/apt-transport-storj/symlinks"
)

var (
	concurrentUploads = flag.Int64("concurrent-uploads", 30,
		"Number of concurrent object uploads to issue to Storj network (note that each one may involve many separate TCP connections)")
	concurrentDeletes = flag.Int64("concurrent-deletes", 30,
		"Number of concurrent deletes to issue to Storj network")
	upstreamMirrorHTTPPort = flag.Int("upstream-mirror-http-port", 80,
		"HTTP port to use when fetching from upstream mirror via HTTP")
	continuePrevious = flag.Bool("continue", false,
		"Continue a previous unfinished mirror sync run. The localdir must be unchanged from the previous run.")
	deleteLimit = flag.Int("delete-limit", 40000,
		"Do not perform deletes if the number of objects to delete is higher than this value. A high number of planned deletes may be indicative of a bug.")
)

var (
	excludeFromDelete = map[string]struct{}{
		symlinks.StorjSymlinkMapKey: {},
	}
	excludeFromPush = map[string]struct{}{
		progress.SaveFilename: {},
	}
)

// SyncMirror is the toplevel function used to synchronize a Debian-style mirror on the Storj network
// ("downstream") using a conventional HTTP+rsync mirror ("upstream") as the source.
func SyncMirror(ctx context.Context, logger *zap.Logger, upstreamMirror, upstreamPath, accessString, bucket, localDir string, dryRun, showProgress bool) (err error) {
	ctx = contextWithLogger(ctx, logger)
	var st *progress.SyncState

	// try to get RLIMIT_NOFILE at least up to *concurrentUploads * RSConfig.TotalThreshold
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		logger.Warn("could not inspect limit on file descriptors", zap.Error(err))
	} else {
		existingLimit := rLimit.Cur
		rLimit.Cur = uint64(*concurrentUploads * 150)
		if rLimit.Cur > rLimit.Max {
			rLimit.Cur = rLimit.Max
		}
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
			logger.Warn("could not raise limit on file descriptors", zap.Uint64("current-value", existingLimit), zap.Uint64("desired-value", rLimit.Cur), zap.Error(err))
		}
	}

	status(logger, "Opening Storj project")
	accessGrant, err := uplink.ParseAccess(accessString)
	if err != nil {
		return errs.New("failed to parse access string: %v", err)
	}
	project, err := uplink.OpenProject(ctx, accessGrant)
	if err != nil {
		return errs.New("failed to open project: %v", err)
	}

	if *continuePrevious {
		st, err = progress.ReadPriorState(localDir)
		if err != nil {
			return err
		}
	} else {
		st = &progress.SyncState{}
		group, ctx := errgroup.WithContext(ctx)

		// Get information about what we have downstream and upstream at the same time
		group.Go(func() (err error) {
			st.DownstreamMap, err = gatherDownstreamState(ctx, project, bucket)
			return err
		})
		group.Go(func() (err error) {
			st.SymlinkMap, st.LocalMap, st.ReferencedFilenames, err = gatherUpstreamState(ctx, localDir, upstreamMirror, upstreamPath)
			return err
		})
		err := group.Wait()
		if err != nil {
			return err
		}

		status(logger, "Determining files to transfer or delete")
		st.TransferFiles, st.PushFiles, st.DeleteFiles, err = compareFilenames(st.ReferencedFilenames, st.LocalMap, st.DownstreamMap)
		if err != nil {
			return err
		}

		if err := st.SaveProgress(localDir); err != nil {
			return err
		}
	}
	defer func() { err = errs.Combine(err, st.Close()) }()

	if dryRun {
		return outputPlans(os.Stdout, st)
	}

	status(logger, "Transferring files from upstream to downstream")
	if len(st.TransferFiles) > 0 {
		err = doTransferFiles(ctx, st, upstreamMirror, upstreamPath, project, bucket)
		if err != nil {
			return err
		}
	}
	status(logger, "Pushing updated index-like files to downstream")
	err = pushNewLists(ctx, st, project, bucket, localDir)
	if err != nil {
		if ctx.Err() != nil {
			logger.Error("(error and context is canceled)")
		} else {
			logger.Error("(error and context is not canceled)")
		}
		return err
	}
	status(logger, "Pushing new symlink map to downstream")
	err = symlinks.UploadSymlinkMap(ctx, st.SymlinkMap, project, bucket)
	if err != nil {
		return err
	}
	if err := st.UpdateProgress(progress.ActionSentSymlinkMap, ""); err != nil {
		return err
	}
	status(logger, "Deleting unreferenced files from downstream")
	successes, err := deleteUnreferencedFiles(ctx, st, project, bucket)
	if err != nil {
		return err
	}
	if successes < len(st.DeleteFiles) {
		logger.Warn("not all deletions succeeded", zap.Int("attempted", len(st.DeleteFiles)), zap.Int("succeeded", successes))
	}
	return st.Clear()
}

func gatherDownstreamState(ctx context.Context, project *uplink.Project, bucket string) (downstreamMap common.FileMap, err error) {
	logger := loggerFor(ctx)
	status(logger, "Fetching downstream file list")
	downstreamMap, err = lists.GetDownstreamFileList(ctx, project, bucket)
	if err != nil {
		return nil, err
	}
	logger.Debug("downstream listing complete", zap.Int("num-entries", len(downstreamMap)))
	return downstreamMap, nil
}

func gatherUpstreamState(ctx context.Context, localDir, upstreamMirror, upstreamPath string) (symlinkMap symlinks.SymlinkMap, localMap common.FileMap, referencedFilenames []common.FileEntry, err error) {
	logger := loggerFor(ctx)
	status(logger, "Performing rsync of index-like files from upstream")
	err = lists.DoListsRsync(ctx, upstreamMirror, upstreamPath, localDir)
	if err != nil {
		return nil, nil, nil, err
	}
	status(logger, "Building symlink map")
	symlinkMap, err = lists.GetUpstreamFileList(ctx, logger, localDir, upstreamMirror, upstreamPath)
	if err != nil {
		return nil, nil, nil, err
	}
	status(logger, "Extracting referenced filenames from synced index files")
	localMap, referencedFilenames, err = lists.FindAllReferencedFilenames(ctx, logger, localDir, symlinkMap)
	if err != nil {
		return nil, nil, nil, err
	}
	return symlinkMap, localMap, referencedFilenames, nil
}

func status(logger *zap.Logger, s string) {
	logger.WithOptions(zap.AddCallerSkip(1)).
		Info("********************* STATUS UPDATE", zap.String("status", s))
}

// Conceptually:
//
// * The to-transfer list is all entries in referencedFilenames that do not exist
//   (or exist but have different sizes) in downstreamFiles.
// * The to-push list is all entries in localMap that do not exist (or exist but
//   have earlier timestamps) in downstreamFiles.
// * The to-delete list is all entries that exist in downstreamFiles but have no
//   corresponding entry in referencedFilenames or localMap.
func compareFilenames(referencedFilenames []common.FileEntry, localMap common.FileMap, downstreamMap common.FileMap) (toTransfer, toPush, toDelete []string, err error) {
	referencedSet := make(map[string]struct{})
	for _, entry := range referencedFilenames {
		referencedSet[entry.Path] = struct{}{}
	}

	for key, entry := range downstreamMap {
		if _, ok := referencedSet[key]; ok {
			// don't need to delete this; it's still referenced by indexes on the upstream mirror
			continue
		}
		if _, ok := localMap[key]; ok {
			// don't need to delete this; it's still an index on the upstream mirror
			continue
		}
		if _, ok := excludeFromDelete[key]; ok {
			continue
		}
		toDelete = append(toDelete, entry.Path)
	}
	for _, entry := range referencedFilenames {
		if existingEntry, ok := downstreamMap[entry.Path]; ok {
			if existingEntry.Size == entry.Size {
				// don't need to transfer this; it's already downstream. Really we
				// probably shouldn't even need to check the size; according to my
				// understanding of Debian-style mirrors, a non-index file with a
				// given name will never change contents once written. Still, this
				// check is included for safety's sake.
				continue
			}
		}
		toTransfer = append(toTransfer, entry.Path)
	}
	for _, entry := range localMap {
		if existingEntry, ok := downstreamMap[entry.Path]; ok {
			if existingEntry.Timestamp.After(entry.Timestamp) && existingEntry.Size == entry.Size {
				// don't need to transfer this; it's already downstream.
				continue
			}
		}
		if _, ok := excludeFromPush[entry.Path]; ok {
			continue
		}
		toPush = append(toPush, entry.Path)
	}
	return toTransfer, toPush, toDelete, nil
}

func doTransferFiles(ctx context.Context, st *progress.SyncState, upstreamMirror, upstreamPath string, project *uplink.Project, bucket string) error {
	logger := loggerFor(ctx).With(zap.String("action", "transfer"))
	jobLimiter, ctx := limiter.NewJobLimiter(ctx, *concurrentUploads)
	httpHost := upstreamMirror
	if *upstreamMirrorHTTPPort != 80 {
		httpHost = fmt.Sprintf("%s:%d", httpHost, *upstreamMirrorHTTPPort)
	}
	urlBase := (&url.URL{
		Scheme: "http",
		Host:   httpHost,
		Path:   strings.TrimRight(upstreamPath, "/") + "/",
	}).String()
	var numSent int32
	var total = int32(len(st.TransferFiles))
	for _, fileKey := range st.TransferFiles {
		fileKey := fileKey
		jobLimiter.AddJob(func() error {
			fullURL := urlBase + fileKey
			logger := logger.With(zap.String("key", fileKey), zap.String("fullURL", fullURL))
			logger.Debug("beginning transfer from upstream to downstream")
			startTime := time.Now()
			size, err := transferFile(ctx, fullURL, project, bucket, fileKey)
			if err != nil {
				logger.Error("failed to transfer", zap.Error(err))
				return nil
			}
			duration := time.Since(startTime)
			// megabits/second = bits/microsecond
			megaBitsPerSec := float64(8*time.Microsecond) * float64(size) / float64(duration)
			soFar := atomic.AddInt32(&numSent, 1)
			logger.Info("transferred", zap.Duration("duration", duration), zap.Int64("size", size), zap.Float64("Mbps", megaBitsPerSec), zap.String("progress", fmt.Sprintf("%d/%d", soFar, total)))
			return st.UpdateProgress(progress.ActionTransfer, fileKey)
		})
	}
	return jobLimiter.Wait()
}

func transferFile(ctx context.Context, fullURL string, project *uplink.Project, bucket, fileKey string) (int64, error) {
	resp, err := http.Get(fullURL)
	if err != nil {
		return 0, fmt.Errorf("http GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http GET failed with %d: %s", resp.StatusCode, resp.Status)
	}
	defer func() { _ = resp.Body.Close() }()
	upload, err := project.UploadObject(ctx, bucket, fileKey, nil)
	if err != nil {
		return 0, errs.New("failed to create upload for downstream transfer: %v", err)
	}
	n, err := io.Copy(upload, resp.Body)
	if err != nil {
		err = errs.New("failed to transfer from upstream to downstream: %v", err)
		return 0, errs.Combine(err, upload.Abort())
	}
	if err := upload.Commit(); err != nil {
		return 0, errs.New("failed to commit transfer upload: %v", err)
	}
	return n, nil
}

func pushNewLists(ctx context.Context, st *progress.SyncState, project *uplink.Project, bucket, localDir string) error {
	jobLimiter, ctx := limiter.NewJobLimiter(ctx, *concurrentUploads)
	logger := loggerFor(ctx).With(zap.String("action", "push-lists"), zap.String("local-dir", localDir))

	var numSent int32
	var total = int32(len(st.PushFiles))
	for _, pushPath := range st.PushFiles {
		if err := ctx.Err(); err != nil {
			return err
		}
		pushPath := pushPath
		jobLimiter.AddJob(func() error {
			localName := path.Join(localDir, pushPath)
			logger := logger.With(zap.String("key", pushPath))
			f, err := os.Open(localName)
			if err != nil {
				logger.Error("failed to open local file", zap.Error(err))
				return nil
			}
			startTime := time.Now()
			upload, err := project.UploadObject(ctx, bucket, pushPath, nil)
			if err != nil {
				logger.Error("failed to initialize upload", zap.Error(err))
				return nil
			}
			n, err := io.Copy(upload, f)
			if err != nil {
				logger.Error("failed to perform upload", zap.Error(err))
				if err := upload.Abort(); err != nil {
					logger.Error("failed to abort upload", zap.Error(err))
				}
				return nil
			}
			if err := upload.Commit(); err != nil {
				logger.Error("failed to commit upload", zap.Error(err))
				return nil
			}
			endTime := time.Now()
			soFar := atomic.AddInt32(&numSent, 1)
			logger.Info("transferred", zap.Duration("duration", endTime.Sub(startTime)), zap.Int64("size", n), zap.String("progress", fmt.Sprintf("%d/%d", soFar, total)))
			return st.UpdateProgress(progress.ActionPush, pushPath)
		})
	}
	return jobLimiter.Wait()
}

func deleteUnreferencedFiles(ctx context.Context, st *progress.SyncState, project *uplink.Project, bucket string) (successes int, err error) {
	if len(st.DeleteFiles) > *deleteLimit {
		return 0, fmt.Errorf("refusing to perform %d deletions (delete-limit = %d)", len(st.DeleteFiles), *deleteLimit)
	}

	var numDeleted int32
	var total = int32(len(st.DeleteFiles))
	logger := loggerFor(ctx).With(zap.String("action", "delete"))
	jobLimiter, ctx := limiter.NewJobLimiter(ctx, *concurrentDeletes)

	for _, deletePath := range st.DeleteFiles {
		deletePath := deletePath
		jobLimiter.AddJob(func() error {
			logger := logger.With(zap.String("key", deletePath))
			logger.Debug("issuing delete")
			startTime := time.Now()
			_, err := project.DeleteObject(ctx, bucket, deletePath)
			if err != nil && err != uplink.ErrObjectNotFound {
				logger.Error("delete failure", zap.Error(err))
				return nil
			}
			endTime := time.Now()
			soFar := atomic.AddInt32(&numDeleted, 1)
			logger.Info("deleted", zap.Duration("duration", endTime.Sub(startTime)), zap.String("progress", fmt.Sprintf("%d/%d", soFar, total)))
			return st.UpdateProgress(progress.ActionDelete, deletePath)
		})
	}
	err = jobLimiter.Wait()
	return int(numDeleted), err
}

func outputPlans(w io.Writer, st *progress.SyncState) error {
	maxNum := len(st.TransferFiles)
	if len(st.PushFiles) > maxNum {
		maxNum = len(st.PushFiles)
	}
	if len(st.DeleteFiles) > maxNum {
		maxNum = len(st.DeleteFiles)
	}
	maxWidth := len(strconv.Itoa(maxNum))

	if _, err := fmt.Fprintf(w, "* Would transfer %d paths directly from upstream to downstream:\n", len(st.TransferFiles)); err != nil {
		return err
	}
	for i, toTransfer := range st.TransferFiles {
		if _, err := fmt.Fprintf(w, "T%0*d %s\n", maxWidth, i, toTransfer); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "* Would upload %d list files from local volume:\n", len(st.PushFiles)); err != nil {
		return err
	}
	for i, toPush := range st.PushFiles {
		if _, err := fmt.Fprintf(w, "P%0*d %s\n", maxWidth, i, toPush); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "* Would delete %d objects from downstream bucket:\n", len(st.DeleteFiles)); err != nil {
		return err
	}
	for i, toDelete := range st.DeleteFiles {
		if _, err := fmt.Fprintf(w, "D%0*d %s\n", maxWidth, i, toDelete); err != nil {
			return err
		}
	}
	return nil
}

type loggerContextKey int

func contextWithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey(0), logger)
}

func loggerFor(ctx context.Context) *zap.Logger {
	logger := ctx.Value(loggerContextKey(0))
	if logger != nil {
		return logger.(*zap.Logger)
	}
	return zap.L()
}
