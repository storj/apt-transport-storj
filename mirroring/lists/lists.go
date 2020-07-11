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

package lists

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"pault.ag/go/debian/control"
	"pault.ag/go/debian/deb"
	"storj.io/uplink"

	"storj.io/apt-transport-storj/common"
	"storj.io/apt-transport-storj/limiter"
	"storj.io/apt-transport-storj/mirroring/progress"
	"storj.io/apt-transport-storj/symlinks"
)

var (
	rsyncExecutable = flag.String("rsync", "rsync",
		"How to invoke rsync executable")
	concurrentListReads = flag.Int64("concurrent-list-reads", 10,
		"Maximum number of concurrent local list file read operations")
)

const (
	lsListFile = "ls-lR.gz"
)

// GetUpstreamFileList builds a list of all symlinks present on the upstream mirror (using,
// if available, the ls-lR.gz file that should be synced from the root of the mirror).
func GetUpstreamFileList(ctx context.Context, logger *zap.Logger, localDir, upstreamMirror, upstreamPath string) (symlinks.SymlinkMap, error) {
	// if the upstream mirror provides an ls-lR.gz, prefer that, as it's much less work for the server
	// and can transfer more quickly.
	logger.Debug("checking for local ls list")
	fileEntries, err := readLocalLsList(localDir)
	if err == nil {
		logger.Debug("local ls list read success")
		return fileEntries, nil
	}
	logger.Debug("local ls list read failed. falling back on querying rsync for file list", zap.Error(err))
	return getUpstreamRsyncList(ctx, upstreamMirror, upstreamPath)
}

func getUpstreamRsyncList(ctx context.Context, upstreamMirror, upstreamPath string) (symlinks.SymlinkMap, error) {
	cmd := exec.CommandContext(ctx, *rsyncExecutable, "-rlz", upstreamMirror+"::"+upstreamPath, "--filter=exclude_.~tmp~")
	stdout, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, errs.New("Failed to get remote file list via rsync (exit status %d). stderr follows:\n%s", exitErr.ExitCode(), exitErr.Stderr)
		}
		return nil, errs.New("Failed to get remote file list via rsync: %v", err)
	}

	return parseUpstreamRsyncList(stdout)
}

func readLocalLsList(localDir string) (_ symlinks.SymlinkMap, err error) {
	f, err := os.Open(path.Join(localDir, lsListFile))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errs.Combine(err, f.Close())
	}()

	return parseUpstreamLsList(f)
}

var entrySpace = regexp.MustCompile(`[ \t]+`)

func parseUpstreamLsList(gzippedContents io.Reader) (symlinkMap symlinks.SymlinkMap, err error) {
	r, err := gzip.NewReader(gzippedContents)
	if err != nil {
		return nil, err
	}
	defer func() { err = errs.Combine(err, r.Close()) }()

	type readState int
	const (
		expectDirName readState = iota
		expectLinkCount
		expectFileEntry
	)

	const (
		// example:
		// lrwxrwxrwx  1 dak debadmin   79 Jun 21 20:12 Contents-udeb-s390x.gz -> by-hash/SHA256/f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec
		lsFields         = 9
		symlinkSeparator = " -> "
	)

	state := expectDirName
	lineNum := 0
	dirName := ""
	symlinkMap = symlinks.NewSymlinkMap()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		switch state {
		case expectDirName:
			if len(line) < 2 || line[0] != '.' || line[len(line)-1] != ':' || (len(line) > 2 && line[1] != '/') {
				return nil, errs.New("unexpected content at line %d of ls-lR.gz: %q", lineNum, line)
			}
			dirName = line[:len(line)-1]
			state = expectLinkCount
		case expectLinkCount:
			if !strings.HasPrefix(line, "total ") {
				return nil, errs.New("unexpected content at line %d of ls-lR.gz: %q", lineNum, line)
			}
			state = expectFileEntry
		case expectFileEntry:
			if line == "" {
				state = expectDirName
				continue
			}
			parts := entrySpace.Split(line, lsFields)
			if len(parts) < lsFields {
				return nil, errs.New("unexpected content at line %d of ls-lR.gz: %q", lineNum, line)
			}
			pos := strings.Index(parts[lsFields-1], symlinkSeparator)
			if pos < 0 {
				// not a symlink
				continue
			}
			linkName := path.Join(dirName, parts[lsFields-1][:pos])
			linkDest := parts[lsFields-1][pos+len(symlinkSeparator):]
			symlinkMap.Add(linkName, linkDest)
		}
	}
	return symlinkMap, nil
}

func parseUpstreamRsyncList(contents []byte) (symlinks.SymlinkMap, error) {
	const (
		fileListStart = "receiving incremental file list"

		// example symlink line: lrwxrwxrwx             22 2020/06/22 12:08:48 dists/bullseye/ChangeLog -> ChangeLog.202006221008
		listFields       = 7
		symlinkSeparator = " -> "
	)

	inFileList := false
	lineNum := 0
	symlinkMap := symlinks.NewSymlinkMap()

	scanner := bufio.NewScanner(bytes.NewBuffer(contents))
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if !inFileList {
			if line == fileListStart {
				inFileList = true
			}
			continue
		}
		if line == "" {
			break
		}
		if line[0] != 'l' {
			// this isn't a symbolic link; ignore it
			continue
		}
		parts := entrySpace.Split(line, listFields)
		if len(parts) < listFields {
			return nil, errs.New("unexpected content at line %d of rsync list output: %q", lineNum, line)
		}
		if parts[5] != symlinkSeparator {
			return nil, errs.New("unexpected content at line %d of rsync list output: %q", lineNum, line)
		}
		linkName := parts[4]
		linkDest := parts[6]
		if len(linkDest) > 0 && linkDest[0] == '/' {
			// ignore symlink with absolute path
			continue
		}
		symlinkMap.Add(linkName, linkDest)
	}

	return symlinkMap, nil
}

// DoListsRsync performs an rsync operation to sync all list-like files to the local host.
// As it stands, this fetches all files other than source packages (*.tar.*, *.diff.*, *.dsc,
// *.changes, *.upload) and binary packages (*.deb, *.udeb). Those files are expected to make
// up the bulk of the mirror and should all be referenced by the corresponding list files, so
// we can gather enough information here to pull directly from the upstream mirror and upload
// to the Storj network at the same time, rather than keeping those files stored locally.
func DoListsRsync(ctx context.Context, upstreamMirror, upstreamPath, localDir string) error {
	cmd := exec.CommandContext(ctx, *rsyncExecutable,
		"-vmprltHSB8192",
		"--bwlimit=0",
		"--safe-links",
		"--timeout=120",
		"--stats",
		"--no-human-readable",
		"--no-inc-recursive",
		"--filter=exclude_.~tmp~",
		"--filter=exclude_/project/trace/_hierarchy",
		"--filter=protect_/project/trace/_hierarchy",
		"--filter=exclude_/project/trace/_traces",
		"--filter=protect_/project/trace/_traces",
		"--filter=include_/project/",
		"--filter=protect_/project/",
		"--filter=include_/project/trace/",
		"--filter=protect_/project/trace/",
		"--filter=include_/project/trace/*",
		"--filter=protect_/"+progress.SaveFilename,
		upstreamMirror+"::"+upstreamPath,
		strings.TrimRight(localDir, "/")+"/",
		"--filter=exclude_*.deb",
		"--filter=exclude_*.dsc",
		"--filter=exclude_*.udeb",
		"--filter=exclude_*.diff.*",
		"--filter=include_**/dep11/**",
		"--filter=exclude_*.tar.*",
		"--filter=exclude_*.changes",
		"--filter=exclude_*.upload",
		"--filter=exclude_**/by-hash/MD5Sum/**",
		"--filter=exclude_**/by-hash/SHA1/**",
		"--max-delete=40000",
		"--delay-updates",
		"--delete",
		"--delete-delay",
		"--delete-excluded",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// GetDownstreamFileList fetches a FileMap of all objects present on the downstream,
// with Timestamp set according to the objects' creation times, and Size set according
// to the objects' Content-Lengths.
func GetDownstreamFileList(ctx context.Context, project *uplink.Project, bucket string) (common.FileMap, error) {
	fileIter := project.ListObjects(ctx, bucket, &uplink.ListObjectsOptions{
		Recursive: true,
		System:    true,
	})
	entries := make(common.FileMap)
	for fileIter.Next() {
		item := fileIter.Item()
		entries[item.Key] = common.FileEntry{
			Path:      item.Key,
			Timestamp: item.System.Created,
			Size:      item.System.ContentLength,
		}
	}
	return entries, fileIter.Err()
}

// FindAllReferencedFilenames scans localDir, collecting a FileMap of all locally
// present files and also collecting a list of FileEntry records as found in all
// list files (files that reference source or binary packages on the mirror). List
// files have names that match "{Packages,Sources}{,.gz,.bz2,.xz,.lzma}".
func FindAllReferencedFilenames(ctx context.Context, logger *zap.Logger, localDir string, symlinkMap symlinks.SymlinkMap) (common.FileMap, []common.FileEntry, error) {
	localMap := make(common.FileMap)

	listTypes := []struct {
		fileName string
		readFunc func(ctx context.Context, f io.Reader) ([]common.FileEntry, error)
	}{
		{"Packages", readPackagesList},
		{"Sources", readSourcesList},
	}
	compressionTypes := []string{
		"",
		".gz",
		".bz2",
		".xz",
		".lzma",
	}

	type listFileReadJob struct {
		fileName        string
		compressionType string
		readFunc        func(ctx context.Context, r io.Reader) ([]common.FileEntry, error)
	}
	var toRead []listFileReadJob

	prefix := strings.TrimRight(localDir, "/") + "/"
	err := filepath.Walk(prefix, func(pathToItem string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			relativePath := path.Clean(pathToItem[len(prefix):])
			localMap[relativePath] = common.FileEntry{
				Path:      relativePath,
				Timestamp: info.ModTime(),
				Size:      info.Size(),
			}
			return nil
		}
		if !info.IsDir() {
			return nil
		}

		for _, listType := range listTypes {
			for _, compressionType := range compressionTypes {
				fileName := path.Join(pathToItem, listType.fileName+compressionType)
				if _, err := os.Stat(fileName); err != nil {
					if !os.IsNotExist(err) {
						logger.Warn("failed to stat list file", zap.String("path", fileName), zap.Error(err))
					}
					continue
				}
				toRead = append(toRead, listFileReadJob{
					fileName:        fileName,
					compressionType: compressionType,
					readFunc:        listType.readFunc,
				})
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	jobLimiter, ctx := limiter.NewJobLimiter(ctx, *concurrentListReads)
	var resultLock sync.Mutex
	numRead := 0
	referencedSet := make(map[string]common.FileEntry)

	for _, readJob := range toRead {
		readJob := readJob
		jobLimiter.AddJob(func() error {
			logger := logger.With(zap.String("action", "checkForList"), zap.String("filename", readJob.fileName), zap.String("compression-type", readJob.compressionType))
			f, err := os.Open(readJob.fileName)
			if err != nil {
				logger.Error("failed to open list file", zap.Error(err))
				return nil
			}
			defer func() {
				closeErr := f.Close()
				if closeErr != nil {
					logger.Warn("failed to close list file", zap.Error(err))
				}
			}()

			entries := readListFile(ctx, logger, f, readJob.compressionType, readJob.readFunc)
			resolvedEntries := make([]common.FileEntry, 0, len(entries))
			for _, entry := range entries {
				resolvedPath, err := symlinkMap.Resolve(entry.Path)
				if err != nil {
					logger.Error("could not resolve symlink", zap.String("path", entry.Path), zap.Error(err))
					continue
				}
				resolvedEntries = append(resolvedEntries, common.FileEntry{
					Path: resolvedPath,
					Size: entry.Size,
				})
			}

			resultLock.Lock()
			for _, entry := range resolvedEntries {
				referencedSet[entry.Path] = entry
			}
			numRead++
			soFar := numRead
			resultLock.Unlock()
			logger.Debug("list read progress", zap.String("progress", fmt.Sprintf("%d/%d", soFar, len(toRead))))
			return nil
		})
	}
	if err := jobLimiter.Wait(); err != nil {
		return nil, nil, err
	}

	allReferenced := make([]common.FileEntry, 0, len(referencedSet))
	for _, entry := range referencedSet {
		allReferenced = append(allReferenced, entry)
	}
	sort.Sort(byPath(allReferenced))
	return localMap, allReferenced, err
}

func readPackagesList(ctx context.Context, r io.Reader) ([]common.FileEntry, error) {
	paragraphs, err := control.ParseBinaryIndex(bufio.NewReader(&cancellableReader{ctx, r}))
	if err != nil {
		return nil, err
	}
	entries := make([]common.FileEntry, len(paragraphs))
	for i, paragraph := range paragraphs {
		entries[i].Path = paragraph.Filename
		size, err := strconv.ParseInt(paragraph.Size, 10, 64)
		if err != nil {
			return nil, errs.New("invalid package size %q for %q: %v", paragraph.Size, paragraph.Filename, err)
		}
		entries[i].Size = size
	}
	return entries, nil
}

func readSourcesList(ctx context.Context, r io.Reader) ([]common.FileEntry, error) {
	// just an estimate; sources usually have a .dsc, a .diff.gz, and an .orig.tar.gz
	const averageFilesPerSource = 3

	paragraphs, err := control.ParseSourceIndex(bufio.NewReader(&cancellableReader{ctx, r}))
	if err != nil {
		return nil, err
	}
	entries := make([]common.FileEntry, 0, len(paragraphs)*averageFilesPerSource)
	for _, paragraph := range paragraphs {
		sourceDir := paragraph.Directory
		for _, fileInfo := range paragraph.Files {
			entries = append(entries, common.FileEntry{
				Path: path.Join(sourceDir, fileInfo.Filename),
				Size: fileInfo.Size,
			})
		}
	}
	return entries, nil
}

func readListFile(ctx context.Context, logger *zap.Logger, listFile *os.File, compressionType string, readFunc func(ctx context.Context, r io.Reader) ([]common.FileEntry, error)) (entries []common.FileEntry) {
	logger.Debug("preparing to read list file")
	listReader, err := deb.DecompressorFor(compressionType)(listFile)
	if err != nil {
		logger.Warn("failed to decompress list file", zap.Error(err))
		return nil
	}
	if listReaderCloser, ok := listReader.(io.ReadCloser); ok {
		// we'll close listFile already- if listReaderCloser is the same object we won't want to close it twice.
		if fileObj, ok := listReaderCloser.(*os.File); !ok || fileObj != listFile {
			defer func() {
				if err := listReaderCloser.Close(); err != nil {
					logger.Warn("failed to close decompression reader", zap.Error(err))
				}
			}()
		}
	}

	entries, err = readFunc(ctx, listReader)
	if err != nil {
		logger.Warn("failed to parse list file", zap.Error(err))
		return nil
	}
	logger.Debug("successfully read from list file", zap.Int("num-entries", len(entries)))
	return entries
}

type cancellableReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr *cancellableReader) Read(p []byte) (n int, err error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}
	return cr.r.Read(p)
}

type byPath []common.FileEntry

func (a byPath) Len() int           { return len(a) }
func (a byPath) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byPath) Less(i, j int) bool { return a[i].Path < a[j].Path }
