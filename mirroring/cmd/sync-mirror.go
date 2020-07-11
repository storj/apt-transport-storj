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

package main

import (
	"context"
	"flag"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"storj.io/apt-transport-storj/mirroring"
)

var (
	upstreamMirror = flag.String("upstream", "",
		"Hostname of the upstream mirror to sync from (must support rsync access)")
	upstreamPath = flag.String("up-path", "debian/",
		"Path to the mirror root on the upstream mirror")
	accessString = flag.String("access", "",
		"Serialized Storj access string allowing uploads to your mirror on Storj")
	bucket = flag.String("bucket", "debian",
		"Name of bucket in your Storj project to which the mirror will be synced")
	localDir = flag.String("local-dir", "/srv/mirrors/debian",
		"Path to a local directory where index and contents files will be synced")
	color = flag.Bool("color", false,
		"Enable color highlighting in logs")
	logFile = flag.String("log-file", "",
		"Send logs to the named file instead of stderr")
	logJSON = flag.Bool("log-json", false,
		"Encode log fields as JSON")
	debug = flag.Bool("debug", false,
		"Enable debug logging")
	dryRun = flag.Bool("dry-run", false,
		"Output what paths would be transferred, uploaded, and deleted, but don't perform any of those actions (note: lists sync from upstream is still performed)")
	progress = flag.Bool("show-progress", false,
		"Enable progress bar visualization. Setting -logFile too is highly recommended")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	startTime := time.Now()

	logConfig := zap.NewDevelopmentConfig()
	if *color {
		logConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	if *logFile != "" {
		logConfig.OutputPaths = []string{*logFile}
	}
	if *logJSON {
		logConfig.Encoding = "json"
	} else {
		logConfig.Encoding = "console"
	}
	if *debug {
		logConfig.Level.SetLevel(zap.DebugLevel)
	} else {
		logConfig.Level.SetLevel(zap.InfoLevel)
		logConfig.DisableStacktrace = true
	}
	var err error
	logger, err := logConfig.Build(zap.AddCaller())
	if err != nil {
		_, _ = os.Stderr.Write([]byte("Could not initialize logging: " + err.Error() + "\n"))
		os.Exit(1)
	}

	err = mirroring.SyncMirror(ctx, logger, *upstreamMirror, *upstreamPath, *accessString, *bucket, *localDir, *dryRun, *progress)
	if err != nil {
		logger.Fatal("Error syncing mirror", zap.Error(err))
	}

	endTime := time.Now()
	logger.Info("Sync completed", zap.Duration("duration", endTime.Sub(startTime)))
}
