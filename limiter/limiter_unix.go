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
	"golang.org/x/sys/unix"
)

func readOrCancel(reader FDReader, cancelFD int, b []byte) (n int, canceled bool, err error) {
	ffd := int(reader.Fd())
	pollFDs := []unix.PollFd{
		{Fd: int32(ffd), Events: unix.POLLIN | unix.POLLHUP},
		{Fd: int32(cancelFD), Events: unix.POLLHUP},
	}

	for {
		_, err := unix.Poll(pollFDs, -1)
		if err != nil {
			return 0, false, err
		}

		for _, ev := range pollFDs {
			if ev.Revents != 0 {
				switch int(ev.Fd) {
				case ffd:
					n, err = reader.Read(b)
					return n, false, err
				case cancelFD:
					return 0, true, nil
				}
			}
		}
	}
}
