// Copyright 2018 Google LLC
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

package method

import (
	"testing"
)

const (
	capMsg = `100 Capabilities
Send-Config: true
Pipeline: true
Single-Instance: yes
`

	// The trailing blank line is intentional
	acqMsg = `600 URI Acquire
URI: tardigrade://thisisnotreallyanaccessgrant/apt-repo-bucket/apt/generic/python-bernhard_0.2.3-1_all.deb
Filename: /tmp/python-bernhard_0.2.3-1_all.deb

600 URI Acquire
URI: tardigrade://thisisnotreallyanaccessgrant/apt-repo-bucket/apt/generic/riemann-sumd_0.7.2-1_all.deb
Filename: /tmp/riemann-sumd_0.7.2-1_all.deb

`

	// The trailing blank line is intentional
	configMsg = `601 Configuration
Config-Item: Dir::Log=var/log/apt
Config-Item: Dir::Log::Terminal=term.log
Config-Item: Dir::Log::History=history.log
Config-Item: Dir::Ignore-Files-Silently::=~$
Config-Item: Acquire::cdrom::mount=/media/cdrom
Config-Item: Aptitude::Get-Root-Command=sudo:/usr/bin/sudo
Config-Item: Unattended-Upgrade::Allowed-Origins::=${distro_id}:${distro_codename}-security

`
)

func TestCapabilities(t *testing.T) {
	actual := capabilities().String()
	if actual != capMsg {
		t.Errorf("capabilities() = %s; expected %s", actual, capMsg)
	}
}
