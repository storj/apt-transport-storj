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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	capMsg = `100 Capabilities
Send-Config: true
Pipeline: true
Single-Instance: yes
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
Config-Item: Acquire::Storj::EncryptionPassphrase=AS%2A%20XDRF%2A%26U

`
)

func TestCapabilities(t *testing.T) {
	require.Equal(t, capMsg, capabilities().String())
}

func TestSettingEncryptionPassphrase(t *testing.T) {
	reader := strings.NewReader(configMsg)
	method := New()

	err := method.processMessages(context.Background(), reader)
	require.NoError(t, err)
	require.Equal(t, "AS* XDRF*&U", method.encryptionPassphrase)
}
