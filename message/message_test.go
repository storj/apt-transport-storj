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

package message

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	fakeMsg = `700 Fake Description
Foo: bar
Baz: false
Filename: apt-transport.deb
`
	configMsg = `601 Configuration
Config-Item: APT::Architecture=amd64
Config-Item: APT::Build-Essential::=build-essential
Config-Item: APT::Color::Highlight=%1b[32m
Config-Item: APT::Update::Post-Invoke-Success::=touch%20/var/lib/apt/periodic/update-success-stamp%202>/dev/null%20||%20true
Config-Item: Acquire::cdrom::mount=/media/cdrom
Config-Item: Aptitude::Get-Root-Command=sudo:/usr/bin/sudo
Config-Item: CommandLine::AsString=apt-get%20install%20riemann-sumd
Config-Item: DPkg::Post-Invoke::=if%20[%20-d%20/var/lib/update-notifier%20];%20then%20touch%20/var/lib/update-notifier/dpkg-run-stamp;%20fi;%20if%20[%20-e%20/var/lib/update-notifier/updates-available%20];%20then%20echo%20>%20/var/lib/update-notifier/updates-available;%20fi%20
Config-Item: DPkg::Pre-Install-Pkgs::=/usr/sbin/dpkg-preconfigure%20--apt%20||%20true
Config-Item: Dir::State=var/lib/apt/
Config-Item: Dir=/
Config-Item: Unattended-Upgrade::Allowed-Origins::=${distro_id}:${distro_codename}-security
Config-Item: APT::Compressor::lzma::UncompressArg::=--format%3dlzma
`
	lzmaUncompressArgDecoded = `--format=lzma`

	acqMsg = `600 URI Acquire
URI: tardigrade://satellite.tardigrade.io:1234/fakeapikey/my-fake-bucket/apt/generic/python-bernhard_0.2.3-1_all.deb
Filename: /var/cache/apt/archives/partial/python-bernhard_0.2.3-1_all.deb
`
	acqMsgNoSpaces = `600 URI Acquire
URI:tardigrade://satellite.tardigrade.io:1234/fakeapikey/project-a/dists/trusty/main/binary-amd64/Packages
Filename:Packages.downloaded
Fail-Ignore:true
Index-File:true
`
)

func TestMessageString(t *testing.T) {
	h := &Header{Status: 700, Description: "Fake Description"}
	f := []*Field{
		{Name: "Foo", Value: "bar"},
		{Name: "Baz", Value: "false"},
		{Name: "Filename", Value: "apt-transport.deb"},
	}
	m := &Message{Header: h, Fields: f}

	require.Equal(t, fakeMsg, m.String())
}

func TestParseConfigurationMsg(t *testing.T) {
	m, err := FromBytes([]byte(configMsg))
	require.NoErrorf(t, err, "failed to parse %q into a message", configMsg)

	require.Len(t, m.Fields, 13)
	require.Equal(t, "Config-Item: APT::Architecture=amd64", m.Fields[0].String())

	fields := m.GetFieldList("Config-Item")
	require.Len(t, fields, 13)

	configKey := "APT::Compressor::lzma::UncompressArg::"
	found := false
	for _, f := range fields {
		parts := strings.Split(f.Value, "=")
		require.Len(t, parts, 2)
		if parts[0] == configKey {
			found = true
			value, err := url.PathUnescape(parts[1])
			require.NoError(t, err)
			require.Equal(t, lzmaUncompressArgDecoded, value)
		}
	}
	require.True(t, found, "Did not find config item DPkg::Post-Invoke::")
}

func TestGetFieldValue(t *testing.T) {
	h := &Header{Status: 700, Description: "Fake Description"}
	f := []*Field{
		{Name: "Foo", Value: "bar"},
		{Name: "Baz", Value: "qux"},
		{Name: "Filename", Value: "apt-transport.deb"},
	}
	m := &Message{Header: h, Fields: f}

	actual, present := m.GetFieldValue("Foo")
	require.True(t, present, "No Foo field found")
	require.Equal(t, "bar", actual)
	actual, present = m.GetFieldValue("Baz")
	require.True(t, present, "No Baz field found")
	require.Equal(t, "qux", actual)
	actual, present = m.GetFieldValue("Filename")
	require.True(t, present, "No Filename field found")
	require.Equal(t, "apt-transport.deb", actual)
}

func TestGetFieldList(t *testing.T) {
	h := &Header{Status: 700, Description: "Fake Description"}
	f := []*Field{
		{Name: "Config-Item", Value: "bar"},
		{Name: "Config-Item", Value: "qux"},
		{Name: "Filename", Value: "apt-transport.deb"},
	}
	m := &Message{Header: h, Fields: f}

	require.Len(t, m.GetFieldList("Config-Item"), 2)
}

func TestParseAcquireMsg(t *testing.T) {
	m, err := FromBytes([]byte(acqMsg))
	require.NoErrorf(t, err, "Failed to parse %q into a message", acqMsg)
	require.Len(t, m.Fields, 2)

	require.Equal(t, 600, m.Header.Status)
	require.Equal(t, "URI Acquire", m.Header.Description)

	value, present := m.GetFieldValue("Filename")
	require.True(t, present)
	require.Equal(t, "/var/cache/apt/archives/partial/python-bernhard_0.2.3-1_all.deb", value)
}

func TestParseFieldsWithMissingSpaces(t *testing.T) {
	m, err := FromBytes([]byte(acqMsgNoSpaces))
	require.NoErrorf(t, err, "Failed to parse %q into a message", acqMsgNoSpaces)
	require.Len(t, m.Fields, 4)

	field := m.Fields[0]
	require.Equal(t, "URI", field.Name)
	require.Equal(t, "tardigrade://satellite.tardigrade.io:1234/fakeapikey/project-a/dists/trusty/main/binary-amd64/Packages", field.Value)
}
