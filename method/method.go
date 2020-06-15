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

// Package method implements functions to satisfy the method interface of the APT
// software package manager. For more information about the APT method interface
// see, http://www.fifi.org/doc/libapt-pkg-doc/method.html/ch2.html#s2.3.
package method

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zeebo/errs"
	"golang.org/x/sync/errgroup"
	"storj.io/uplink"

	"storj.io/apt-transport-tardigrade/message"
)

const (
	headerCodeCapabilities   = 100
	headerCodeGeneralLog     = 101
	headerCodeStatus         = 102
	headerCodeURIStart       = 200
	headerCodeURIDone        = 201
	headerCodeURIFailure     = 400
	headerCodeGeneralFailure = 401
	headerCodeURIAcquire     = 600
	headerCodeConfiguration  = 601
)

const (
	headerDescriptionCapabilities   = "Capabilities"
	headerDescriptionGeneralLog     = "Log"
	headerDescriptionStatus         = "Status"
	headerDescriptionURIStart       = "URI Start"
	headerDescriptionURIDone        = "URI Done"
	headerDescriptionURIFailure     = "URI Failure"
	headerDescriptionGeneralFailure = "General Failure"
	headerDescriptionURIAcquire     = "URI Acquire"
	headerDescriptionConfiguration  = "Configuration"
)

const (
	fieldNameCapabilities   = "Capabilities"
	fieldNameConfigItem     = "Config-Item"
	fieldNameSendConfig     = "Send-Config"
	fieldNamePipeline       = "Pipeline"
	fieldNameSingleInstance = "Single-Instance"
	fieldNameURI            = "URI"
	fieldNameFilename       = "Filename"
	fieldNameSize           = "Size"
	fieldNameLastModified   = "Last-Modified"
	fieldNameMessage        = "Message"
	fieldNameMD5Hash        = "MD5-Hash"
	fieldNameMD5SumHash     = "MD5Sum-Hash"
	fieldNameSHA1Hash       = "SHA1-Hash"
	fieldNameSHA256Hash     = "SHA256-Hash"
	fieldNameSHA512Hash     = "SHA512-Hash"
)

const (
	fieldValueNotFound = "The specified key does not exist."
)

// A Method implements the logic to process incoming apt messages and respond
// accordingly.
type Method struct {
	msgChan       chan []byte
	stdout        *log.Logger
	clientMapLock sync.Mutex
	ctx           context.Context
	waitGroup     *errgroup.Group
	storjClients  map[string]*uplink.Project
}

// New returns a new Method configured to read from os.Stdin and write to
// os.Stdout.
func New() *Method {
	m := &Method{
		msgChan:      make(chan []byte),
		stdout:       log.New(os.Stdout, "", 0),
		storjClients: make(map[string]*uplink.Project),
	}
	return m
}

// Run flushes the Method's capabilities and then begins reading messages from
// os.Stdin. Results are written to os.Stdout. The running Method waits for all
// Messages to be processed before exiting.
func (m *Method) Run(ctx context.Context) {
	m.flushCapabilities()
	m.processMessages(ctx, os.Stdin)
}

func (m *Method) flushCapabilities() {
	msg := capabilities()
	m.stdout.Println(msg)
}

func capabilities() *message.Message {
	header := header(headerCodeCapabilities, headerDescriptionCapabilities)
	fields := []*message.Field{
		field(fieldNameSendConfig, "true"),
		field(fieldNamePipeline, "true"),
		field(fieldNameSingleInstance, "yes"),
	}
	return &message.Message{Header: header, Fields: fields}
}

// processMessages loops over the channel of Messages
// and starts a goroutine to process each Message.
func (m *Method) processMessages(ctx context.Context, input io.Reader) {
	scanner := bufio.NewScanner(input)
	buffer := &bytes.Buffer{}
	waitGroup, ctx := errgroup.WithContext(ctx)
	for {
		hasLine := scanner.Scan()
		if hasLine {
			line := fmt.Sprintf("%s\n", scanner.Text())
			buffer.WriteString(line)
			trimmed := strings.TrimRight(line, "\n")

			// Messages are terminated with a blank line. If a line with no content
			// comes in and the buffer already has some content, it's assuming that
			// the buffer currently contains a complete message ready to be processed.
			if len(trimmed) == 0 && buffer.Len() > 3 {
				waitGroup.Go(func() error {
					m.handleBytes(ctx, buffer.Bytes())
					return nil
				})
				buffer = &bytes.Buffer{}
			}
		} else {
			break
		}
	}
	err := waitGroup.Wait()
	if err != nil {
		m.outputGeneralFailure(err)
	}
}

// handleBytes initializes a new Message and dispatches it according to
// the Message.Header.Status value.
func (m *Method) handleBytes(ctx context.Context, b []byte) {
	msg, err := message.FromBytes(b)
	if err != nil {
		m.outputGeneralFailure(err)
		return
	}
	if msg.Header.Status == headerCodeURIAcquire {
		// URI Acquire message
		err := m.uriAcquire(ctx, msg)
		if err != nil {
			m.outputGeneralFailure(err)
			return
		}
	} else if msg.Header.Status == headerCodeConfiguration {
		m.outputGeneralFailure(errors.New("we don't need no configuration"))
	}
}

func (m *Method) getClient(ctx context.Context, grant string) (_ *uplink.Project, err error) {
	m.clientMapLock.Lock()
	defer m.clientMapLock.Unlock()

	client, ok := m.storjClients[grant]
	if !ok {
		client, err = m.storjClient(ctx, grant)
		if err != nil {
			return nil, err
		}
		m.storjClients[grant] = client
	}
	return client, nil
}

func uriParse(uri string) (accessGrant, bucket, objectKey string, err error) {
	uriObject, err := url.Parse(uri)
	if err != nil {
		return "", "", "", err
	}
	pathParts := filepath.SplitList(uriObject.RawPath)
	if len(pathParts) < 2 {
		return "", "", "", fmt.Errorf("invalid Tardigrade source URI %q", uri)
	}
	bucket = pathParts[0]
	objectKey = strings.Join(pathParts[1:], "/")
	return uriObject.Host, bucket, objectKey, nil
}

// uriAcquire downloads and stores objects from S3 based on the contents
// of the provided Message.
func (m *Method) uriAcquire(ctx context.Context, msg *message.Message) error {
	uri, hasField := msg.GetFieldValue(fieldNameURI)
	if !hasField {
		return errors.New("acquire message missing required field: URI")
	}
	grant, bucket, path, err := uriParse(uri)
	if err != nil {
		return err
	}
	client, err := m.getClient(ctx, grant)
	if err != nil {
		return err
	}

	download, err := client.DownloadObject(ctx, bucket, path, nil)
	if err != nil {
		return err
	}
	downloadInfo := download.Info()
	expectedLen := downloadInfo.System.ContentLength
	lastModified := downloadInfo.System.Created
	if err := m.outputURIStart(uri, expectedLen, lastModified); err != nil {
		return err
	}

	filename, hasField := msg.GetFieldValue(fieldNameFilename)
	if !hasField {
		return errors.New("acquire message missing required field: Filename")
	}
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		err = errs.Combine(err, file.Close())
	}()

	numBytes, err := io.Copy(file, download)
	if err != nil {
		return err
	}
	if err := m.outputURIDone(uri, numBytes, lastModified, filename); err != nil {
		return err
	}
	return nil
}

// storjClient provides an initialized client blah blah
func (m *Method) storjClient(ctx context.Context, grant string) (*uplink.Project, error) {
	access, err := uplink.ParseAccess(grant)
	if err != nil {
		return nil, err
	}
	return uplink.OpenProject(ctx, access)
}

// requestStatus constructs a Message that when printed looks like the
// following example:
//
// 102 Status
// URI: tardigrade://fake-serialized-access/bucket-name/apt/trusty/riemann-sumd_0.7.2-1_all.deb
// Message: Connecting to $satellite
func requestStatus(objectURI, status string) *message.Message {
	h := header(headerCodeStatus, headerDescriptionStatus)
	uriField := field(fieldNameURI, objectURI)
	messageField := field(fieldNameMessage, status)
	return &message.Message{Header: h, Fields: []*message.Field{uriField, messageField}}
}

// uriStart constructs a Message that when printed looks like the following
// example:
//
// 200 URI Start
// URI: tardigrade://fake-serialized-access/bucket-name/apt/trusty/riemann-sumd_0.7.2-1_all.deb
// Size: 9012
// Last-Modified: Thu, 25 Oct 2018 20:17:39 GMT
func (m *Method) uriStart(objectURI string, size int64, t time.Time) (*message.Message, error) {
	h := header(headerCodeURIStart, headerDescriptionURIStart)
	uriField := field(fieldNameURI, objectURI)
	sizeField := field(fieldNameSize, strconv.FormatInt(size, 10))
	lmField, err := m.lastModified(t)
	if err != nil {
		return nil, err
	}
	return &message.Message{Header: h, Fields: []*message.Field{uriField, sizeField, lmField}}, nil
}

// uriDone constructs a Message that when printed looks like the following
// example:
//
// 201 URI Done
// URI: tardigrade://fake-serialized-access/bucket-name/apt/trusty/riemann-sumd_0.7.2-1_all.deb
// Filename: /var/cache/apt/archives/partial/riemann-sumd_0.7.2-1_all.deb
// Size: 9012
// Last-Modified: Thu, 25 Oct 2018 20:17:39 GMT
// MD5-Hash: 1964cb59e339e7a41cf64e9d40f219b1
// MD5Sum-Hash: 1964cb59e339e7a41cf64e9d40f219b1
// SHA1-Hash: 0d02ab49503be20d153cea63a472c43ebfad2efc
// SHA256-Hash: 92a3f70eb1cf2c69880988a8e74dc6fea7e4f15ee261f74b9be55c866f69c64b
// SHA512-Hash: ab3b1c94618cb58e2147db1c1d4bd3472f17fb11b1361e77216b461ab7d5f5952a5c6bb0443a1507d8ca5ef1eb18ac7552d0f2a537a0d44b8612d7218bf379fb
func (m *Method) uriDone(objectURI string, size int64, t time.Time, filename string) (*message.Message, error) {
	h := header(headerCodeURIDone, headerDescriptionURIDone)
	uriField := field(fieldNameURI, objectURI)
	filenameField := field(fieldNameFilename, filename)
	sizeField := field(fieldNameSize, strconv.FormatInt(size, 10))
	lmField, err := m.lastModified(t)
	if err != nil {
		return nil, err
	}
	fileBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	md5Field, err := m.md5Field(fileBytes)
	if err != nil {
		return nil, err
	}
	md5SumField, err := m.md5SumField(fileBytes)
	if err != nil {
		return nil, err
	}
	sha1Field, err := m.sha1Field(fileBytes)
	if err != nil {
		return nil, err
	}
	sha256Field, err := m.sha256Field(fileBytes)
	if err != nil {
		return nil, err
	}
	sha512Field, err := m.sha512Field(fileBytes)
	if err != nil {
		return nil, err
	}
	fields := []*message.Field{
		uriField,
		filenameField,
		sizeField,
		lmField,
		md5Field,
		md5SumField,
		sha1Field,
		sha256Field,
		sha512Field,
	}
	return &message.Message{Header: h, Fields: fields}, nil
}

// notFound constructs a Message that when printed looks like the following
// example:
//
// 400 URI Failure
// Message: The specified key does not exist.
// URI: tardigrade://fake-serialized-access/bucket-name/apt/trusty/riemann-sumd_0.7.2-1_all.deb
func notFound(objectURI string) *message.Message {
	h := header(headerCodeURIFailure, headerDescriptionURIFailure)
	uriField := field(fieldNameURI, objectURI)
	messageField := field(fieldNameMessage, fieldValueNotFound)
	return &message.Message{Header: h, Fields: []*message.Field{uriField, messageField}}
}

// generalFailure constructs a Message that when printed looks like the
// following example:
//
// 401 General Failure
// Message: Error retrieving ...
func generalFailure(err error) *message.Message {
	h := header(headerCodeGeneralFailure, headerDescriptionGeneralFailure)
	msg := strings.Replace(err.Error(), "\n", " ", -1)
	messageField := field(fieldNameMessage, msg)
	return &message.Message{Header: h, Fields: []*message.Field{messageField}}
}

func (m *Method) outputRequestStatus(objectURI, status string) error {
	msg := requestStatus(objectURI, status)
	m.stdout.Println(msg.String())
	return nil
}

func (m *Method) outputURIStart(objectURI string, size int64, lastModified time.Time) error {
	msg, err := m.uriStart(objectURI, size, lastModified)
	if err != nil {
		return err
	}
	m.stdout.Println(msg.String())
	return nil
}

// outputURIDone prints a message including the details of the finished URI.
func (m *Method) outputURIDone(objectURI string, size int64, lastModified time.Time, filename string) error {
	msg, err := m.uriDone(objectURI, size, lastModified, filename)
	if err != nil {
		return err
	}
	m.stdout.Println(msg.String())
	return nil
}

// outputNotFound prints a message including the details of the URI that could
// not be found.
func (m *Method) outputNotFound(objectURI string) {
	msg := notFound(objectURI)
	m.stdout.Println(msg.String())
}

func (m *Method) outputGeneralFailure(err error) {
	msg := generalFailure(err)
	m.stdout.Println(msg.String())
}

func header(code int, description string) *message.Header {
	return &message.Header{Status: code, Description: description}
}

func field(name string, value string) *message.Field {
	return &message.Field{Name: name, Value: value}
}

// lastModified returns a Field with the given Time formatted using the RFC1123
// specification in GMT, as specified in the APT method interface documentation.
func (m *Method) lastModified(t time.Time) (*message.Field, error) {
	gmt, err := time.LoadLocation("GMT")
	if err != nil {
		return nil, err
	}
	return field(fieldNameLastModified, t.In(gmt).Format(time.RFC1123)), nil
}

func (m *Method) md5Field(bytes []byte) (*message.Field, error) {
	md5Hash := md5.New()
	md5String, err := m.computeHash(md5Hash, bytes)
	if err != nil {
		return nil, err
	}
	return field(fieldNameMD5Hash, md5String), nil
}

func (m *Method) md5SumField(bytes []byte) (*message.Field, error) {
	md5Hash := md5.New()
	md5String, err := m.computeHash(md5Hash, bytes)
	if err != nil {
		return nil, err
	}
	return field(fieldNameMD5SumHash, md5String), nil
}

func (m *Method) sha1Field(bytes []byte) (*message.Field, error) {
	sha1Hash := sha1.New()
	sha1String, err := m.computeHash(sha1Hash, bytes)
	if err != nil {
		return nil, err
	}
	return field(fieldNameSHA1Hash, sha1String), nil
}

func (m *Method) sha256Field(bytes []byte) (*message.Field, error) {
	sha256Hash := sha256.New()
	sha256String, err := m.computeHash(sha256Hash, bytes)
	if err != nil {
		return nil, err
	}
	return field(fieldNameSHA256Hash, sha256String), nil
}

func (m *Method) sha512Field(bytes []byte) (*message.Field, error) {
	sha512Hash := sha512.New()
	sha512String, err := m.computeHash(sha512Hash, bytes)
	if err != nil {
		return nil, err
	}
	return field(fieldNameSHA512Hash, sha512String), nil
}

func (m *Method) computeHash(h hash.Hash, fileBytes []byte) (string, error) {
	if err := m.prepareHash(h, fileBytes); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (m *Method) prepareHash(h hash.Hash, fileBytes []byte) error {
	_, err := io.Copy(h, bytes.NewReader(fileBytes))
	return err
}
