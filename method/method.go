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
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"storj.io/uplink"

	"storj.io/apt-transport-storj/common"
	"storj.io/apt-transport-storj/limiter"
	"storj.io/apt-transport-storj/message"
	"storj.io/apt-transport-storj/symlinks"
)

const (
	aptTransportUserAgent = "apt-transport-storj/" + common.Version
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
	fieldNameIMSHit         = "IMS-Hit"
	fieldNameFilename       = "Filename"
	fieldNameSize           = "Size"
	fieldNameLastModified   = "Last-Modified"
	fieldNameVersion        = "Version"
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

const (
	configItemEncryptionPassphrase = "Acquire::Storj::EncryptionPassphrase"
	configItemDialTimeout          = "Acquire::Storj::ConnectionTimeout"
)

const (
	defaultEncryptionPassphrase = "debian"
	defaultDialTimeout          = 10 * time.Second
)

type storjClient struct {
	project    *uplink.Project
	symlinkMap symlinks.SymlinkMap

	Err    error
	locker sync.Mutex
	ready  *sync.Cond
}

type apiKeyAndBucket struct {
	apiKey string
	bucket string
}

// A Method implements the logic to process incoming apt messages and respond
// accordingly.
type Method struct {
	inputStream          io.Reader
	outputStream         io.Writer
	clientMapLock        sync.Mutex
	clients              map[apiKeyAndBucket]*storjClient
	encryptionPassphrase string
	dialTimeout          time.Duration
}

// New returns a new Method configured to read from os.Stdin and write to
// os.Stdout.
func New() *Method {
	return &Method{
		inputStream:          os.Stdin,
		outputStream:         os.Stdout,
		clients:              make(map[apiKeyAndBucket]*storjClient),
		encryptionPassphrase: defaultEncryptionPassphrase,
		dialTimeout:          defaultDialTimeout,
	}
}

// Run flushes the Method's capabilities and then begins reading messages from
// m.inputStream. Results are written to m.outputStream. The running Method
// waits for all Messages to be processed before exiting.
func (m *Method) Run(ctx context.Context) {
	m.send(capabilities())
	if err := m.processMessages(ctx, m.inputStream); err != nil {
		m.send(generalFailure(err))
	}
}

func capabilities() *message.Message {
	header := header(headerCodeCapabilities, headerDescriptionCapabilities)
	fields := []*message.Field{
		field(fieldNameSendConfig, "true"),
		field(fieldNamePipeline, "true"),
		field(fieldNameSingleInstance, "yes"),
		field(fieldNameVersion, common.Version),
	}
	return &message.Message{Header: header, Fields: fields}
}

// processMessages loops over messages given on the input stream
// and starts a goroutine to process each Message.
func (m *Method) processMessages(ctx context.Context, input io.Reader) error {
	buffer := &bytes.Buffer{}
	waitGroup, ctx := errgroup.WithContext(ctx)
	inStream := input
	if inFDReader, ok := input.(limiter.FDReader); ok {
		inStream = limiter.NewFileReaderWithContext(ctx, inFDReader)
	}
	scanner := bufio.NewScanner(inStream)
	for scanner.Scan() {
		line := scanner.Text()
		buffer.WriteString(line + "\n")

		// Messages are terminated with a blank line. If a line with no content
		// comes in and the buffer already has some content, it's assuming that
		// the buffer currently contains a complete message ready to be processed.
		if len(line) == 0 && buffer.Len() > 3 {
			err := m.handleBytes(ctx, waitGroup, buffer.Bytes())
			if err != nil {
				return err
			}
			buffer = &bytes.Buffer{}
		}
	}
	scannerErr := scanner.Err()
	err := waitGroup.Wait()
	if err == nil {
		err = scannerErr
	}
	return err
}

// handleBytes initializes a new Message and dispatches it according to
// the Message.Header.Status value.
func (m *Method) handleBytes(ctx context.Context, waitGroup *errgroup.Group, b []byte) error {
	msg, err := message.FromBytes(b)
	if err != nil {
		return err
	}
	if msg.Header.Status == headerCodeURIAcquire {
		// URI Acquire message; APT wants us to get a file
		waitGroup.Go(func() error {
			return m.uriAcquire(ctx, msg)
		})
	} else if msg.Header.Status == headerCodeConfiguration {
		// Configuration message; APT is sending its config to us. Process this
		// synchronously so that following Acquire messages are sure to see the
		// results of the configure step.
		err = m.configure(msg)
	}
	return err
}

func (m *Method) configure(msg *message.Message) error {
	items := msg.GetFieldList(fieldNameConfigItem)
	for _, f := range items {
		config := strings.Split(f.Value, "=")
		if len(config) != 2 {
			return fmt.Errorf("invalid configuration item %q", f.Value)
		}
		value, err := url.PathUnescape(config[1])
		if err != nil {
			return fmt.Errorf("bad encoding on configuration item %q: %v", f.Value, err)
		}
		switch config[0] {
		case configItemDialTimeout:
			timeout, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("invalid value for %s: %s", config[0], err)
			}
			m.dialTimeout = timeout
		case configItemEncryptionPassphrase:
			m.encryptionPassphrase = value
		}
	}
	return nil
}

func (m *Method) getClient(ctx context.Context, satelliteAddr, apiKey, bucket string, statusFunc func(string)) (_ *storjClient, err error) {
	m.clientMapLock.Lock()
	defer m.clientMapLock.Unlock()

	client, ok := m.clients[apiKeyAndBucket{apiKey, bucket}]
	if !ok {
		client, err = m.storjClient(ctx, satelliteAddr, apiKey, bucket, statusFunc)
		if err != nil {
			return nil, err
		}
		m.clients[apiKeyAndBucket{apiKey, bucket}] = client
	}
	return client, nil
}

func (m *Method) storjDebURIParse(uri string) (satelliteAddress, apiKey, bucket, objectKey string, err error) {
	uriObject, err := url.Parse(uri)
	if err != nil {
		return "", "", "", "", err
	}
	cleanPath := path.Clean(uriObject.Path)
	pathParts := strings.Split(strings.TrimLeft(cleanPath, "/"), "/")
	if len(pathParts) < 2 {
		return "", "", "", "", fmt.Errorf("invalid storj-apt source URI %q, %v", uri, pathParts)
	}
	bucket = pathParts[1]
	objectKey = strings.Join(pathParts[2:], "/")
	satelliteAddress = uriObject.Host
	if uriObject.User != nil {
		satelliteAddress = uriObject.User.Username() + "@" + satelliteAddress
	}
	return satelliteAddress, pathParts[0], bucket, objectKey, nil
}

// uriAcquire downloads and stores objects from S3 based on the contents
// of the provided Message.
func (m *Method) uriAcquire(ctx context.Context, msg *message.Message) (err error) {
	uri, hasField := msg.GetFieldValue(fieldNameURI)
	if !hasField {
		return fmt.Errorf("acquire message missing required field: URI")
	}
	satelliteAddr, apiKey, bucket, pathKey, err := m.storjDebURIParse(uri)
	if err != nil {
		return err
	}
	m.send(aptLogMessage("satelliteAddr=%q, apiKey=%q, bucket=%q, pathKey=%q", satelliteAddr, apiKey, bucket, pathKey))
	statusFunc := func(s string) {
		m.send(requestStatus(uri, s))
	}
	client, err := m.getClient(ctx, satelliteAddr, apiKey, bucket, statusFunc)
	if err != nil {
		return err
	}

	filename, hasField := msg.GetFieldValue(fieldNameFilename)
	if !hasField {
		return fmt.Errorf("acquire message missing required field: Filename")
	}
	var lastModifiedInAPT time.Time
	lastModifiedStr, hasField := msg.GetFieldValue(fieldNameLastModified)
	if hasField {
		lastModifiedInAPT, err = time.Parse(time.RFC1123, lastModifiedStr)
		if err != nil {
			m.send(aptLogMessage("ignoring invalid time string: %q (%v)", lastModifiedStr, err))
			lastModifiedInAPT = time.Time{}
		}
	}

	download, err := client.DownloadObject(ctx, bucket, pathKey, nil)
	if err != nil {
		m.reportClientFailure(ctx, uri, err)
		return nil
	}
	defer common.DeferClose(download, &err)
	downloadInfo := download.Info()
	expectedLen := downloadInfo.System.ContentLength
	lastModifiedInStorj := downloadInfo.System.Created

	if !lastModifiedInStorj.IsZero() && !lastModifiedInAPT.IsZero() {
		if lastModifiedInStorj.Before(lastModifiedInAPT) {
			doneMsg, err := uriDone(uri, expectedLen, lastModifiedInAPT, filename, true)
			if err != nil {
				return err
			}
			m.send(doneMsg)
			return nil
		}
	}
	m.send(uriStart(uri, expectedLen, lastModifiedInStorj))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer common.DeferClose(file, &err)

	numBytes, err := io.Copy(file, download)
	if err != nil {
		m.reportClientFailure(ctx, uri, err)
		return nil
	}
	doneMsg, err := uriDone(uri, numBytes, lastModifiedInStorj, filename, false)
	if err != nil {
		return err
	}
	m.send(doneMsg)
	return nil
}

func (m *Method) reportClientFailure(ctx context.Context, uri string, err error) {
	if ctx.Err() != nil {
		// if context has already been closed, do not report errors for individual URIs.
		// We are already tearing everything down, and all errors are likely to be due
		// to that. A general failure will be reported.
		return
	}
	var msg *message.Message
	if errors.Is(err, uplink.ErrObjectNotFound) {
		msg = notFound(uri)
	} else {
		msg = uriFailure(uri, err.Error())
	}
	m.send(msg)
}

// storjClient returns a configured and opened Project handle, which can be used
// to download specific paths within the project (if the api key allows it).
func (m *Method) storjClient(ctx context.Context, satelliteAddr, apiKey, bucket string, statusFunc func(string)) (*storjClient, error) {
	uplinkConfig := uplink.Config{
		UserAgent:   aptTransportUserAgent,
		DialTimeout: m.dialTimeout,
	}
	shortForm := satelliteAddr
	if ind := strings.Index(satelliteAddr, "@"); ind >= 0 {
		shortForm = satelliteAddr[ind+1:]
	}
	statusFunc("Connecting to " + shortForm)
	access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, satelliteAddr, apiKey, m.encryptionPassphrase)
	if err != nil {
		return nil, err
	}
	client := &storjClient{}
	client.ready = sync.NewCond(&client.locker)
	go client.doSetup(ctx, access, bucket, statusFunc)
	return client, nil
}

func (c *storjClient) doSetup(ctx context.Context, access *uplink.Access, bucket string, statusFunc func(string)) {
	defer c.ready.Broadcast()

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		c.Err = err
		return
	}
	c.project = project
	statusFunc("Fetching symlink map")
	symlinkMap, err := symlinks.DownloadSymlinkMap(ctx, project, bucket)
	if err != nil {
		c.Err = fmt.Errorf("failed to get symlink map: %v", err)
		return
	}
	c.symlinkMap = symlinkMap
}

func (c *storjClient) getProject() (*uplink.Project, error) {
	if c.Err == nil && c.symlinkMap == nil {
		c.locker.Lock()
		for c.Err == nil && c.symlinkMap == nil {
			c.ready.Wait()
		}
		c.locker.Unlock()
	}
	if c.Err != nil {
		return nil, c.Err
	}
	return c.project, nil
}

func (c *storjClient) DownloadObject(ctx context.Context, bucket, pathKey string, options *uplink.DownloadOptions) (*uplink.Download, error) {
	project, err := c.getProject()
	if err != nil {
		return nil, err
	}
	resolvedPath, err := c.symlinkMap.Resolve(pathKey)
	if err != nil {
		return nil, err
	}
	return project.DownloadObject(ctx, bucket, resolvedPath, options)
}

// aptLogMessage constructs a Message that when printed looks like the
// following example:
//
// 101 Log
// Message: Now reticulating splines
func aptLogMessage(messageFormat string, args ...interface{}) *message.Message {
	h := header(headerCodeGeneralLog, headerDescriptionGeneralLog)
	messageField := field(fieldNameMessage, fmt.Sprintf(messageFormat, args...))
	return &message.Message{Header: h, Fields: []*message.Field{messageField}}
}

// requestStatus constructs a Message that when printed looks like the
// following example:
//
// 102 Status
// URI: storj-apt://us-central-1.tardigrade.io:7777/apiKeyString/bucket-name/apt/trusty/riemann-sumd_0.7.2-1_all.deb
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
// URI: storj-apt://us-central-1.tardigrade.io:7777/apiKeyString/bucket-name/apt/trusty/riemann-sumd_0.7.2-1_all.deb
// Size: 9012
// Last-Modified: Thu, 25 Oct 2018 20:17:39 GMT
func uriStart(objectURI string, size int64, t time.Time) *message.Message {
	h := header(headerCodeURIStart, headerDescriptionURIStart)
	uriField := field(fieldNameURI, objectURI)
	sizeField := field(fieldNameSize, strconv.FormatInt(size, 10))
	lmField := lastModifiedField(t)
	return &message.Message{Header: h, Fields: []*message.Field{uriField, sizeField, lmField}}
}

// uriDone constructs a Message that when printed looks like the following
// example:
//
// 201 URI Done
// URI: storj-apt://us-central-1.tardigrade.io:7777/apiKeyString/bucket-name/apt/trusty/riemann-sumd_0.7.2-1_all.deb
// Filename: /var/cache/apt/archives/partial/riemann-sumd_0.7.2-1_all.deb
// Size: 9012
// Last-Modified: Thu, 25 Oct 2018 20:17:39 GMT
// MD5-Hash: 1964cb59e339e7a41cf64e9d40f219b1
// MD5Sum-Hash: 1964cb59e339e7a41cf64e9d40f219b1
// SHA1-Hash: 0d02ab49503be20d153cea63a472c43ebfad2efc
// SHA256-Hash: 92a3f70eb1cf2c69880988a8e74dc6fea7e4f15ee261f74b9be55c866f69c64b
// SHA512-Hash: ab3b1c94618cb58e2147db1c1d4bd3472f17fb11b1361e77216b461ab7d5f5952a5c6bb0443a1507d8ca5ef1eb18ac7552d0f2a537a0d44b8612d7218bf379fb
func uriDone(objectURI string, size int64, t time.Time, filename string, imsHit bool) (*message.Message, error) {
	h := header(headerCodeURIDone, headerDescriptionURIDone)
	uriField := field(fieldNameURI, objectURI)
	filenameField := field(fieldNameFilename, filename)
	sizeField := field(fieldNameSize, strconv.FormatInt(size, 10))
	lmField := lastModifiedField(t)
	fields := []*message.Field{
		uriField,
		filenameField,
		sizeField,
		lmField,
	}
	if imsHit {
		fields = append(fields, field(fieldNameIMSHit, "true"))
	} else {
		fileBytes, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}

		fieldMethods := []func([]byte) *message.Field{
			md5Field,
			md5SumField,
			sha1Field,
			sha256Field,
			sha512Field,
		}
		for _, fieldMethod := range fieldMethods {
			fields = append(fields, fieldMethod(fileBytes))
		}
	}
	return &message.Message{Header: h, Fields: fields}, nil
}

// notFound constructs a Message that when printed looks like the following
// example:
//
// 400 URI Failure
// Message: The specified key does not exist.
// URI: storj-apt://us-central-1.tardigrade.io:7777/apiKeyString/bucket-name/apt/trusty/riemann-sumd_0.7.2-1_all.deb
func notFound(objectURI string) *message.Message {
	return uriFailure(objectURI, fieldValueNotFound)
}

// uriFailure constructs a Message that when printed looks like the following
// example:
//
// 400 URI Failure
// Message: A bad thing went wrong.
// URI: storj-apt://us-central-1.tardigrade.io:7777/apiKeyString/bucket-name/apt/trusty/riemann-sumd_0.7.2-1_all.deb
func uriFailure(objectURI string, messageText string) *message.Message {
	h := header(headerCodeURIFailure, headerDescriptionURIFailure)
	uriField := field(fieldNameURI, objectURI)
	messageField := field(fieldNameMessage, messageText)
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

// send formats a message and sends it back to APT, via the outputStream.
func (m *Method) send(msg *message.Message) {
	_, err := m.outputStream.Write([]byte(msg.String() + "\n"))
	if err != nil {
		// if we can't write that message, we probably can't write a general failure either.
		panic(err)
	}
}

func header(code int, description string) *message.Header {
	return &message.Header{Status: code, Description: description}
}

func field(name string, value string) *message.Field {
	return &message.Field{Name: name, Value: value}
}

// lastModifiedField returns a Field with the given Time formatted using the
// RFC1123 specification in GMT, as specified in the APT method interface
// documentation.
func lastModifiedField(t time.Time) *message.Field {
	return field(fieldNameLastModified, t.UTC().Format(time.RFC1123))
}

func md5Field(bytes []byte) *message.Field {
	return field(fieldNameMD5Hash, computeHash(md5.New(), bytes))
}

func md5SumField(bytes []byte) *message.Field {
	return field(fieldNameMD5SumHash, computeHash(md5.New(), bytes))
}

func sha1Field(bytes []byte) *message.Field {
	return field(fieldNameSHA1Hash, computeHash(sha1.New(), bytes))
}

func sha256Field(bytes []byte) *message.Field {
	return field(fieldNameSHA256Hash, computeHash(sha256.New(), bytes))
}

func sha512Field(bytes []byte) *message.Field {
	return field(fieldNameSHA512Hash, computeHash(sha512.New(), bytes))
}

func computeHash(h hash.Hash, fileBytes []byte) string {
	_, err := io.Copy(h, bytes.NewReader(fileBytes))
	if err != nil {
		// This should never happen. It appears that io.Copy will return
		// an error only when it gets an error (besides io.EOF) while
		// reading or while writing. bytes.Reader appears never to return
		// an error (besides io.EOF) if given valid arguments, and io.Copy
		// can probably be relied upon to give valid arguments. Likewise,
		// writing to a hash.Hash object never returns an error.
		panic("Could not copy from bytes.Reader to hash.Hash: " + err.Error())
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
