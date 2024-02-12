//go:build !nolabstore
// +build !nolabstore

/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package object

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type labstore struct {
	RestfulStorage
	configId string
}

func (l *labstore) String() string {
	uri, _ := url.ParseRequestURI(l.endpoint)
	return fmt.Sprintf("labstore://%s/", uri.Host)
}

func labstoreSigner(req *http.Request, accessKey, secretKey, signName string) {
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", secretKey))
}

func (l *labstore) Put(key string, bodyReader io.Reader) error {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(body)

	res, err := l.request("PUT", key, bytes.NewReader(body), map[string]string{
		"Content-Length":      fmt.Sprintf("%d", len(body)),
		"X-LabStore-ConfigId": l.configId,
		"X-LabStore-SHA256":   base64.RawURLEncoding.EncodeToString(hash[:]),
	})
	if err != nil {
		return err
	}
	defer cleanup(res)
	if res.StatusCode != 200 {
		return parseError(res)
	}
	return nil
}

func (l *labstore) SetStorageClass(sc string) {
	if sc == "" {
		l.configId = "default"
	} else {
		l.configId = sc
	}
}

var _ ObjectStorage = (*labstore)(nil)
var _ SupportStorageClass = (*labstore)(nil)

// todo: implement this...
// func (l *labstore) List(prefix, marker, delimiter string, limit int64, followLink bool) ([]Object, error) {
// 	return nil, notSupported
// }

func newLabStore(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
	if !strings.Contains(endpoint, "://") {
		endpoint = fmt.Sprintf("https://%s/objects", endpoint)
	}
	return &labstore{RestfulStorage: RestfulStorage{DefaultObjectStorage{}, endpoint, "", secretKey, "", labstoreSigner}, configId: "default"}, nil
}

func init() {
	Register("labstore", newLabStore)
}
