/*
Copyright 2021 The Flux authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package notifier

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAzureEventHub(t *testing.T) {
	tests := []struct {
		name              string
		endpointURL       string
		token             string
		eventHubNamespace string
		err               error
	}{
		{
			name:              "JWT Authentication",
			endpointURL:       "https://example.com",
			token:             "jwt-token",
			eventHubNamespace: "namespace",
		},
		{
			name:              "SAS Authentication",
			endpointURL:       "Endpoint=sb://example.com/;SharedAccessKeyName=keyName;SharedAccessKey=key;EntityPath=eventhub",
			token:             "",
			eventHubNamespace: "namespace",
		},
		{
			name:              "SAS Authentication",
			endpointURL:       "Endpoint=sb://example.com/;SharedAccessKeyName=keyName;SharedAccessKey=key",
			token:             "",
			eventHubNamespace: "namespace",
			err:               errors.New("failed to create a eventhub using SAS connection string does not contain an EntityPath. eventHub cannot be an empty string"),
		},
		{
			name:              "Default Azure Credential",
			endpointURL:       "https://example.com",
			token:             "",
			eventHubNamespace: "namespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewAzureEventHub(tt.endpointURL, tt.token, tt.eventHubNamespace)
			if tt.err != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.err, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
			}
		})
	}
}
