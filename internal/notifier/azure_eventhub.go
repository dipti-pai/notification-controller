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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
)

// AzureEventHub holds the eventhub client
type AzureEventHub struct {
	ProducerClient *azeventhubs.ProducerClient
}

// NewAzureEventHub creates a eventhub client
func NewAzureEventHub(endpointURL, token, eventHubNamespace string) (*AzureEventHub, error) {
	var err error
	var producer *azeventhubs.ProducerClient

	// token should only be defined if JWT is used
	if token != "" {
		tokenProvider := NewJWTProvider(token)

		producer, err = azeventhubs.NewProducerClient(eventHubNamespace, endpointURL, tokenProvider, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create a eventhub using JWT %v", err)
		}
	} else {
		// if SharedAccessKey string is present in the endpoint URL use SAS string for authentication
		if strings.Contains(endpointURL, "SharedAccessKey") {
			producer, err = azeventhubs.NewProducerClientFromConnectionString(endpointURL, "", nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create a eventhub using SAS %v", err)
			}
		} else {
			defaultAzureCred, err := azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				return nil, err
			}

			producer, err = azeventhubs.NewProducerClient(eventHubNamespace, endpointURL, defaultAzureCred, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create a eventhub using SAS %v", err)
			}
		}
	}

	return &AzureEventHub{
		ProducerClient: producer,
	}, nil
}

// Post all notification-controller messages to EventHub
func (e *AzureEventHub) Post(ctx context.Context, event eventv1.Event) (err error) {
	defer func() {
		closeErr := e.ProducerClient.Close(ctx)
		if err != nil {
			err = kerrors.NewAggregate([]error{err, closeErr})
		} else {
			err = closeErr
		}
	}()

	// Skip Git commit status update event.
	if event.HasMetadata(eventv1.MetaCommitStatusKey, eventv1.MetaCommitStatusUpdateValue) {
		return
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("unable to marshall event: %w", err)
		return
	}

	batch, err := e.ProducerClient.NewEventDataBatch(ctx, nil)
	if err != nil {
		err = fmt.Errorf("failed to create new event data batch: %w", err)
		return
	}
	err = batch.AddEventData(&azeventhubs.EventData{Body: eventBytes}, nil)
	if err != nil {
		err = fmt.Errorf("failed to add event data: %w", err)
		return
	}

	err = e.ProducerClient.SendEventDataBatch(ctx, batch, nil)
	if err != nil {
		err = fmt.Errorf("failed to send msg: %w", err)
		return
	}

	return
}

// PureJWT just contains the jwt
type PureJWT struct {
	jwt string
}

// NewJWTProvider create a pureJWT method
func NewJWTProvider(jwt string) *PureJWT {
	return &PureJWT{
		jwt: jwt,
	}
}

// GetToken uses a JWT token, we assume that we will get new tokens when needed, thus no Expiry defined
func (j *PureJWT) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	var expiryTime time.Time
	return azcore.AccessToken{
		Token:     j.jwt,
		ExpiresOn: expiryTime,
	}, nil
}
