/*
Copyright 2025 The Kubernetes Authors.

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

package openstack

import (
	"context"

	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumes"
	"k8s.io/cloud-provider-openstack/pkg/metrics"
	"k8s.io/klog/v2"
)

// rawInitializeConnectionOpts implements volumes.InitializeConnectionOptsBuilder
// using a raw map, allowing arbitrary connector properties (e.g. SDC GUID for
// PowerFlex) beyond the typed fields in gophercloud's InitializeConnectionOpts.
type rawInitializeConnectionOpts struct {
	connector map[string]any
}

func (o rawInitializeConnectionOpts) ToVolumeInitializeConnectionMap() (map[string]any, error) {
	return map[string]any{
		"os-initialize_connection": map[string]any{
			"connector": o.connector,
		},
	}, nil
}

// rawTerminateConnectionOpts implements volumes.TerminateConnectionOptsBuilder
// using a raw map.
type rawTerminateConnectionOpts struct {
	connector map[string]any
}

func (o rawTerminateConnectionOpts) ToVolumeTerminateConnectionMap() (map[string]any, error) {
	return map[string]any{
		"os-terminate_connection": map[string]any{
			"connector": o.connector,
		},
	}, nil
}

// InitializeConnection calls Cinder's os-initialize_connection volume action
// to set up a connection between the volume and the host described by
// connectorProperties. It returns the connection_info dict that the node-side
// os-brick connector needs to attach the volume locally.
func (os *OpenStack) InitializeConnection(ctx context.Context, volumeID string, connectorProperties map[string]any) (map[string]any, error) {
	blockstorageClient, err := openstack.NewBlockStorageV3(os.blockstorage.ProviderClient, os.epOpts)
	if err != nil {
		return nil, err
	}

	mc := metrics.NewMetricContext("volume", "initialize_connection")
	opts := rawInitializeConnectionOpts{connector: connectorProperties}
	connectionInfo, err := volumes.InitializeConnection(ctx, blockstorageClient, volumeID, opts).Extract()
	if mc.ObserveRequest(err) != nil {
		klog.Errorf("Failed to InitializeConnection for volume %s: %v", volumeID, err)
		return nil, err
	}

	return connectionInfo, nil
}

// TerminateConnection calls Cinder's os-terminate_connection volume action
// to clean up the connection between the volume and the host described by
// connectorProperties.
func (os *OpenStack) TerminateConnection(ctx context.Context, volumeID string, connectorProperties map[string]any) error {
	blockstorageClient, err := openstack.NewBlockStorageV3(os.blockstorage.ProviderClient, os.epOpts)
	if err != nil {
		return err
	}

	mc := metrics.NewMetricContext("volume", "terminate_connection")
	opts := rawTerminateConnectionOpts{connector: connectorProperties}
	err = volumes.TerminateConnection(ctx, blockstorageClient, volumeID, opts).ExtractErr()
	if mc.ObserveRequest(err) != nil {
		klog.Errorf("Failed to TerminateConnection for volume %s: %v", volumeID, err)
		return err
	}

	return nil
}
