/*
Copyright 2019 The Kubernetes Authors.

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

package manilaclient

import (
	"context"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/sharedfilesystems/v2/messages"
	"github.com/gophercloud/gophercloud/v2/openstack/sharedfilesystems/v2/shares"
	"github.com/gophercloud/gophercloud/v2/openstack/sharedfilesystems/v2/sharetypes"
	"github.com/gophercloud/gophercloud/v2/openstack/sharedfilesystems/v2/snapshots"
	shares_utils "github.com/gophercloud/utils/v2/openstack/sharedfilesystems/v2/shares"
	sharetypes_utils "github.com/gophercloud/utils/v2/openstack/sharedfilesystems/v2/sharetypes"
	snapshots_utils "github.com/gophercloud/utils/v2/openstack/sharedfilesystems/v2/snapshots"
)

type Client struct {
	c *gophercloud.ServiceClient
}

func (c Client) GetShareByID(shareID string) (*shares.Share, error) {
	return shares.Get(context.TODO(), c.c, shareID).Extract()
}

func (c Client) GetShareByName(shareName string) (*shares.Share, error) {
	shareID, err := shares_utils.IDFromName(context.TODO(), c.c, shareName)
	if err != nil {
		return nil, err
	}

	return shares.Get(context.TODO(), c.c, shareID).Extract()
}

func (c Client) CreateShare(opts shares.CreateOptsBuilder) (*shares.Share, error) {
	return shares.Create(context.TODO(), c.c, opts).Extract()
}

func (c Client) DeleteShare(shareID string) error {
	return shares.Delete(context.TODO(), c.c, shareID).ExtractErr()
}

func (c Client) ExtendShare(shareID string, opts shares.ExtendOptsBuilder) error {
	return shares.Extend(context.TODO(), c.c, shareID, opts).ExtractErr()
}

func (c Client) GetExportLocations(shareID string) ([]shares.ExportLocation, error) {
	return shares.ListExportLocations(context.TODO(), c.c, shareID).Extract()
}

func (c Client) SetShareMetadata(shareID string, opts shares.SetMetadataOptsBuilder) (map[string]string, error) {
	return shares.SetMetadata(context.TODO(), c.c, shareID, opts).Extract()
}

func (c Client) GetAccessRights(shareID string) ([]shares.AccessRight, error) {
	return shares.ListAccessRights(context.TODO(), c.c, shareID).Extract()
}

func (c Client) GrantAccess(shareID string, opts shares.GrantAccessOptsBuilder) (*shares.AccessRight, error) {
	return shares.GrantAccess(context.TODO(), c.c, shareID, opts).Extract()
}

func (c Client) GetSnapshotByID(snapID string) (*snapshots.Snapshot, error) {
	return snapshots.Get(context.TODO(), c.c, snapID).Extract()
}

func (c Client) GetSnapshotByName(snapName string) (*snapshots.Snapshot, error) {
	snapID, err := snapshots_utils.IDFromName(context.TODO(), c.c, snapName)
	if err != nil {
		return nil, err
	}

	return snapshots.Get(context.TODO(), c.c, snapID).Extract()
}

func (c Client) CreateSnapshot(opts snapshots.CreateOptsBuilder) (*snapshots.Snapshot, error) {
	return snapshots.Create(context.TODO(), c.c, opts).Extract()
}

func (c Client) DeleteSnapshot(snapID string) error {
	return snapshots.Delete(context.TODO(), c.c, snapID).ExtractErr()
}

func (c Client) GetExtraSpecs(shareTypeID string) (sharetypes.ExtraSpecs, error) {
	return sharetypes.GetExtraSpecs(context.TODO(), c.c, shareTypeID).Extract()
}

func (c Client) GetShareTypes() ([]sharetypes.ShareType, error) {
	allPages, err := sharetypes.List(c.c, sharetypes.ListOpts{}).AllPages(context.TODO())
	if err != nil {
		return nil, err
	}

	return sharetypes.ExtractShareTypes(allPages)
}

func (c Client) GetShareTypeIDFromName(shareTypeName string) (string, error) {
	return sharetypes_utils.IDFromName(context.TODO(), c.c, shareTypeName)
}

func (c Client) GetUserMessages(opts messages.ListOptsBuilder) ([]messages.Message, error) {
	allPages, err := messages.List(c.c, opts).AllPages(context.TODO())
	if err != nil {
		return nil, err
	}

	return messages.ExtractMessages(allPages)
}
