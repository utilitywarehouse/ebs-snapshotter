package clients

import (
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/pkg/errors"
)

type EC2Volumes map[string]*ec2.Volume
type EC2Snapshots map[string]*ec2.Snapshot

type Client interface {
	GetVolumes() (EC2Volumes, error)
	GetSnapshots() (EC2Snapshots, error)
	CreateSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot) error
	RemoveSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot) error
}

type EBSClient struct {
	ec2Client *ec2.EC2
}

func NewEBSClient(client *ec2.EC2) *EBSClient {
	return &EBSClient{
		ec2Client: client,
	}
}

func (c *EBSClient) GetVolumes() (EC2Volumes, error) {
	maxResults := int64(1000)
	volumes := make([]*ec2.Volume, 0)
	input := &ec2.DescribeVolumesInput{
		MaxResults: &maxResults,
	}

	vols, err := c.ec2Client.DescribeVolumes(input)
	if err != nil {
		return nil, errors.Wrap(err, "error while describing volumes")
	}

	volumes = append(volumes, vols.Volumes...)
	for vols.NextToken != nil {
		vols, err := c.ec2Client.DescribeVolumes(&ec2.DescribeVolumesInput{
			MaxResults: &maxResults,
			NextToken:  vols.NextToken,
		})
		if err != nil {
			return nil, errors.Wrap(err, "error while describing volumes")
		}
		volumes = append(volumes, vols.Volumes...)
	}

	return mapVolumesToIds(volumes), nil
}

func (c *EBSClient) GetSnapshots() (EC2Snapshots, error) {
	maxResults := int64(1000)
	snapshots := make([]*ec2.Snapshot, 0)

	snaps, err := c.ec2Client.DescribeSnapshots(&ec2.DescribeSnapshotsInput{
		MaxResults: &maxResults,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error while describing snapshots")
	}

	snapshots = append(snapshots, snaps.Snapshots...)
	for snaps.NextToken != nil {
		snaps, err = c.ec2Client.DescribeSnapshots(&ec2.DescribeSnapshotsInput{
			MaxResults: &maxResults,
			NextToken:  snaps.NextToken,
		})
		if err != nil {
			return nil, errors.Wrap(err, "error while describing snapshots")
		}
		snapshots = append(snapshots, snaps.Snapshots...)
	}

	return mapMostRecentSnapshotToVolumes(snapshots), nil
}

func (c *EBSClient) CreateSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot) error {
	desc := string("Created by ebs-snapshotter")
	input := &ec2.CreateSnapshotInput{
		VolumeId:    vol.VolumeId,
		Description: &desc,
	}

	if _, err := c.ec2Client.CreateSnapshot(input); err != nil {
		return errors.Wrap(err, "error while creating a snapshot")
	}

	return nil
}

func (c *EBSClient) RemoveSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot) error {
	if _, err := c.ec2Client.DeleteSnapshot(&ec2.DeleteSnapshotInput{
		SnapshotId: lastSnapshot.SnapshotId,
	}); err != nil {
		return errors.Wrap(err, "error while removing a snapshot")
	}

	return nil
}

func mapVolumesToIds(volumes []*ec2.Volume) EC2Volumes {
	output := make(EC2Volumes)
	for _, vol := range volumes {
		output[*vol.VolumeId] = vol
	}
	return output
}

func mapMostRecentSnapshotToVolumes(snapshots []*ec2.Snapshot) EC2Snapshots {
	output := make(EC2Snapshots)
	for _, snapshot := range snapshots {
		existingSnapshot := output[*snapshot.VolumeId]
		if existingSnapshot == nil || existingSnapshot.StartTime.Before(*snapshot.StartTime) {
			output[*snapshot.VolumeId] = snapshot
		}
	}
	return output
}
