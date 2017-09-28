package clients

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type Client interface {
	GetVolumes() (map[string]*ec2.Volume, error)
	GetSnapshots() (map[string]*ec2.Snapshot, error)
	CreateSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot) error
	RemoveSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot) error
}

type EBSClient struct {
	ec2Client               *ec2.EC2
	snapshotsCreatedCounter *prometheus.CounterVec
	snapshotsDeletedCounter *prometheus.CounterVec
}

func NewEBSClient(c *ec2.EC2, scc, sdc *prometheus.CounterVec) EBSClient {
	return EBSClient{
		ec2Client:               c,
		snapshotsCreatedCounter: scc,
		snapshotsDeletedCounter: sdc,
	}
}

func (c *EBSClient) GetVolumes() (map[string]*ec2.Volume, error) {
	maxResults := int64(1000)
	volumes := make([]*ec2.Volume, 0)
	input := &ec2.DescribeVolumesInput{
		MaxResults: &maxResults,
	}

	vols, err := c.ec2Client.DescribeVolumes(input)
	if err != nil {
		return nil, fmt.Errorf("error while describing volumes: %v", err)
	}

	volumes = append(volumes, vols.Volumes...)
	for vols.NextToken != nil {
		vols, err := c.ec2Client.DescribeVolumes(&ec2.DescribeVolumesInput{
			MaxResults: &maxResults,
			NextToken:  vols.NextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("error while describing volumes: %v", err)
		}
		volumes = append(volumes, vols.Volumes...)
	}

	return mapVolumesToIds(volumes), nil
}

func (c *EBSClient) GetSnapshots() (map[string]*ec2.Snapshot, error) {
	maxResults := int64(1000)
	snapshots := make([]*ec2.Snapshot, 0)

	snaps, err := c.ec2Client.DescribeSnapshots(&ec2.DescribeSnapshotsInput{
		MaxResults: &maxResults,
	})
	if err != nil {
		return nil, fmt.Errorf("error while describing volumes: %v", err)
	}

	snapshots = append(snapshots, snaps.Snapshots...)
	for snaps.NextToken != nil {
		snaps, err = c.ec2Client.DescribeSnapshots(&ec2.DescribeSnapshotsInput{
			MaxResults: &maxResults,
			NextToken:  snaps.NextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("error while describing volumes: %v", err)
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
		return err
	}
	log.Printf("created snapshot for volume %s", *vol.VolumeId)
	c.snapshotsCreatedCounter.WithLabelValues(*vol.VolumeId).Inc()

	return nil
}

func (c *EBSClient) RemoveSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot) error {
	if _, err := c.ec2Client.DeleteSnapshot(&ec2.DeleteSnapshotInput{
		SnapshotId: lastSnapshot.SnapshotId,
	}); err != nil {
		return err
	}
	log.Printf("old snapshot with id %s for volume %s has been deleted", *vol.VolumeId, *lastSnapshot.SnapshotId)
	c.snapshotsDeletedCounter.WithLabelValues(*vol.VolumeId, *lastSnapshot.SnapshotId).Inc()

	return nil
}

func mapVolumesToIds(volumes []*ec2.Volume) map[string]*ec2.Volume {
	output := make(map[string]*ec2.Volume)
	for _, vol := range volumes {
		output[*vol.VolumeId] = vol
	}
	return output
}

func mapMostRecentSnapshotToVolumes(snapshots []*ec2.Snapshot) map[string]*ec2.Snapshot {
	output := make(map[string]*ec2.Snapshot)
	for _, snapshot := range snapshots {
		existingSnapshot := output[*snapshot.VolumeId]
		if existingSnapshot == nil || existingSnapshot.StartTime.Before(*snapshot.StartTime) {
			output[*snapshot.VolumeId] = snapshot
		}
	}
	return output
}
