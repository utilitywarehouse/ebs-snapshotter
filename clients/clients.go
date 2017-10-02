package clients

import (
	"sort"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/pkg/errors"
)

var (
	resultsPerRequest = int64(1000)
)

// EC2Volumes is type alias for EC2 Volume map
type EC2Volumes map[string]*ec2.Volume

// EC2Snapshots is type alias for EC2 Snapshot map
type EC2Snapshots map[string][]*ec2.Snapshot

// EBSClient interface specifies EBS client functions
type EBSClient interface {
	GetVolumes() (EC2Volumes, error)
	GetSnapshots() (EC2Snapshots, error)
	CreateSnapshot(volume *ec2.Volume) error
	RemoveSnapshot(snapshot *ec2.Snapshot) error
}

type ebsClient struct {
	ec2Client *ec2.EC2
}

// NewEBSClient used to create a new EBS client instance
func NewEBSClient(client *ec2.EC2) EBSClient {
	return &ebsClient{
		ec2Client: client,
	}
}

// GetVolumes used to obtain EC2 volumes
func (c *ebsClient) GetVolumes() (EC2Volumes, error) {
	volumes := make([]*ec2.Volume, 0)
	input := &ec2.DescribeVolumesInput{
		MaxResults: &resultsPerRequest,
	}

	vols, err := c.ec2Client.DescribeVolumes(input)
	if err != nil {
		return nil, errors.Wrap(err, "error while describing volumes")
	}

	volumes = append(volumes, vols.Volumes...)
	for vols.NextToken != nil {
		v, err := c.ec2Client.DescribeVolumes(&ec2.DescribeVolumesInput{
			MaxResults: &resultsPerRequest,
			NextToken:  vols.NextToken,
		})
		if err != nil {
			return nil, errors.Wrap(err, "error while describing volumes")
		}
		volumes = append(volumes, v.Volumes...)
	}

	return mapVolumesToIds(volumes), nil
}

// GetLatestSnapshots used to obtain recent EC2 EBS snapshots
func (c *ebsClient) GetSnapshots() (EC2Snapshots, error) {
	snapshots := make([]*ec2.Snapshot, 0)

	snaps, err := c.ec2Client.DescribeSnapshots(&ec2.DescribeSnapshotsInput{
		MaxResults: &resultsPerRequest,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error while describing snapshots")
	}

	snapshots = append(snapshots, snaps.Snapshots...)
	for snaps.NextToken != nil {
		snaps, err = c.ec2Client.DescribeSnapshots(&ec2.DescribeSnapshotsInput{
			MaxResults: &resultsPerRequest,
			NextToken:  snaps.NextToken,
		})
		if err != nil {
			return nil, errors.Wrap(err, "error while describing snapshots")
		}
		snapshots = append(snapshots, snaps.Snapshots...)
	}

	mappedSnapshots := MapSnapshotsToVolumes(snapshots)
	for _, snaps := range mappedSnapshots {
		SortSnapshotsByStartTime(snaps)
	}

	return mappedSnapshots, nil
}

// CreateSnapshot used to create a new EC2 EBS snapshot for given volume
func (c *ebsClient) CreateSnapshot(volume *ec2.Volume) error {
	desc := string("Created by ebs-snapshotter")
	input := &ec2.CreateSnapshotInput{
		VolumeId:    volume.VolumeId,
		Description: &desc,
	}

	if _, err := c.ec2Client.CreateSnapshot(input); err != nil {
		return errors.Wrap(err, "error while creating a snapshot")
	}

	return nil
}

// RemoveSnapshot used to remove EC2 EBS snapshot
func (c *ebsClient) RemoveSnapshot(snapshot *ec2.Snapshot) error {
	if _, err := c.ec2Client.DeleteSnapshot(&ec2.DeleteSnapshotInput{
		SnapshotId: snapshot.SnapshotId,
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

// MapSnapshotsToVolumes used to map EBS snapshots by Volume ID
func MapSnapshotsToVolumes(snapshots []*ec2.Snapshot) EC2Snapshots {
	output := make(EC2Snapshots)
	for _, snapshot := range snapshots {
		switch {
		case output[*snapshot.VolumeId] == nil:
			output[*snapshot.VolumeId] = []*ec2.Snapshot{snapshot}
		default:
			output[*snapshot.VolumeId] = append(output[*snapshot.VolumeId], snapshot)
		}

	}
	return output
}

// SortSnapshotsByStartTime used to sort EBS snapshots by start time
func SortSnapshotsByStartTime(snapshots []*ec2.Snapshot) {
	sort.Sort(SortByStartTime(snapshots))
}

// SortByStartTime used to sort EBS snapshots by start time in descending order
type SortByStartTime []*ec2.Snapshot

func (snap SortByStartTime) Len() int {
	return len(snap)
}

func (snap SortByStartTime) Swap(a, b int) {
	snap[b], snap[a] = snap[a], snap[b]
}

func (snap SortByStartTime) Less(a, b int) bool {
	return snap[b].StartTime.Before(*snap[a].StartTime)
}
