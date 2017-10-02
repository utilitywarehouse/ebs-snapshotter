package clients_test

import (
	"testing"

	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/utilitywarehouse/ebs-snapshotter/clients"
	. "gopkg.in/check.v1"
)

var _ = Suite(&EBSClientSuite{})

type EBSClientSuite struct{}

func TestEBSClient(t *testing.T) { TestingT(t) }

func (s *EBSClientSuite) TestSnapshotsCorrectlySortedByStartTime(c *C) {
	timeNow := time.Now()
	snaps := []*ec2.Snapshot{
		createFakeEBSSnapshot("test-snapshot-6", "", timeNow),
		createFakeEBSSnapshot("test-snapshot-4", "", timeNow.Add(10*time.Hour)),
		createFakeEBSSnapshot("test-snapshot-7", "", timeNow.Add(-10*time.Hour)),
		createFakeEBSSnapshot("test-snapshot-2", "", timeNow.Add(40*time.Hour)),
		createFakeEBSSnapshot("test-snapshot-1", "", timeNow.Add(100*time.Hour)),
		createFakeEBSSnapshot("test-snapshot-8", "", timeNow.Add(-400*time.Hour)),
		createFakeEBSSnapshot("test-snapshot-3", "", timeNow.Add(30*time.Hour)),
		createFakeEBSSnapshot("test-snapshot-5", "", timeNow.Add(10*time.Minute)),
	}

	clients.SortSnapshotsByStartTime(snaps)

	for i, snap := range snaps {
		c.Assert(*snap.SnapshotId, Equals, fmt.Sprintf("test-snapshot-%d", i+1))
	}
}

func (s *EBSClientSuite) TestSnapshotsCorrectlyMappedPerVolumeId(c *C) {
	timeNow := time.Now()
	snaps := []*ec2.Snapshot{
		createFakeEBSSnapshot("test-snapshot-1", "volume-1", timeNow),
		createFakeEBSSnapshot("test-snapshot-1", "volume-2", timeNow),
		createFakeEBSSnapshot("test-snapshot-2", "volume-1", timeNow),
		createFakeEBSSnapshot("test-snapshot-1", "volume-5", timeNow),
		createFakeEBSSnapshot("test-snapshot-3", "volume-1", timeNow),
		createFakeEBSSnapshot("test-snapshot-1", "volume-3", timeNow),
		createFakeEBSSnapshot("test-snapshot-4", "volume-1", timeNow),
		createFakeEBSSnapshot("test-snapshot-2", "volume-2", timeNow),
	}

	mappedSnapshots := clients.MapSnapshotsToVolumes(snaps)

	c.Assert(len(mappedSnapshots["volume-1"]), Equals, 4)
	c.Assert(len(mappedSnapshots["volume-2"]), Equals, 2)
	c.Assert(len(mappedSnapshots["volume-3"]), Equals, 1)
	c.Assert(len(mappedSnapshots["volume-5"]), Equals, 1)
}

func createFakeEBSSnapshot(snapshotId, volumeId string, startTime time.Time) *ec2.Snapshot {
	return &ec2.Snapshot{
		SnapshotId: &snapshotId,
		VolumeId:   &volumeId,
		StartTime:  &startTime,
	}
}
