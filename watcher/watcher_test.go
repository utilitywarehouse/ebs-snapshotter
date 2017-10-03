package watcher_test

import (
	"errors"
	"testing"
	"time"

	"strings"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/utilitywarehouse/ebs-snapshotter/clients"
	"github.com/utilitywarehouse/ebs-snapshotter/models"
	w "github.com/utilitywarehouse/ebs-snapshotter/watcher"
	. "gopkg.in/check.v1"
)

var (
	retentionPeriod = int64(10)
)

var _ = Suite(&WatcherSuite{})

var (
	crCounter, delCounter, errCounter *prometheus.CounterVec

	ec2Volumes   clients.EC2Volumes
	ec2Snapshots clients.EC2Snapshots

	volumesErrorOnGet     error
	snapshotsErrorOnGet   error
	SnapshotErrorOnCreate error
	snapshotErrorOnRemove error
)

type WatcherSuite struct {
	watcher *w.EBSSnapshotWatcher
}

func TestEBSWatcher(t *testing.T) { TestingT(t) }

func (s *WatcherSuite) SetUpSuite(c *C) {
	logrus.SetLevel(logrus.DebugLevel)

	crCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "snapshots_created",
		Help: "A counter of the total number of snapshots created",
	}, []string{"tag"})
	delCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "snapshots_deleted",
		Help: "A counter of the total number of old snapshots deleted",
	}, []string{"tag"})
	errCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "errors_total",
		Help: "A counter of the total number of errors encountered",
	}, []string{"tag"})

	s.watcher = w.NewEBSSnapshotWatcher(&MockClient{}, crCounter, delCounter, errCounter)
}

func (s *WatcherSuite) TestLogErrorWhenFailedToGetEC2Volumes(c *C) {
	errorMsg := "test volume error message"
	volumesErrorOnGet = errors.New(errorMsg)
	snapshotsErrorOnGet = nil

	err := s.watcher.WatchSnapshots(&models.VolumeSnapshotConfigs{})

	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "error while fetching volumes: test volume error message")
}

func (s *WatcherSuite) TestLogErrorWhenFailedToGetEC2Snapshots(c *C) {
	errorMsg := "test snapshots error message"
	snapshotsErrorOnGet = errors.New(errorMsg)
	volumesErrorOnGet = nil

	err := s.watcher.WatchSnapshots(&models.VolumeSnapshotConfigs{})

	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "error while fetching snapshots: test snapshots error message")
}

func (s *WatcherSuite) TestSnapshotNotDeletedWhenUpToDateSnapshotAndRetentionPeriodNotExceeded(c *C) {
	hook := test.NewGlobal()

	intervalSeconds := int64(11)
	config := models.VolumeSnapshotConfigs{
		{
			Labels: models.Label{
				Key:   "test-key-1",
				Value: "test-value-1",
			},
			IntervalSeconds:      intervalSeconds,
			RetentionPeriodHours: retentionPeriod,
		},
	}

	volumeID := "volume-1"
	ec2Volumes = clients.EC2Volumes{
		"test-key-1": createFakeVolume("snapshot-1", volumeID, "test-key-1", "test-value-1"),
	}
	ec2Snapshots = clients.EC2Snapshots{
		volumeID: createFakeSnapshot(time.Now().Add(time.Duration(-intervalSeconds+10)*time.Second), "snapshot-1", "ok"),
	}

	snapshotsErrorOnGet = nil
	volumesErrorOnGet = nil
	s.watcher.WatchSnapshots(&config)

	c.Assert(len(hook.Entries), Equals, 4)
	c.Assert(hook.Entries[0].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[0].Message, Equals, "checking volumes and snapshots")

	c.Assert(hook.Entries[1].Level, Equals, logrus.DebugLevel)
	c.Assert(strings.Contains(hook.Entries[1].Message, "volume volume-1 has an up to date snapshot"), Equals, true)

	c.Assert(hook.Entries[2].Level, Equals, logrus.InfoLevel)
	c.Assert(strings.Contains(hook.Entries[2].Message, "skipped snapshot removal, retention period not exceeded, volume: volume-1, snapshot id: snapshot-1"), Equals, true)

	c.Assert(hook.Entries[3].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[3].Message, Equals, "finished checking volumes and snapshots")
}

func (s *WatcherSuite) TestIfOldSnapshotNotDeletedOnCreateNewSnapshotError(c *C) {
	hook := test.NewGlobal()

	intervalSeconds := int64(11)
	config := models.VolumeSnapshotConfigs{
		{
			Labels: models.Label{
				Key:   "test-key-1",
				Value: "test-value-1",
			},
			IntervalSeconds:      intervalSeconds,
			RetentionPeriodHours: retentionPeriod,
		},
	}

	volumeID := "volume-1"
	ec2Volumes = clients.EC2Volumes{
		"test-key-1": createFakeVolume("snapshot-1", volumeID, "test-key-1", "test-value-1"),
	}
	ec2Snapshots = clients.EC2Snapshots{
		volumeID: createFakeSnapshot(time.Now().Add(time.Duration(-intervalSeconds-10)*time.Second), "snapshot-1", "ok"),
	}

	snapshotsErrorOnGet = nil
	volumesErrorOnGet = nil

	errorMsg := "test snapshots error message"
	SnapshotErrorOnCreate = errors.New(errorMsg)
	volumesErrorOnGet = nil
	snapshotErrorOnRemove = nil

	s.watcher.WatchSnapshots(&config)

	c.Assert(len(hook.Entries), Equals, 3)
	c.Assert(hook.Entries[0].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[0].Message, Equals, "checking volumes and snapshots")

	c.Assert(hook.Entries[1].Level, Equals, logrus.ErrorLevel)
	c.Assert(hook.Entries[1].Message, Equals, "error occurred while creating a new snapshot")
	c.Assert(hook.Entries[1].Data["error"].(error).Error(), Equals, errorMsg)

	c.Assert(hook.Entries[2].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[2].Message, Equals, "finished checking volumes and snapshots")
}

func (s *WatcherSuite) TestIfOldSnapshotNotDeletedWhenRetentionPeriodNotExceeded(c *C) {
	hook := test.NewGlobal()

	intervalSeconds := int64(11)
	config := models.VolumeSnapshotConfigs{
		{
			Labels: models.Label{
				Key:   "test-key-1",
				Value: "test-value-1",
			},
			IntervalSeconds:      intervalSeconds,
			RetentionPeriodHours: retentionPeriod,
		},
	}

	volumeID := "volume-1"
	ec2Volumes = clients.EC2Volumes{
		"test-key-1": createFakeVolume("snapshot-1", volumeID, "test-key-1", "test-value-1"),
	}
	ec2Snapshots = clients.EC2Snapshots{
		volumeID: createFakeSnapshot(time.Now().Add(time.Duration(-(retentionPeriod-1))*time.Hour), "snapshot-1", "ok"),
	}

	snapshotsErrorOnGet = nil
	volumesErrorOnGet = nil

	SnapshotErrorOnCreate = nil
	volumesErrorOnGet = nil
	snapshotErrorOnRemove = nil

	s.watcher.WatchSnapshots(&config)

	c.Assert(len(hook.Entries), Equals, 4)
	c.Assert(hook.Entries[0].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[0].Message, Equals, "checking volumes and snapshots")

	c.Assert(hook.Entries[1].Level, Equals, logrus.InfoLevel)
	c.Assert(strings.Contains(hook.Entries[1].Message, "created a new snapshot for volume-1 volume"), Equals, true)

	c.Assert(hook.Entries[2].Level, Equals, logrus.InfoLevel)
	c.Assert(strings.Contains(hook.Entries[2].Message, "skipped snapshot removal, retention period not exceeded, volume: volume-1, snapshot id: snapshot-1"), Equals, true)

	c.Assert(hook.Entries[3].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[3].Message, Equals, "finished checking volumes and snapshots")
}

func (s *WatcherSuite) TestIfOldSnapshotDeletedWhenRetentionPeriodExceeded(c *C) {
	hook := test.NewGlobal()

	intervalSeconds := int64(11)
	config := models.VolumeSnapshotConfigs{
		{
			Labels: models.Label{
				Key:   "test-key-1",
				Value: "test-value-1",
			},
			IntervalSeconds:      intervalSeconds,
			RetentionPeriodHours: retentionPeriod,
		},
	}

	volumeID := "volume-1"
	ec2Volumes = clients.EC2Volumes{
		"test-key-1": createFakeVolume("snapshot-1", volumeID, "test-key-1", "test-value-1"),
	}
	ec2Snapshots = clients.EC2Snapshots{
		volumeID: createFakeSnapshot(time.Now().Add(time.Duration(-(retentionPeriod))*time.Hour), "snapshot-1", "ok"),
	}

	snapshotsErrorOnGet = nil
	volumesErrorOnGet = nil

	SnapshotErrorOnCreate = nil
	volumesErrorOnGet = nil
	snapshotErrorOnRemove = nil

	s.watcher.WatchSnapshots(&config)

	c.Assert(len(hook.Entries), Equals, 4)
	c.Assert(hook.Entries[0].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[0].Message, Equals, "checking volumes and snapshots")

	c.Assert(hook.Entries[1].Level, Equals, logrus.InfoLevel)
	c.Assert(strings.Contains(hook.Entries[1].Message, "created a new snapshot for volume-1 volume, old snapshot id: snapshot-1"), Equals, true)

	c.Assert(hook.Entries[2].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[2].Message, Equals, "old snapshot with id snapshot-1 for volume volume-1 has been deleted")

	c.Assert(hook.Entries[3].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[3].Message, Equals, "finished checking volumes and snapshots")
}

func (s *WatcherSuite) TestIfOldSnapshotNotDeletedWhileRemovingOldSnapshotEncounteredError(c *C) {
	hook := test.NewGlobal()

	intervalSeconds := int64(11)
	config := models.VolumeSnapshotConfigs{
		{
			Labels: models.Label{
				Key:   "test-key-1",
				Value: "test-value-1",
			},
			IntervalSeconds:      intervalSeconds,
			RetentionPeriodHours: retentionPeriod,
		},
	}

	volumeID := "volume-1"
	ec2Volumes = clients.EC2Volumes{
		"test-key-1": createFakeVolume("snapshot-1", volumeID, "test-key-1", "test-value-1"),
	}
	ec2Snapshots = clients.EC2Snapshots{
		volumeID: createFakeSnapshot(time.Now().Add(time.Duration(-(retentionPeriod))*time.Hour), "snapshot-1", "ok"),
	}

	snapshotsErrorOnGet = nil
	volumesErrorOnGet = nil

	SnapshotErrorOnCreate = nil
	volumesErrorOnGet = nil

	errorMsg := "test remove old snapshot error message"
	snapshotErrorOnRemove = errors.New(errorMsg)

	s.watcher.WatchSnapshots(&config)

	c.Assert(len(hook.Entries), Equals, 4)
	c.Assert(hook.Entries[0].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[0].Message, Equals, "checking volumes and snapshots")

	c.Assert(hook.Entries[1].Level, Equals, logrus.InfoLevel)
	c.Assert(strings.Contains(hook.Entries[1].Message, "created a new snapshot for volume-1 volume"), Equals, true)

	c.Assert(hook.Entries[2].Level, Equals, logrus.ErrorLevel)
	c.Assert(hook.Entries[2].Message, Equals, "failed to remove old snapshot")
	c.Assert(hook.Entries[2].Data["error"].(error).Error(), Equals, errorMsg)

	c.Assert(hook.Entries[3].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[3].Message, Equals, "finished checking volumes and snapshots")
}

func (s *WatcherSuite) TestOnlyOldSnapshotDeletedWhenRetentionPeriodExceeded(c *C) {
	hook := test.NewGlobal()

	intervalSeconds := int64(11)
	config := models.VolumeSnapshotConfigs{
		{
			Labels: models.Label{
				Key:   "test-key-1",
				Value: "test-value-1",
			},
			IntervalSeconds:      intervalSeconds,
			RetentionPeriodHours: retentionPeriod,
		},
	}

	volumeID := "volume-1"
	ec2Volumes = clients.EC2Volumes{
		"test-key-1": createFakeVolume("snapshot-1", volumeID, "test-key-1", "test-value-1"),
	}
	snapshotIDOne := "snapshot-1"
	snapshotIDTwo := "snapshot-2"
	snapshotState := "ok"
	retentionExceeded := time.Now().Add(time.Duration(-(retentionPeriod)) * time.Hour)
	retentionNotExceeded := time.Now().Add(time.Duration(-(retentionPeriod - 1)) * time.Hour)
	ec2Snapshots = clients.EC2Snapshots{
		volumeID: []*ec2.Snapshot{
			{
				SnapshotId: &snapshotIDOne,
				StartTime:  &retentionExceeded,
				State:      &snapshotState,
			},
			{
				SnapshotId: &snapshotIDTwo,
				StartTime:  &retentionNotExceeded,
				State:      &snapshotState,
			},
		},
	}

	snapshotsErrorOnGet = nil
	volumesErrorOnGet = nil

	SnapshotErrorOnCreate = nil
	volumesErrorOnGet = nil
	snapshotErrorOnRemove = nil

	s.watcher.WatchSnapshots(&config)

	c.Assert(len(hook.Entries), Equals, 5)
	c.Assert(hook.Entries[0].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[0].Message, Equals, "checking volumes and snapshots")

	c.Assert(hook.Entries[1].Level, Equals, logrus.InfoLevel)
	c.Assert(strings.Contains(hook.Entries[1].Message, "created a new snapshot for volume-1 volume"), Equals, true)

	c.Assert(hook.Entries[2].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[2].Message, Equals, "old snapshot with id snapshot-1 for volume volume-1 has been deleted")

	c.Assert(hook.Entries[3].Level, Equals, logrus.InfoLevel)
	c.Assert(strings.Contains(hook.Entries[3].Message, "skipped snapshot removal, retention period not exceeded, volume: volume-1, snapshot id: snapshot-2"), Equals, true)

	c.Assert(hook.Entries[4].Level, Equals, logrus.InfoLevel)
	c.Assert(hook.Entries[4].Message, Equals, "finished checking volumes and snapshots")
}

func createFakeVolume(snapshotId, volumeId, tagKey, tagValue string) *ec2.Volume {
	return &ec2.Volume{
		SnapshotId: &snapshotId,
		VolumeId:   &volumeId,
		Tags: []*ec2.Tag{
			{
				Key:   &tagKey,
				Value: &tagValue,
			},
		},
	}
}

func createFakeSnapshot(startTime time.Time, snapshotID, snapshotState string) []*ec2.Snapshot {
	return []*ec2.Snapshot{
		{
			SnapshotId: &snapshotID,
			StartTime:  &startTime,
			State:      &snapshotState,
		},
	}
}

type Client interface {
	GetVolumes() (clients.EC2Volumes, error)
	GetSnapshots() (clients.EC2Snapshots, error)
	CreateSnapshot(volume *ec2.Volume) error
	RemoveSnapshot(snapshot *ec2.Snapshot) error
}

type MockClient struct{}

func (c *MockClient) GetVolumes() (clients.EC2Volumes, error) {
	return ec2Volumes, volumesErrorOnGet
}

func (c *MockClient) GetSnapshots() (clients.EC2Snapshots, error) {
	return ec2Snapshots, snapshotsErrorOnGet
}

func (c *MockClient) CreateSnapshot(volume *ec2.Volume) error {
	return SnapshotErrorOnCreate
}

func (c *MockClient) RemoveSnapshot(snapshot *ec2.Snapshot) error {
	return snapshotErrorOnRemove
}
