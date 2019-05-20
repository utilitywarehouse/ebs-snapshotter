package watcher_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/prometheus/client_golang/prometheus"
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
	snapshotCounter                   *prometheus.GaugeVec

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
	crCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "snapshots_performed",
		Help: "A counter of the total number of snapshots created",
	}, []string{"pvc_name", "pvc_namespace", "volume_id"})
	delCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "old_snapshots_removed",
		Help: "A counter of the total number of old snapshots removed",
	}, []string{"pvc_name", "pvc_namespace", "volume_id"})
	errCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "errors_total",
		Help: "A counter of the total number of errors encountered",
	}, []string{"pvc_name", "pvc_namespace", "volume_id"})
	snapshotCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "snapshots_total",
		Help: "A counter of the total number of snapshots",
	}, []string{"pvc_name", "pvc_namespace", "volume_id"})

	s.watcher = w.NewEBSSnapshotWatcher(&MockClient{}, crCounter, delCounter, errCounter, snapshotCounter)
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
}

func (s *WatcherSuite) TestIfOldSnapshotNotDeletedOnCreateNewSnapshotError(c *C) {
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
}

func (s *WatcherSuite) TestIfOldSnapshotNotDeletedWhenRetentionPeriodNotExceeded(c *C) {
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
}

func (s *WatcherSuite) TestIfOldSnapshotDeletedWhenRetentionPeriodExceeded(c *C) {
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

}

func (s *WatcherSuite) TestIfOldSnapshotNotDeletedWhileRemovingOldSnapshotEncounteredError(c *C) {
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

}

func (s *WatcherSuite) TestOnlyOldSnapshotDeletedWhenRetentionPeriodExceeded(c *C) {
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
