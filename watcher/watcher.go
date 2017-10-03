package watcher

import (
	"time"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/utilitywarehouse/ebs-snapshotter/clients"
	"github.com/utilitywarehouse/ebs-snapshotter/models"
)

// Watcher interface specifies EBS snapshot watcher functions
type Watcher interface {
	WatchSnapshots(config *models.VolumeSnapshotConfigs)
}

// EBSSnapshotWatcher used to check EC2 EBS snapshots
type EBSSnapshotWatcher struct {
	ebsClient                         clients.EBSClient
	crCounter, delCounter, errCounter *prometheus.CounterVec
}

// NewEBSSnapshotWatcher used to create a new instance of EBS snapshot watcher
func NewEBSSnapshotWatcher(
	ebsClient clients.EBSClient,
	crCounter, delCounter, errCounter *prometheus.CounterVec) *EBSSnapshotWatcher {

	return &EBSSnapshotWatcher{
		ebsClient:  ebsClient,
		crCounter:  crCounter,
		delCounter: delCounter,
		errCounter: errCounter,
	}
}

// WatchSnapshots used to check EBS snapshots to create new ones and/or delete old ones.
func (w *EBSSnapshotWatcher) WatchSnapshots(config *models.VolumeSnapshotConfigs) error {
	volumes, err := w.ebsClient.GetVolumes()
	if err != nil {
		return errors.Wrap(err, "error while fetching volumes")
	}

	snapshots, err := w.ebsClient.GetSnapshots()
	if err != nil {
		return errors.Wrap(err, "error while fetching snapshots")
	}

	log.Info("checking volumes and snapshots")
	for _, config := range *config {
		retentionStartDate := time.Now().Add(-time.Duration(config.RetentionPeriodHours) * time.Hour)
		acceptableStartTime := time.Now().Add(time.Duration(-config.IntervalSeconds) * time.Second)

		key := config.Labels.Key
		val := config.Labels.Value
		for _, volume := range volumes {
			for _, tag := range volume.Tags {
				if *tag.Key == key && *tag.Value == val {
					var latestSnapshot *ec2.Snapshot

					// If the volume already have at least one snapshot, use the latest
					if len(snapshots[*volume.VolumeId]) > 0 {
						latestSnapshot = snapshots[*volume.VolumeId][0]
					}
					if err := createNewEBSSnapshot(w, latestSnapshot, volume, acceptableStartTime, tag.Value); err != nil {
						log.WithError(err).Error("error occurred while creating a new snapshot")
						continue
					}

					// Removing all old snapshots for given volume
					for _, snapshot := range snapshots[*volume.VolumeId] {
						if err := removeOldEBSSnapshot(w, snapshot, volume, retentionStartDate, tag.Value); err != nil {
							log.WithError(err).Error("failed to remove old snapshot")
						}
						time.Sleep(2 * time.Second) // A delay so that we don't exceed AWS request limits
					}
				}
			}
		}
	}
	log.Info("finished checking volumes and snapshots")
	return nil
}

func createNewEBSSnapshot(
	w *EBSSnapshotWatcher,
	snapshot *ec2.Snapshot,
	volume *ec2.Volume,
	acceptableStartTime time.Time,
	tag *string) error {

	if snapshot != nil && !snapshot.StartTime.Before(acceptableStartTime) && *snapshot.State != "error" {
		log.Debugf("volume %s has an up to date snapshot, snapshot start time: %s, acceptable start time: %s",
			*volume.VolumeId, *snapshot.StartTime, acceptableStartTime)
		return nil
	}
	if err := w.ebsClient.CreateSnapshot(volume); err != nil {
		w.errCounter.WithLabelValues(*tag).Inc()
		return err
	}
	log.Infof(
		"created a new snapshot for %s volume, old snapshot id: %s; snapshot start time: %s, acceptable start time: %s",
		*volume.VolumeId, *snapshot.SnapshotId, *snapshot.StartTime, acceptableStartTime)
	w.crCounter.WithLabelValues(*tag).Inc()
	return nil
}

func removeOldEBSSnapshot(
	w *EBSSnapshotWatcher,
	snapshot *ec2.Snapshot,
	volume *ec2.Volume,
	retentionStartDate time.Time,
	tag *string) error {

	if snapshot != nil && snapshot.StartTime.After(retentionStartDate) {
		log.Infof(
			"skipped snapshot removal, retention period not exceeded, "+
				"volume: %s, snapshot id: %s, snapshot start time: %s, retention start time: %s",
			*volume.VolumeId,
			*snapshot.SnapshotId,
			*snapshot.StartTime,
			retentionStartDate)
		return nil
	}

	// An error is an indication of a state that is not valid for old snapshot to be removed.
	// This is done to avoid removing last remaining ebs snapshot in case of error.
	if err := w.ebsClient.RemoveSnapshot(snapshot); err != nil {
		w.errCounter.WithLabelValues(*tag).Inc()
		return err
	}

	w.delCounter.WithLabelValues(*tag).Inc()
	log.Infof(
		"old snapshot with id %s for volume %s has been deleted",
		*snapshot.SnapshotId, *volume.VolumeId)

	return nil
}
