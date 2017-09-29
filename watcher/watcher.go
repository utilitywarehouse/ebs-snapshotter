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
	ebsClient                      clients.EBSClient
	createdCounter, deletedCounter *prometheus.CounterVec
}

// NewEBSSnapshotWatcher used to create a new instance of EBS snapshot watcher
func NewEBSSnapshotWatcher(
	ebsClient clients.EBSClient,
	createdCounter, deletedCounter *prometheus.CounterVec) *EBSSnapshotWatcher {

	return &EBSSnapshotWatcher{
		ebsClient:      ebsClient,
		createdCounter: createdCounter,
		deletedCounter: deletedCounter,
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
		key := config.Labels.Key
		val := config.Labels.Value

		acceptableStartTime := time.Now().Add(time.Duration(-config.IntervalSeconds) * time.Second)
		for _, volume := range volumes {
			for _, tag := range volume.Tags {
				if *tag.Key == key && *tag.Value == val {
					lastSnapshot := snapshots[*volume.VolumeId]

					if err := createNewEBSSnapshot(w, lastSnapshot, volume, acceptableStartTime); err != nil {
						log.WithError(err).Error("error occurred while creating a new snapshot")
						continue
					}
					removeOldEBSSnapshot(w, lastSnapshot, volume, retentionStartDate)
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
	acceptableStartTime time.Time) error {

	if snapshot != nil && !snapshot.StartTime.Before(acceptableStartTime) && *snapshot.State != "error" {
		log.Debugf("volume %s has an up to date snapshot", *volume.VolumeId)
		return nil
	}
	if err := w.ebsClient.CreateSnapshot(volume); err != nil {
		return err
	}
	log.Infof("created a new snapshot for volume %s", *volume.VolumeId)
	w.createdCounter.WithLabelValues(*volume.VolumeId).Inc()
	return nil
}

func removeOldEBSSnapshot(
	w *EBSSnapshotWatcher,
	snapshot *ec2.Snapshot,
	volume *ec2.Volume,
	retentionStartDate time.Time) {

	if snapshot != nil && snapshot.StartTime.After(retentionStartDate) {
		log.Infof(
			"skipped snapshot removal, retention period not exceeded: "+
				"volume - %s; snapshot - %s",
			*volume.VolumeId,
			*snapshot.SnapshotId)
		return
	}

	// An error is an indication of a state that is not valid for old snapshot to be removed.
	// This is done to avoid removing last remaining ebs snapshot in case of error.
	if err := w.ebsClient.RemoveSnapshot(snapshot); err != nil {
		log.WithError(err).Error("failed to remove old snapshot")
		return
	}

	w.deletedCounter.WithLabelValues(*volume.VolumeId, *snapshot.SnapshotId).Inc()
	log.Infof(
		"old snapshot with id %s for volume %s has been deleted",
		*snapshot.SnapshotId, *volume.VolumeId)
}
