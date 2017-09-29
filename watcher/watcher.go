package watcher

import (
	"time"

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

					if lastSnapshot != nil && !lastSnapshot.StartTime.Before(acceptableStartTime) &&
						*lastSnapshot.State != "error" {

						log.Infof("volume %s has an up to date snapshot", *volume.VolumeId)
						continue
					}
					if err := w.ebsClient.CreateSnapshot(volume); err != nil {
						log.WithError(err).Error("error occurred while creating snapshot")
						continue
					}
					log.Infof("created snapshot for volume %s", *volume.VolumeId)
					w.createdCounter.WithLabelValues(*volume.VolumeId).Inc()

					if lastSnapshot != nil && lastSnapshot.StartTime.After(retentionStartDate) {
						log.Infof(
							"skipped snapshot removal, retention period not exceeded: "+
								"volume - %s; snapshot - %s",
							*volume.VolumeId,
							*lastSnapshot.SnapshotId)
						continue
					}

					// An error is an indication of a state that is not valid for old snapshot to be removed.
					// This is done to avoid removing last remaining ebs snapshot in case of error.
					if err := w.ebsClient.RemoveSnapshot(lastSnapshot); err != nil {
						log.WithError(err).Error("failed to remove old snapshot")
						continue
					}

					w.deletedCounter.WithLabelValues(*volume.VolumeId, *lastSnapshot.SnapshotId).Inc()
					log.Infof(
						"old snapshot with id %s for volume %s has been deleted",
						*lastSnapshot.SnapshotId, *volume.VolumeId)
				}
			}
		}
	}
	log.Info("finished checking volumes and snapshots")
	return nil
}
