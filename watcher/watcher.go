package watcher

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/utilitywarehouse/ebs-snapshotter/clients"
	"github.com/utilitywarehouse/ebs-snapshotter/models"
	"time"
)

type Watcher interface {
	WatchSnapshots(config *models.VolumeSnapshotConfigs)
}

type EBSSnapshotWatcher struct {
	retentionPeriod                int
	ebsClient                      clients.Client
	createdCounter, deletedCounter *prometheus.CounterVec
}

func NewEBSSnapshotWatcher(
	retentionPeriod int,
	ebsClient clients.Client,
	createdCounter, deletedCounter *prometheus.CounterVec) *EBSSnapshotWatcher {

	return &EBSSnapshotWatcher{
		retentionPeriod: retentionPeriod,
		ebsClient:       ebsClient,
		createdCounter:  createdCounter,
		deletedCounter:  deletedCounter,
	}
}

func (w *EBSSnapshotWatcher) WatchSnapshots(config *models.VolumeSnapshotConfigs) error {
	vols, err := w.ebsClient.GetVolumes()
	if err != nil {
		return errors.Wrap(err, "error while fetching volumes")
	}

	snaps, err := w.ebsClient.GetSnapshots()
	if err != nil {
		return errors.Wrap(err, "error while fetching snapshots")
	}

	log.Info("checking volumes and snapshots")
	retentionStartDate := time.Now().Add(-time.Duration(w.retentionPeriod) * time.Hour)
	for _, config := range *config {
		key := config.Labels.Key
		val := config.Labels.Value

		acceptableStartTime := time.Now().Add(time.Duration(-config.IntervalSeconds) * time.Second)
		for _, vol := range vols {
			for _, tag := range vol.Tags {
				if *tag.Key == key && *tag.Value == val {
					lastSnapshot := snaps[*vol.VolumeId]

					if lastSnapshot != nil && !lastSnapshot.StartTime.Before(acceptableStartTime) &&
						*lastSnapshot.State != "error" {

						log.Infof("volume %s has an up to date snapshot", *vol.VolumeId)
						continue
					}
					if err := w.ebsClient.CreateSnapshot(vol, lastSnapshot); err != nil {
						log.WithError(err).Error("error occurred while creating snapshot")
						continue
					}
					log.Infof("created snapshot for volume %s", *vol.VolumeId)
					w.createdCounter.WithLabelValues(*vol.VolumeId).Inc()

					if lastSnapshot != nil && lastSnapshot.StartTime.After(retentionStartDate) {
						log.Infof(
							"skipped snapshot removal, retention period not exceeded: "+
								"volume - %s; snapshot - %s",
							*vol.VolumeId,
							*lastSnapshot.SnapshotId)
						continue
					}

					// An error is an indication of a state that is not valid for old snapshot to be removed.
					// This is done to avoid removing last remaining ebs snapshot in case of error.
					if err := w.ebsClient.RemoveSnapshot(vol, lastSnapshot); err != nil {
						log.WithError(err).Error("failed to remove old snapshot")
						continue
					}

					w.deletedCounter.WithLabelValues(*vol.VolumeId, *lastSnapshot.SnapshotId).Inc()
					log.Infof(
						"old snapshot with id %s for volume %s has been deleted",
						*lastSnapshot.SnapshotId, *vol.VolumeId)
				}
			}
		}
	}
	log.Info("finished checking volumes and snapshots")
	return nil
}
