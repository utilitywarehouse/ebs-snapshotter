package models

// VolumeSnapshotConfigs type alias for volume snapshot config slice
type VolumeSnapshotConfigs []*VolumeSnapshotConfig

// VolumeSnapshotConfig used to store volume snapshot configuration details
type VolumeSnapshotConfig struct {
	Labels          Label `json:"labels"`
	IntervalSeconds int64 `json:"intervalSeconds"`
}

// Label used to store volume and snapshot information
type Label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
