package models

type VolumeSnapshotConfigs []*VolumeSnapshotConfig

type VolumeSnapshotConfig struct {
	Labels          Label `json:"labels"`
	IntervalSeconds int64 `json:"intervalSeconds"`
}

type Label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
