# EBS Snapshotter

Takes snapshots of EBS volumes.

Requires a configuration file shown below. It will list all EBS volumes available and
create a snapshot for any that match the label key and value provided and only if
the last snapshot was taken greater than `intervalSeconds` ago.

```
[
  {
    "intervalSeconds": 43200,
    "labels": {
      "key": "kubernetes.io/created-for/pvc/name",
      "value": "datadir-kafka-0"
    }
  }
]
```


Delete me if [this](https://github.com/kubernetes/community/blob/master/contributors/design-proposals/volume-snapshotting.md) goes anywhere.
