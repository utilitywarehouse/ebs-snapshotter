# EBS Snapshotter

Takes snapshots of EBS volumes and deletes old EBS snapshots when the retention
period is exceeded.

Requires a configuration file shown below. It will list all EBS volumes
available and create a snapshot for any that match the label key and value
provided and only if the last snapshot was taken greater than `intervalSeconds`
ago.  The old snapshots are removed when snapshot match the label key, value
and the `retentionPeriodHours` is exceeded.

## Example configuration file
```json
[
  {
    "retentionPeriodHours": 336,
    "intervalSeconds": 43200,
    "labels": {
      "key": "kubernetes.io/created-for/pvc/name",
      "value": "datadir-kafka-0"
    }
  }
]
```
