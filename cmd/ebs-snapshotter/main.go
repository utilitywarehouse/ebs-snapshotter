package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/utilitywarehouse/ebs-snapshotter/clients"
	"github.com/utilitywarehouse/ebs-snapshotter/models"
	w "github.com/utilitywarehouse/ebs-snapshotter/watcher"
)

const (
	name        = "ebs-snapshotter"
	description = `Snapshots EBS volumes automatically`
)

var (
	gitHash                           string
	crCounter, delCounter, errCounter *prometheus.CounterVec
	snapshotCounter                   *prometheus.GaugeVec
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func main() {
	var (
		httpPort                 = getEnv("HTTP_PORT", "8080")
		volumeSnapshotConfigFile = getEnv("VOLUME_SNAPSHOT_CONFIG_FILE", "")
		pollIntervalSeconds      = getEnv("POLL_INTERVAL_SECONDS", "1800")
	)

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

	prometheus.DefaultRegisterer.MustRegister(crCounter, delCounter, errCounter, snapshotCounter)

	snapshotConfigs := loadVolumeSnapshotConfig(volumeSnapshotConfigFile)

	sess, err := session.NewSession(&aws.Config{})
	ec2Client := ec2.New(sess)
	ebsClient := clients.NewEBSClient(ec2Client)

	watcher := w.NewEBSSnapshotWatcher(ebsClient, crCounter, delCounter, errCounter, snapshotCounter)

	httpPortInt, err := strconv.Atoi(httpPort)
	if err != nil {
		log.Fatalf("httpPort must be convertible to Int, got %v", httpPort)
	}
	pollIntSecInt, err := strconv.Atoi(pollIntervalSeconds)
	if err != nil {
		log.Fatalf("pollIntervalSeconds must be convertible to Int, got %v", httpPort)
	}

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		http.ListenAndServe(fmt.Sprintf(":%d", httpPortInt), promhttp.Handler())
	}()
	log.Printf("Listening on port %v", httpPortInt)

	for {
		watcher.WatchSnapshots(snapshotConfigs)
		<-time.After(time.Duration(pollIntSecInt) * time.Second)
		log.Printf("Watching snapshots")
	}
}

func loadVolumeSnapshotConfig(volumeSnapshotConfigFile string) *models.VolumeSnapshotConfigs {
	confFile, err := os.Open(volumeSnapshotConfigFile)
	if err != nil {
		log.Fatalf("Error while opening volume snapshot config file: %v", err)
	}
	fileContent, err := ioutil.ReadAll(confFile)
	if err != nil {
		log.Fatalf("Error while reading volume snapshot config file: %v", err)
	}
	snapshotConfigs := &models.VolumeSnapshotConfigs{}
	if err = json.Unmarshal(fileContent, snapshotConfigs); err != nil {
		log.Fatalf("Error while deserialising volume snapshot config file: %v", err)
	}
	return snapshotConfigs
}
