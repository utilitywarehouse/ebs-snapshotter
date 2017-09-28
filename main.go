package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/utilitywarehouse/ebs-snapshotter/clients"
	"github.com/utilitywarehouse/go-operational/op"
)

var (
	gitHash        string = ""
	NAME                  = "ebs-snapshotter"
	DESC                  = `Snapshots EBS volumes automatically`
	createdCounter *prometheus.CounterVec
	deletedCounter *prometheus.CounterVec
	errors         prometheus.Counter
)

type VolumeSnapshotConfigs []*VolumeSnapshotConfig

type VolumeSnapshotConfig struct {
	Labels          Label `json:"labels"`
	IntervalSeconds int64 `json:"intervalSeconds"`
}

type Label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	app := cli.App(NAME, DESC)
	httpPort := app.Int(cli.IntOpt{
		Name:   "http-port",
		Desc:   "HTTP port to listen on ",
		EnvVar: "HTTP_POST",
		Value:  8080,
	})
	awsAccessKey := app.String(cli.StringOpt{
		Name:   "aws-access-key",
		Desc:   "An AWS access key",
		EnvVar: "AWS_ACCESS_KEY",
		Value:  "",
	})
	awsSecretKey := app.String(cli.StringOpt{
		Name:   "aws-secret-key",
		Desc:   "An AWS secret key",
		EnvVar: "AWS_SECRET_KEY",
		Value:  "",
	})
	volumeSnapshotConfigFile := app.String(cli.StringOpt{
		Name:   "volume-snapshot-config-file",
		Desc:   "A path to the volume snapshot json config file",
		EnvVar: "VOLUME_SNAPSHOT_CONFIG_FILE",
		Value:  "",
	})
	pollIntervalSeconds := app.Int(cli.IntOpt{
		Name:   "poll-interval-seconds",
		Desc:   "The interval in seconds between snapshot freshness checks",
		EnvVar: "POLL_INTERVAL_SECONDS",
		Value:  1800,
	})
	ec2Region := app.String(cli.StringOpt{
		Name:   "region",
		Desc:   "AWS Region",
		EnvVar: "AWS_REGION",
		Value:  "eu-west-1",
	})
	oldSnapshotsRetentionPeriod := app.Int(cli.IntOpt{
		Name:   "old-snapshots-retention-period-hours",
		Desc:   "Specifies for how long time period to retain the old EBS snapshots",
		EnvVar: "OLD_SNAPSHOTS_RETENTION_PERIOD_HOURS",
		Value:  168,
	})

	createdCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "snapshots_performed",
		Help: "A counter of the total number of snapshots created",
	}, []string{"volumeId"})
	deletedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "old_snapshots_removed",
		Help: "A counter of the total number of old snapshots removed",
	}, []string{"volumeId", "snapshotId"})
	errors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "errors",
		Help: "A counter of the total number of errors encountered",
	})

	prometheus.DefaultRegisterer.MustRegister(createdCounter, deletedCounter, errors)

	app.Action = func() {
		snapshotConfigs := LoadVolumeSnapshotConfig(*volumeSnapshotConfigFile)
		ec2Client := CreateEc2Client(*awsAccessKey, *awsSecretKey, *ec2Region)

		ebsClient := clients.NewEBSClient(ec2Client)

		go initialiseHttpServer(*httpPort)
		WatchSnapshots(
			*pollIntervalSeconds,
			*oldSnapshotsRetentionPeriod,
			snapshotConfigs,
			ebsClient,
			createdCounter,
			deletedCounter)
	}
	app.Run(os.Args)
}

func WatchSnapshots(
	intervalSeconds, retentionPeriod int,
	snapshotConfigs *VolumeSnapshotConfigs,
	ebsClient clients.Client,
	createdCounter, deletedCounter *prometheus.CounterVec) {

	for {
		vols, err := ebsClient.GetVolumes()
		if err != nil {
			log.Printf("error while fetching volumes: %v", err)
		}

		snaps, err := ebsClient.GetSnapshots()
		if err != nil {
			log.Printf("error while fetching snapshots: %v", err)
		}

		log.Print("checking volumes and snapshots")
		retentionStartDate := time.Now().Add(-time.Duration(retentionPeriod) * time.Hour)
		for _, config := range *snapshotConfigs {
			key := config.Labels.Key
			val := config.Labels.Value

			acceptableStartTime := time.Now().Add(time.Duration(-config.IntervalSeconds) * time.Second)
			for _, vol := range vols {
				for _, tag := range vol.Tags {
					if *tag.Key == key && *tag.Value == val {
						lastSnapshot := snaps[*vol.VolumeId]

						if lastSnapshot != nil && !lastSnapshot.StartTime.Before(acceptableStartTime) && *lastSnapshot.State != "error" {
							log.Printf("volume %s has an up to date snapshot", *vol.VolumeId)
							continue
						}
						if err := ebsClient.CreateSnapshot(vol, lastSnapshot); err != nil {
							log.Printf("error occured while creating snapshot: %v", err)
							continue
						}
						log.Printf("created snapshot for volume %s", *vol.VolumeId)
						createdCounter.WithLabelValues(*vol.VolumeId).Inc()

						if lastSnapshot == nil && lastSnapshot.StartTime.After(retentionStartDate) {
							log.Printf(
								"skiped snapshot removal, retention period not exceeded: "+
									"volume - %s; snapshot - %s",
								*vol.VolumeId,
								lastSnapshot.SnapshotId)
							continue
						}
						deletedCounter.WithLabelValues(*vol.VolumeId, *lastSnapshot.SnapshotId).Inc()
						log.Printf("old snapshot with id %s for volume %s has been deleted", *vol.VolumeId, *lastSnapshot.SnapshotId)

						// An error is an indication of a state that is not valid for old snapshot to be removed.
						// This is done to avoid removing last remaining ebs snapshot in case of error.
						if err := ebsClient.RemoveSnapshot(vol, lastSnapshot); err != nil {
							log.Printf("failed to remove old snapshot: %v", err)
						}
					}
				}
			}
		}
		log.Print("finished checking volumes and snapshots")
		<-time.After(time.Duration(intervalSeconds) * time.Second)
	}
}

func CreateEc2Client(awsAccessKey string, awsSecretKey string, ec2Region string) *ec2.EC2 {
	config := aws.NewConfig()
	config.WithCredentials(credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, "")).
		WithRegion(ec2Region)
	awsSess := session.New(config)
	ec2Client := ec2.New(awsSess)
	return ec2Client
}

func LoadVolumeSnapshotConfig(volumeSnapshotConfigFile string) *VolumeSnapshotConfigs {
	confFile, err := os.Open(volumeSnapshotConfigFile)
	if err != nil {
		log.Fatalf("Error while opening volume snapshot config file: %v", err)
	}
	fileContent, err := ioutil.ReadAll(confFile)
	if err != nil {
		log.Fatalf("Error while reading volume snapshot config file: %v", err)
	}
	snapshotConfigs := &VolumeSnapshotConfigs{}
	if err = json.Unmarshal(fileContent, snapshotConfigs); err != nil {
		log.Fatalf("Error while deserialising volume snapshot config file: %v", err)
	}
	return snapshotConfigs
}

func CreateSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot, acceptableStartTime time.Time, ec2Client *ec2.EC2) error {
	if lastSnapshot != nil && !lastSnapshot.StartTime.Before(acceptableStartTime) && *lastSnapshot.State != "error" {
		log.Printf("Volume %s has an up to date snapshot", *vol.VolumeId)
		return fmt.Errorf("volume %s has an up to date snapshot", *vol.VolumeId)
	}
	err := makeSnapshot(ec2Client, vol)
	if err != nil {
		errors.Inc()
		return fmt.Errorf("error creating snapshot for volume %s: %v", *vol.VolumeId, err)
	}
	log.Printf("created snapshot for volume %s", *vol.VolumeId)
	createdCounter.WithLabelValues(*vol.VolumeId).Inc()
	return nil
}

func makeSnapshot(ec2Client *ec2.EC2, volume *ec2.Volume) error {
	desc := string("Created by ebs-snapshotter")
	_, err := ec2Client.CreateSnapshot(&ec2.CreateSnapshotInput{
		VolumeId:    volume.VolumeId,
		Description: &desc,
	})
	return err
}

func initialiseHttpServer(port int) {
	router := mux.NewRouter()

	router.NewRoute().PathPrefix("/__/").
		Methods(http.MethodGet).
		Handler(getOpHandler())

	router.NewRoute().Path("/_/metrics").
		Methods(http.MethodGet).
		Handler(promhttp.Handler())

	loggingHandler := handlers.LoggingHandler(os.Stdout, router)
	http.Handle("/", loggingHandler)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func getOpHandler() http.Handler {
	return op.NewHandler(
		op.NewStatus(NAME, DESC).
			AddOwner("telecom", "#telecom").
			SetRevision(gitHash).
			ReadyUseHealthCheck().
			AddLink("VCS Repo", "https://github.com/utilitywarehouse/ebs-snapshotter"),
	)
}
