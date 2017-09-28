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
	"github.com/utilitywarehouse/go-operational/op"
)

var (
	gitHash          string = ""
	NAME                    = "ebs-snapshotter"
	DESC                    = `Snapshots EBS volumes automatically`
	snapshotsCreated *prometheus.CounterVec
	snapshotsDeleted *prometheus.CounterVec
	errors           prometheus.Counter
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

	snapshotsCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "snapshots_performed",
		Help: "A counter of the total number of snapshots created",
	}, []string{"volumeId"})
	snapshotsDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "old_snapshots_removed",
		Help: "A counter of the total number of old snapshots removed",
	}, []string{"volumeId", "snapshotId"})
	errors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "errors",
		Help: "A counter of the total number of errors encountered",
	})

	prometheus.DefaultRegisterer.MustRegister(snapshotsCreated, snapshotsDeleted, errors)

	app.Action = func() {
		snapshotConfigs := LoadVolumeSnapshotConfig(*volumeSnapshotConfigFile)
		ec2Client := CreateEc2Client(*awsAccessKey, *awsSecretKey, *ec2Region)

		go initialiseHttpServer(*httpPort)
		WatchSnapshots(*pollIntervalSeconds, *oldSnapshotsRetentionPeriod, ec2Client, snapshotConfigs)
	}
	app.Run(os.Args)
}

func WatchSnapshots(intervalSeconds, retentionPeriod int, ec2Client *ec2.EC2, snapshotConfigs *VolumeSnapshotConfigs) {
	for {
		vols, err := getVolumes(ec2Client)
		if err != nil {
			log.Printf("error while fetching volumes: %v", err)

		}

		snaps, err := getSnapshots(ec2Client)
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
						CheckSnapshot(vol, lastSnapshot, acceptableStartTime, ec2Client)
						RemoveOldSnapshot(vol, lastSnapshot, retentionStartDate, ec2Client)
					}
				}
			}
		}
		log.Print("finished checking volumes and snapshots")
		<-time.After(time.Duration(intervalSeconds) * time.Second)
	}
}

func RemoveOldSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot, retentionStartDate time.Time, ec2Client *ec2.EC2) {
	if lastSnapshot != nil && lastSnapshot.StartTime.Before(retentionStartDate) {
		deleteSnapshot(ec2Client, lastSnapshot.SnapshotId)
		return
	}
	log.Printf("old snapshot with id %s for volume %s has been deleted", *vol.VolumeId, *lastSnapshot.SnapshotId)

	snapshotsDeleted.WithLabelValues(*vol.VolumeId, *lastSnapshot.SnapshotId).Inc()
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

func CheckSnapshot(vol *ec2.Volume, lastSnapshot *ec2.Snapshot, acceptableStartTime time.Time, ec2Client *ec2.EC2) {
	if lastSnapshot != nil && !lastSnapshot.StartTime.Before(acceptableStartTime) && *lastSnapshot.State != "error" {
		log.Printf("Volume %s has an up to date snapshot", *vol.VolumeId)
		return
	}
	err := makeSnapshot(ec2Client, vol)
	if err != nil {
		log.Printf("Error creating snapshot for volume %s: %v", *vol.VolumeId, err)
		errors.Inc()
		return
	}
	log.Printf("Created snapshot for volume %s", *vol.VolumeId)
	snapshotsCreated.WithLabelValues(*vol.VolumeId).Inc()
}

func makeSnapshot(ec2Client *ec2.EC2, volume *ec2.Volume) error {
	desc := string("Created by ebs-snapshotter")
	_, err := ec2Client.CreateSnapshot(&ec2.CreateSnapshotInput{
		VolumeId:    volume.VolumeId,
		Description: &desc,
	})
	return err
}

func deleteSnapshot(ec2Client *ec2.EC2, snapshotId *string) error {
	_, err := ec2Client.DeleteSnapshot(&ec2.DeleteSnapshotInput{
		SnapshotId: snapshotId,
	})
	return err
}

func getSnapshots(ec2Client *ec2.EC2) (map[string]*ec2.Snapshot, error) {
	maxResults := int64(1000)
	snapshots := make([]*ec2.Snapshot, 0)
	snaps, err := ec2Client.DescribeSnapshots(&ec2.DescribeSnapshotsInput{MaxResults: &maxResults})
	if err != nil {
		return nil, fmt.Errorf("Error while describing volumes: %v", err)
	}
	snapshots = append(snapshots, snaps.Snapshots...)
	for snaps.NextToken != nil {
		snaps, err = ec2Client.DescribeSnapshots(&ec2.DescribeSnapshotsInput{
			MaxResults: &maxResults,
			NextToken:  snaps.NextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("Error while describing volumes: %v", err)
		}
		snapshots = append(snapshots, snaps.Snapshots...)
	}
	return MapMostRecentSnapshotToVolumes(snapshots), nil
}

func MapMostRecentSnapshotToVolumes(snapshots []*ec2.Snapshot) map[string]*ec2.Snapshot {
	output := make(map[string]*ec2.Snapshot)
	for _, snapshot := range snapshots {
		existingSnapshot := output[*snapshot.VolumeId]
		if existingSnapshot == nil || existingSnapshot.StartTime.Before(*snapshot.StartTime) {
			output[*snapshot.VolumeId] = snapshot
		}
	}
	return output
}

func getVolumes(ec2Client *ec2.EC2) (map[string]*ec2.Volume, error) {
	maxResults := int64(1000)
	volumes := make([]*ec2.Volume, 0)
	vols, err := ec2Client.DescribeVolumes(&ec2.DescribeVolumesInput{
		MaxResults: &maxResults,
	})
	if err != nil {
		return nil, fmt.Errorf("Error while describing volumes: %v", err)
	}
	volumes = append(volumes, vols.Volumes...)
	for vols.NextToken != nil {
		vols, err := ec2Client.DescribeVolumes(&ec2.DescribeVolumesInput{
			MaxResults: &maxResults,
			NextToken:  vols.NextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("Error while describing volumes: %v", err)
		}
		volumes = append(volumes, vols.Volumes...)
	}
	return MapVolumesToIds(volumes), nil
}

func MapVolumesToIds(volumes []*ec2.Volume) map[string]*ec2.Volume {
	output := make(map[string]*ec2.Volume)
	for _, vol := range volumes {
		output[*vol.VolumeId] = vol
	}
	return output
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
