package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	log "github.com/sirupsen/logrus"
	"github.com/utilitywarehouse/ebs-snapshotter/clients"
	"github.com/utilitywarehouse/ebs-snapshotter/models"
	w "github.com/utilitywarehouse/ebs-snapshotter/watcher"
	"github.com/utilitywarehouse/go-operational/op"
)

const (
	name        = "ebs-snapshotter"
	description = `Snapshots EBS volumes automatically`
)

var (
	gitHash        string
	createdCounter *prometheus.CounterVec
	deletedCounter *prometheus.CounterVec
	errorsTotal    prometheus.Counter
)

func main() {
	app := cli.App(name, description)
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
	logLevelOpt := app.String(cli.StringOpt{
		Name:   "log-level",
		Desc:   "Log level (e.g. INFO, DEBUG, WARN)",
		EnvVar: "LOG_LEVEL",
		Value:  "INFO",
	})
	logFormatOpt := app.String(cli.StringOpt{
		Name:   "log-f",
		Desc:   "Log format, if set to text will use text as logging format, otherwise will use json",
		EnvVar: "LOG_FORMAT",
		Value:  "text",
	})

	createdCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "snapshots_performed",
		Help: "A counter of the total number of snapshots created",
	}, []string{"volumeId"})
	deletedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "old_snapshots_removed",
		Help: "A counter of the total number of old snapshots removed",
	}, []string{"volumeId", "snapshotId"})
	errorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "errors_total",
		Help: "A counter of the total number of errors encountered",
	})

	prometheus.DefaultRegisterer.MustRegister(createdCounter, deletedCounter, errorsTotal)

	configureLogging(logLevelOpt, logFormatOpt)

	app.Action = func() {
		snapshotConfigs := loadVolumeSnapshotConfig(*volumeSnapshotConfigFile)
		ec2Client := createEc2Client(*awsAccessKey, *awsSecretKey, *ec2Region)

		ebsClient := clients.NewEBSClient(ec2Client)

		watcher := w.NewEBSSnapshotWatcher(
			ebsClient,
			createdCounter,
			deletedCounter,
			errorsTotal,
		)

		go initialiseHTTPServer(*httpPort)
		for {
			watcher.WatchSnapshots(snapshotConfigs)
			<-time.After(time.Duration(*pollIntervalSeconds) * time.Second)
		}
	}
	app.Run(os.Args)
}

func createEc2Client(awsAccessKey string, awsSecretKey string, ec2Region string) *ec2.EC2 {
	config := aws.NewConfig()
	config.WithCredentials(credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, "")).
		WithRegion(ec2Region)
	awsSess := session.New(config)
	ec2Client := ec2.New(awsSess)
	return ec2Client
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

func initialiseHTTPServer(port int) {
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
		op.NewStatus(name, description).
			AddOwner("telecom", "#telecom").
			SetRevision(gitHash).
			ReadyUseHealthCheck().
			AddLink("VCS Repo", "https://github.com/utilitywarehouse/ebs-snapshotter"),
	)
}

func configureLogging(logLevel, logFormat *string) {
	switch {
	case *logFormat == "text":
		log.SetFormatter(&log.TextFormatter{})
	default:
		log.SetFormatter(&log.JSONFormatter{})
	}
	level, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("error parsing log level: %v", err)
	}
	log.SetLevel(level)
}
