package main

import (
	"encoding/json"
	"time"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"

	"github.com/akshit8/cadence-quickstart/api"
	"github.com/akshit8/cadence-quickstart/config"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const clientName = "Travel-App-Svc"

var logger *zap.Logger

func init() {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	logger = zap.Must(config.Build())

	// register workflow
	workflow.Register(TravelBookingWorkflow)
}

func main() {
	serviceClient := buildCadenceClient()
	worker := buildWorker(serviceClient)
	err := worker.Start()
	if err != nil {
		logger.Fatal("Failed to start worker")
	}
	logger.Info("Started Worker.", zap.String("worker", config.TaskListName))
}

func buildCadenceClient() workflowserviceclient.Interface {
	ch, err := tchannel.NewChannelTransport(tchannel.ServiceName(clientName))
	if err != nil {
		logger.Fatal("Failed to setup tchannel")
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: clientName,
		Outbounds: yarpc.Outbounds{
			config.CadenceService: {Unary: ch.NewSingleOutbound(config.HostPort)},
		},
	})
	if err := dispatcher.Start(); err != nil {
		logger.Fatal("Failed to start dispatcher")
	}

	return workflowserviceclient.New(dispatcher.ClientConfig(config.CadenceService))
}

func buildWorker(service workflowserviceclient.Interface) worker.Worker {
	workerOptions := worker.Options{
		Logger:       logger,
		MetricsScope: tally.NewTestScope(config.TaskListName, map[string]string{}),
	}
	return worker.New(
		service,
		config.Domain,
		config.TaskListName,
		workerOptions,
	)
}

func TravelBookingWorkflow(ctx workflow.Context, input []byte) error {
	var bd api.BookingDetails
	if err := json.Unmarshal(input, &bd); err != nil {
		workflow.GetLogger(ctx).Error("Failed to unmarshal input", zap.Error(err))
		return err
	}

	workflow.GetLogger(ctx).Info("Received booking details", zap.String("name", bd.Name), zap.String("email", bd.Email))

	ao := workflow.ActivityOptions{
		TaskList:               "sampleTaskList",
		ScheduleToCloseTimeout: time.Second * 60,
		ScheduleToStartTimeout: time.Second * 60,
		StartToCloseTimeout:    time.Second * 60,
		HeartbeatTimeout:       time.Second * 10,
		WaitForCancellation:    false,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// TODO: set result
	workflow.GetLogger(ctx).Info("Done", zap.String("result", ""))

	return nil
}
