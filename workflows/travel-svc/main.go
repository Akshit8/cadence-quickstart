package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/activity"
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

const clientName = "travel-app-service"

var logger *zap.Logger

func init() {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	logger = zap.Must(config.Build())

	// register workflow
	workflow.Register(TravelBookingWorkflow)

	// register activities
	activity.Register(BookHotelActivity)
	activity.Register(BookCarActivity)
	activity.Register(BookAirplaneActivity)
}

func main() {
	serviceClient := buildCadenceClient()
	worker := buildWorker(serviceClient)
	err := worker.Start()
	if err != nil {
		logger.Fatal("Failed to start worker")
	}
	logger.Info("Started Worker.", zap.String("worker", config.TaskListName))

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-signalChan:
			logger.Info("Received shutdown signal")
			worker.Stop()
			logger.Info("Worker stopped")
			return
		}
	}
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
		TaskList:               config.TaskListName,
		ScheduleToCloseTimeout: time.Second * 60,
		ScheduleToStartTimeout: time.Second * 60,
		StartToCloseTimeout:    time.Second * 60,
		HeartbeatTimeout:       time.Second * 10,
		WaitForCancellation:    false,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Execute activities in sequence
	err := workflow.ExecuteActivity(ctx, BookHotelActivity, bd.Name, bd.Email).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("Failed to book hotel", zap.Error(err))
	}

	err = workflow.ExecuteActivity(ctx, BookCarActivity, bd.Name, bd.Email).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("Failed to book car", zap.Error(err))
	}

	err = workflow.ExecuteActivity(ctx, BookAirplaneActivity, bd.Name, bd.Email).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("Failed to book airplane", zap.Error(err))
	}

	// TODO: set result
	workflow.GetLogger(ctx).Info("Done", zap.String("result", "Booking completed"))

	return nil
}

func BookHotelActivity(ctx context.Context, name, email string) error {
	// Simulate hotel booking
	time.Sleep(2 * time.Second)
	logger.Info("Hotel booked successfully", zap.String("name", name), zap.String("email", email))
	return nil
}

func BookCarActivity(ctx context.Context, name, email string) error {
	// Simulate hotel booking
	time.Sleep(2 * time.Second)
	logger.Info("Car booked successfully", zap.String("name", name), zap.String("email", email))
	return nil
}

func BookAirplaneActivity(ctx context.Context, name, email string) error {
	// Simulate hotel booking
	time.Sleep(2 * time.Second)
	logger.Info("Airplane booked successfully", zap.String("name", name), zap.String("email", email))
	return nil
}
