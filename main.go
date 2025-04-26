package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/akshit8/cadence-quickstart/api"
	"github.com/akshit8/cadence-quickstart/config"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const clientName = "Travel-App-Frontend"

var logger *zap.Logger

func init() {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	logger = zap.Must(config.Build())
}

func main() {
	serviceClient := buildCadenceClient()

	cdClient := client.NewClient(serviceClient, config.Domain, &client.Options{MetricsScope: tally.NewTestScope(config.TaskListName, map[string]string{})})

	input, err := json.Marshal(api.BookingDetails{
		Name:  "Akshit",
		Email: "akshit@cadence.io",
	})
	if err != nil {
		logger.Fatal("Failed to marshal input", zap.Error(err))
	}

	cdClient.StartWorkflow(context.Background(), client.StartWorkflowOptions{
		TaskList:                     config.TaskListName,
		ExecutionStartToCloseTimeout: 10 * time.Second,
	}, "TravelBookingWorkflow", input)
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
