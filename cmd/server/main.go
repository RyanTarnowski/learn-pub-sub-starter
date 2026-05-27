package main

import (
	"fmt"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	const connectionString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connection was successful.")

	newChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error opening new channel %v", err)
	}

	err = pubsub.PublishJSON(newChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})

	if err != nil {
		log.Printf("could not publish: %v", err)
	}

	fmt.Println("Pause message sent!")

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	sig := <-signalChan

	fmt.Printf("\nReceived signal: %s. Exiting...\n", sig)
}
