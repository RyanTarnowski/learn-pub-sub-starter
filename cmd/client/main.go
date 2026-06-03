package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	const connectionString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}
	defer conn.Close()

	newChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error opening new channel %v", err)
	}
	fmt.Println("Connection was successful.")

	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error welcoming client: %v", err)
	}

	gameState := gamelogic.NewGameState(name)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+name,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("Error subscribing to pause queue: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gameState.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gameState),
	)
	if err != nil {
		log.Fatalf("Error subscribing to pause queue: %v", err)
	}

	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}
		switch userInput[0] {
		case "spawn":
			fmt.Println("Spawning units")
			err = gameState.CommandSpawn(userInput)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			fmt.Println("Moving Units")
			armyMove, err := gameState.CommandMove(userInput)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(
				newChannel,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+armyMove.Player.Username,
				armyMove,
			)
			if err != nil {
				log.Printf("could not publish: %v", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(armyMove.Units), armyMove.ToLocation)

		case "status":
			fmt.Println("Status")
			gameState.CommandStatus()
		case "help":
			fmt.Println("Help")
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			fmt.Println("Goodbye")
			return
		default:
			fmt.Println("Unknown command")
		}
	}

}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(am)

		if moveOutcome == gamelogic.MoveOutComeSafe || moveOutcome == gamelogic.MoveOutcomeMakeWar {
			return pubsub.Ack
		}

		return pubsub.NackDiscard
	}
}
