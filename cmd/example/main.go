package main

import (
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strings"
)

func logLevel() slog.Level {
	if os.Getenv("DEBUG") == "1" {
		return slog.LevelDebug
	}

	return slog.LevelInfo
}

func handlerServerCmd() {
	fset := flag.NewFlagSet("server", flag.ExitOnError)
	members := fset.String("members", "", "comma separated list of members")
	port := fset.String("port", "10000", "port to listen on")
	id := fset.Int("id", 0, "id of the replica")

	fset.Parse(os.Args[2:])

	parsedMembers := strings.Split(*members, ",")

	slog.Info("Starting server...", "members", *members, "port", *port, "id", *id)
	newServer(parsedMembers, *port, *id).run()
}

func handleClientCmd() {
	fset := flag.NewFlagSet("client", flag.ExitOnError)
	members := fset.String("members", "", "comma separated list of members")

	fset.Parse(os.Args[2:])

	parsedMembers := strings.Split(*members, ",")
	id := rand.Int()

	fmt.Println("Starting client...", "members", parsedMembers, "id", id)
	newRepl(parsedMembers, id).run()
}

func main() {
	args := os.Args

	logh := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(),
	})
	slog.SetDefault(slog.New(logh))

	if len(args) < 2 {
		fmt.Println("invalid command: only server and client are supported", args)
		return
	}

	if args[1] == "server" {
		handlerServerCmd()
		return
	}

	if args[1] == "client" {
		handleClientCmd()
		return
	}

	fmt.Println("invalid command: only server and client are supported")
}
