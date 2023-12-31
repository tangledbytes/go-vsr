package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"

	"github.com/tangledbytes/go-vsr/internal/simulator"
)

func saveHeapProfile() {
	if os.Getenv("HEAP_PROFILE") == "1" {
		f, err := os.Create("heap.pprof")
		if err != nil {
			panic(err)
		}

		if err := pprof.WriteHeapProfile(f); err != nil {
			panic(err)
		}

		if err := f.Close(); err != nil {
			panic(err)
		}
	}
}

func getLogLevel(entity string) slog.Level {
	switch os.Getenv(entity + "_DEBUG") {
	case "1":
		return slog.LevelDebug
	case "0":
		return slog.LevelInfo
	case "-1":
		return slog.Level(1000)
	}

	return slog.LevelInfo
}

func createLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
}

func exitHooks(fns ...func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		<-c
		for _, fn := range fns {
			fn()
		}

		os.Exit(1)
	}()
}

func setupCPUProfile() {
	if os.Getenv("CPU_PROFILE") == "1" {
		f, err := os.Create("cpu.pprof")
		if err != nil {
			panic(err)
		}

		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
	}
}

func stopCPUProfile() {
	if os.Getenv("CPU_PROFILE") == "1" {
		pprof.StopCPUProfile()
	}
}

func main() {
	var seed int64
	if len(os.Args) < 2 {
		seed = rand.Int63()
	} else {
		var err error
		seed, err = strconv.ParseInt(os.Args[1], 10, 64)
		if err != nil {
			fmt.Println("invalid seed")
			return
		}
	}

	setupCPUProfile()
	exitHooks(
		saveHeapProfile,
		stopCPUProfile,
	)

	replicaLogger := createLogger(getLogLevel("REPLICA"))
	clientLogger := createLogger(getLogLevel("CLIENT"))
	simulatorLogger := createLogger(getLogLevel("SIMULATOR"))
	simulator.New(uint64(seed), replicaLogger, clientLogger, simulatorLogger).Simulate()
}
