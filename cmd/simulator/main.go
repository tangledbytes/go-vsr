package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"

	"github.com/tangledbytes/go-vsr/internal/simulator"
)

func storeHeap() {
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

func main() {
	if len(os.Args) < 2 {
		fmt.Println("seed is required")
		return
	}

	seed, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("invalid seed")
		return
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for sig := range c {
			fmt.Println("captured", sig)
			storeHeap()
			os.Exit(1)
		}
	}()

	simulator.New(uint64(seed)).Simulate()
}
