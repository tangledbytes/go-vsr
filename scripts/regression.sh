#!/bin/bash

export CPU_PROFILE=0
export HEAP_PROFILE=0
export REPLICA_DEBUG=-1
export CLIENT_DEBUG=-1
export SIMULATOR_DEBUG=0

cat records/regression_seeds | xargs -n 1 go run ./cmd/simulator