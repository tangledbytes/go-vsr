# go-vsr

go-vsr is the implementation of ViewStamped Replication[^1] in Golang. The goal here is to NOT build a production ready package rather is to simply play around with the protocol. As of now the it implements:
1. Normal Operation
2. View Change
3. State Transfer

Recovery is not yet supported.

[^1]: https://pmg.csail.mit.edu/papers/vr-revisited.pdf

## Simulator
The simulator in the repository is still in works as is the VSR implmentation. The simulator is a deterministic simulator, i.e. for as long as the given seed is the same, you should be able to generate the same scenarios on repeat. At the moment, the simulator is in working condition and can do the following:
1. Take a seed and create a cluster of random numbers of replicas + random number of clients.
2. Simulates packet drops
3. Simulates unordered deliveries
4. Simulates network delays
5. Can do some sanity tests in the middle of the simulation as simulator drives the entire cluster and clients as well as has complete control over the passage of time.
6. Performs a several assertions at the end of the simulation.

### How to run?
Running the simulator is easy, you just need Go installed on the system.
```console
$ go run ./cmd/simulator <seed>
```

If a seed is given then that seed will be used or else a seed will be generated and will be printed.

## Example
There is a simple client (REPL) server example in the repository. It can mimic a distributed in-memory KV store.

### How to run the example?
```console
$ #export DEBUG=1 will enable debug logs
$ go run ./cmd/example server -members "0.0.0.0:10000,0.0.0.0:10001,0.0.0.0:10002" -id 0 -port 10000
```
```console
$ #export DEBUG=1 will enable debug logs
$ go run ./cmd/example server -members "0.0.0.0:10000,0.0.0.0:10001,0.0.0.0:10002" -id 1 -port 10001
```
```console
$ #export DEBUG=1 will enable debug logs
$ go run ./cmd/example server -members "0.0.0.0:10000,0.0.0.0:10001,0.0.0.0:10002" -id 2 -port 10002
```

The above will start 3 replicas on port 10000, 10001, 10002.

```console
$ #export DEBUG=1 will enable debug logs
$ go run ./cmd/example client -members "0.0.0.0:10000,0.0.0.0:10001,0.0.0.0:10002"
Starting client... members [0.0.0.0:10000 0.0.0.0:10001 0.0.0.0:10002] id 5688340127043569018
client=>GET abc
server=> {"result": ""}
client=>SET abc 123
server=> {"result": "123"}
client=>GET abc
server=> {"result": "123"}
client=>
```

The above will start a client which can communicate with the cluster.

## NOTE
This is below alpha level software at the moment and is being actively worked on.
