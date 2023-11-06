# go-vsr

go-vsr is the implementation of ViewStamped Replication[^1] in Golang. The goal here is to NOT build a production ready package rather is to simply play around with the protocol. As of now the it implements:
1. Normal Operation
2. View Change

Recovery is not yet supported.

[^1]: https://pmg.csail.mit.edu/papers/vr-revisited.pdf

There is a simulator in the repository but it is NOT functional at the moment. There are many reasons for that, primarily that there are still quite a few sources of non-determinism in the code (probably biggest is the networking layer).

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

### NOTE
This is below alpha level software at the moment and is being actively worked on.
