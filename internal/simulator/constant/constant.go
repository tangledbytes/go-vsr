package constant

import "github.com/tangledbytes/go-vsr/pkg/time"

const PACKET_DROP_PERCENT = 0.1
const PACKET_DUPS_PERCENT = 0.01
const UNORDERED_PACKET_DELIVERY_PERCENT = 0.01
const MIN_PACKET_DELAY = 1 * time.MILLISECOND
const MAX_PACKET_DELAY = 30 * time.MILLISECOND
const MAX_PACKET_INQUEUE = 1 << 16

const MIN_REPLICAS = 1
const MAX_REPLICAS = 9
const MAX_REPLICA_MULTIPLIER = 1
const MIN_CLIENTS = 1
const MAX_CLIENTS = 100
const MAX_CLIENT_MULTIPLIER = 1
const MIN_REQUESTS = 1e3
const MAX_REQUESTS = 1e4
