# 🚀 MultiCache: Enterprise-Grade Multi-Level Caching for Go

MultiCache is a high-performance, highly resilient, distributed multi-level caching library for Go. It seamlessly integrates an **In-Memory LRU Cache (L1)** with **Redis (L2)**, while providing advanced features like Pub/Sub synchronization, Circuit Breaking, Singleflight stampede protection, and Snappy binary compression.

Built for microservices that require zero-latency responses and bulletproof database protection.

## ✨ Features

- **🛡️ Multi-Level Architecture:** Millisecond reads from Redis (L2), nanosecond reads from Local RAM (L1).
- **🔄 Distributed Sync:** Uses Redis Pub/Sub to automatically invalidate L1 caches across all instances when data changes.
- **🛑 Cache Stampede Protection:** Built-in `golang.org/x/sync/singleflight` prevents database overload during full cache misses.
- **⚡ Zero-Latency Refresh (Early Expiration):** Refreshes stale data in the background _before_ the absolute TTL expires, meaning users never wait for a DB query.
- **🗜️ Binary Compression:** Uses `snappy` to compress data and a custom 9-byte binary header to reduce Redis and RAM memory footprint by up to 80%.
- **🔌 Redis Circuit Breaker:** Uses `sony/gobreaker`. If Redis crashes, the breaker trips and gracefully falls back to the database, preventing application lockups.
- **🧠 Smart Memory Management:** Strictly limits L1 cache by item count and monitors actual Heap Memory (`runtime.MemStats`) to prevent OOM errors.
- **📊 Observability:** Native integration with Prometheus to track hit rates, misses, and errors.

## 📦 Installation

```bash
go get github.com/afshintalebi/multicache
```

## 🚀 Quick Start

Here is a minimal example to get you started:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yourusername/multicache"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func main() {
	// 1. Initialize Redis Client
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	// 2. Initialize MultiCache Manager
	cache := multicache.New(
		multicache.WithRedis(redisClient),
		multicache.WithL1MaxItems(5000),       // Max 5000 items in RAM
		multicache.WithL1MaxMemoryMB(100),     // Max 100MB RAM usage
	)
	defer cache.Close()

	ctx := context.Background()
	var user User

	// 3. Use GetOrLoad (The Magic Method)
	err := cache.GetOrLoad(ctx, "user:123", 10*time.Minute, &user, func(c context.Context) (interface{}, error) {
		// This block ONLY runs if the data is not in L1 AND not in L2.
		fmt.Println("Fetching from slow database...")
		return User{ID: 123, Name: "John Doe"}, nil
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("User: %s\n", user.Name)
}
```

## ⚙️ Configuration Options

MultiCache uses the Functional Options pattern. You can customize the cache instance using the following options during initialization:

| Option                         | Default             | Description                                                                                   |
| :----------------------------- | :------------------ | :-------------------------------------------------------------------------------------------- |
| `WithRedis(client)`            | **Required**        | Sets the `go-redis` client instance.                                                          |
| `WithL1MaxItems(int)`          | `10000`             | Maximum number of keys held in local L1 memory (LRU).                                         |
| `WithL1MaxMemoryMB(uint64)`    | `100`               | Soft limit for RAM usage. Evicts L1 aggressively if exceeded.                                 |
| `WithKeyPrefix(string)`        | `"cache:"`          | Namespace prefix for all Redis keys to avoid collision.                                       |
| `WithPubSubChannel(string)`    | `"multicache:sync"` | Redis channel used for cross-instance invalidation broadcasts.                                |
| `WithEarlyRefreshRatio(float)` | `0.8`               | Ratio (0.1 to 0.99) defining when a key becomes logically stale. Triggers background refresh. |
| `WithCompression(bool)`        | `false`             | Enables Snappy binary compression. Highly recommended for large JSON arrays.                  |
| `WithCircuitBreaker(bool)`     | `false`             | Enables Redis circuit breaker protection.                                                     |
| `WithCircuitBreakerSettings`   | `3 fails, 5s wait`  | Customizes `MaxFailures` and `Timeout` for the circuit breaker.                               |
| `WithMetrics(bool, prefix)`    | `false`             | Exposes Prometheus metrics (`requests_total` & `errors_total`).                               |

## 🧠 Advanced Use Cases

### 1. Zero-Latency Background Refresh

By default, `earlyRefreshRatio` is set to `0.8`. If you set a TTL of `10 minutes`, the cache becomes "stale" at `minute 8`.
If a user requests the data at `minute 9`, MultiCache instantly returns the stale data (Zero Latency) and fires a detached goroutine to fetch fresh data from the DB and update L1/L2.

### 2. High-Performance Compression

Enable compression for large payloads to save massive amounts of RAM and Network Bandwidth:

```go
cache := multicache.New(
    multicache.WithRedis(redisClient),
    multicache.WithCompression(true), // 🔥 Drops payload size by up to 80%
)
```

### 3. Redis Circuit Breaker

Never let a Redis outage bring down your application.

```go
cache := multicache.New(
    multicache.WithRedis(redisClient),
    multicache.WithCircuitBreaker(true),
    // Trip after 2 real network failures, stay open for 5 seconds
    multicache.WithCircuitBreakerSettings(2, 5*time.Second),
)
```

### 4. Explicit Deletion (Invalidation)

When a user updates their profile in your DB, you must delete the old cache. Calling `Delete` removes it from L2 and broadcasts a Pub/Sub message to drop it from all L1 instances across your cluster:

```go
err := cache.Delete(ctx, "user:123")
```

## 📊 Prometheus Metrics

If `WithMetrics(true, "myapp")` is enabled, MultiCache registers the following vectors:

- `myapp_requests_total{layer="l1/l2/db", status="hit/miss/fetched"}`
- `myapp_errors_total{type="redis/db/internal"}`

You can expose these using `promhttp.Handler()`.

## 🤝 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## 📝 License

[MIT](./LICENSE)
