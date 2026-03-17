package multicache

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
	"golang.org/x/sync/singleflight"
)

const binaryHeaderSize = 9 // 8 bytes for int64 + 1 byte for flag

// cacheMetrics holds the Prometheus collectors
type cacheMetrics struct {
	Requests *prometheus.CounterVec
	Errors   *prometheus.CounterVec
}

type manager struct {
	cfg      *config
	l1       *l1Cache
	pubsub   *redis.PubSub
	closeCh  chan struct{}
	reqGroup singleflight.Group // Protection against cache stampede
	bgGroup  singleflight.Group // Protects DB during background refresh
	metrics  *cacheMetrics
	cb       *gobreaker.CircuitBreaker
}

type invalidationMessage struct {
	Key string `json:"key"`
}

func New(opts ...Option) MultiCache {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.redisClient == nil {
		panic("multicache: redis client is required")
	}

	m := &manager{
		cfg:     cfg,
		l1:      newL1Cache(cfg.l1MaxItems, cfg.l1MaxMemoryMB),
		closeCh: make(chan struct{}),
	}

	// Initialize Prometheus Metrics
	if m.cfg.enableMetrics {
		m.metrics = &cacheMetrics{
			Requests: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: m.cfg.metricsPrefix + "_requests_total",
					Help: "Total number of cache requests partitioned by layer and status",
				},
				[]string{"layer", "status"},
			),
			Errors: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: m.cfg.metricsPrefix + "_errors_total",
					Help: "Total number of errors partitioned by type",
				},
				[]string{"type"},
			),
		}
	}

	// Initialize Circuit Breaker with dynamic configuration
	if m.cfg.enableCircuitBreaker {
		st := gobreaker.Settings{
			Name:        "RedisCircuitBreaker",
			MaxRequests: 1,               // Number of requests allowed in half-open state
			Interval:    0,               // Never clear failure counts based on time
			Timeout:     m.cfg.cbTimeout, // Dynamic timeout from config
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				// Dynamic max failures from config
				return counts.ConsecutiveFailures >= m.cfg.cbMaxFailures
			},
			OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
				log.Printf("🔌 Circuit Breaker '%s' State Changed: %s -> %s", name, from.String(), to.String())
			},
		}
		m.cb = gobreaker.NewCircuitBreaker(st)
	}

	m.startPubSubListener()
	return m
}

func (m *manager) startPubSubListener() {
	m.pubsub = m.cfg.redisClient.Subscribe(context.Background(), m.cfg.pubSubChannel)

	go func() {
		ch := m.pubsub.Channel()
		for {
			select {
			case msg := <-ch:
				var invMsg invalidationMessage
				if err := json.Unmarshal([]byte(msg.Payload), &invMsg); err == nil {
					// Clear L1 cache when other instances update a key
					m.l1.Delete(invMsg.Key)
				}
			case <-m.closeCh:
				m.pubsub.Close()
				return
			}
		}
	}()
}

// --- Internal Helper Functions for Metrics ---
func (m *manager) incReq(layer, status string) {
	if m.cfg.enableMetrics && m.metrics != nil {
		m.metrics.Requests.WithLabelValues(layer, status).Inc()
	}
}

func (m *manager) incErr(errType string) {
	if m.cfg.enableMetrics && m.metrics != nil {
		m.metrics.Errors.WithLabelValues(errType).Inc()
	}
}

// -------------------------------------------

// GetOrLoad attempts to fetch the key from L1, then L2, then DB.
func (m *manager) GetOrLoad(ctx context.Context, key string, ttl time.Duration, dest interface{}, fetchFunc FetchFunc) error {
	var rawBytes []byte

	// 1. FAST PATH: Check L1
	if data, found := m.l1.Get(key); found {
		rawBytes = data
		m.incReq("l1", "hit")
	} else {
		// 2. L1 MISS: Singleflight for L2 & DB
		m.incReq("l1", "miss")
		sfKey := "sf:" + key

		result, err, _ := m.reqGroup.Do(sfKey, func() (interface{}, error) {
			redisKey := m.cfg.prefix + key
			var l2Data []byte
			var l2Err error

			// Safe L2 Fetch with Circuit Breaker
			if m.cb != nil {
				res, cbErr := m.cb.Execute(func() (interface{}, error) {
					bytes, rErr := m.cfg.redisClient.Get(ctx, redisKey).Bytes()
					if rErr == redis.Nil {
						// Cache miss is NOT a failure.
						return nil, nil
					}
					if rErr != nil {
						// Actual network/redis error. Trip the breaker!
						return nil, rErr
					}
					return bytes, nil
				})

				if cbErr != nil {
					l2Err = cbErr // Breaker is OPEN or Redis Failed
				} else {
					if res == nil {
						l2Err = redis.Nil
					} else {
						l2Data = res.([]byte)
						l2Err = nil
					}
				}
			} else {
				l2Data, l2Err = m.cfg.redisClient.Get(ctx, redisKey).Bytes()
			}

			if l2Err == nil {
				m.l1.Set(key, l2Data, ttl)
				m.incReq("l2", "hit")
				return l2Data, nil
			} else if l2Err != redis.Nil {
				// Breaker Open or Redis Failed. Fallback gracefully to DB.
				m.incErr("redis")
			} else {
				m.incReq("l2", "miss")
			}

			// DB Fallback
			return m.fetchAndStore(ctx, key, ttl, fetchFunc)
		})

		if err != nil {
			return err
		}
		rawBytes = result.([]byte)
	}

	// 3. DECODE BINARY FORMAT & DECOMPRESS
	if len(rawBytes) < binaryHeaderSize {
		m.incErr("internal_corruption")
		return fmt.Errorf("multicache: invalid cache data corruption")
	}

	staleAfter := int64(binary.LittleEndian.Uint64(rawBytes[0:8]))
	isCompressed := rawBytes[8] == 1
	payload := rawBytes[binaryHeaderSize:]

	// Trigger Background Refresh
	if time.Now().Unix() > staleAfter {
		go m.backgroundRefresh(key, ttl, fetchFunc)
	}

	// Decompress
	var finalJSON []byte
	if isCompressed {
		decompressed, err := snappy.Decode(nil, payload)
		if err != nil {
			m.incErr("decompression")
			return fmt.Errorf("multicache: failed to decompress data: %w", err)
		}
		finalJSON = decompressed
	} else {
		finalJSON = payload
	}

	return json.Unmarshal(finalJSON, dest)
}

// backgroundRefresh runs in a separate goroutine to update stale data.
func (m *manager) backgroundRefresh(key string, ttl time.Duration, fetchFunc FetchFunc) {
	bgKey := "bg:" + key
	m.bgGroup.Do(bgKey, func() (interface{}, error) {
		bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := m.fetchAndStore(bgCtx, key, ttl, fetchFunc)
		if err != nil {
			return nil, err
		}

		// Broadcast invalidation via Circuit Breaker
		msg := invalidationMessage{Key: key}
		payload, _ := json.Marshal(msg)

		if m.cb != nil {
			_, _ = m.cb.Execute(func() (interface{}, error) {
				return nil, m.cfg.redisClient.Publish(bgCtx, m.cfg.pubSubChannel, payload).Err()
			})
		} else {
			m.cfg.redisClient.Publish(bgCtx, m.cfg.pubSubChannel, payload)
		}

		return nil, nil
	})
}

// fetchAndStore calls the DB, wraps the data, stores in L1/L2.
func (m *manager) fetchAndStore(ctx context.Context, key string, ttl time.Duration, fetchFunc FetchFunc) ([]byte, error) {
	dbData, err := fetchFunc(ctx)
	if err != nil {
		m.incErr("db")
		return nil, fmt.Errorf("fetchFunc failed: %w", err)
	}
	m.incReq("db", "fetched")

	userJSON, err := json.Marshal(dbData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal db data: %w", err)
	}

	payload := userJSON
	isCompressed := false
	if m.cfg.enableCompression {
		payload = snappy.Encode(nil, userJSON)
		isCompressed = true
	}

	staleDuration := time.Duration(float64(ttl) * m.cfg.earlyRefreshRatio)
	staleAfter := uint64(time.Now().Add(staleDuration).Unix())

	finalBytes := make([]byte, binaryHeaderSize+len(payload))
	binary.LittleEndian.PutUint64(finalBytes[0:8], staleAfter)
	if isCompressed {
		finalBytes[8] = 1
	} else {
		finalBytes[8] = 0
	}
	copy(finalBytes[binaryHeaderSize:], payload)

	// Save to L2 via Circuit Breaker
	redisKey := m.cfg.prefix + key
	if m.cb != nil {
		_, _ = m.cb.Execute(func() (interface{}, error) {
			return nil, m.cfg.redisClient.Set(ctx, redisKey, finalBytes, ttl).Err()
		})
	} else {
		m.cfg.redisClient.Set(ctx, redisKey, finalBytes, ttl)
	}

	m.l1.Set(key, finalBytes, ttl)
	return finalBytes, nil
}

func (m *manager) Delete(ctx context.Context, key string) error {
	redisKey := m.cfg.prefix + key
	m.l1.Delete(key)

	msg := invalidationMessage{Key: key}
	payload, _ := json.Marshal(msg)

	// Perform L2 delete and PubSub broadcast via Circuit Breaker
	if m.cb != nil {
		_, err := m.cb.Execute(func() (interface{}, error) {
			pipe := m.cfg.redisClient.TxPipeline()
			pipe.Del(ctx, redisKey)
			pipe.Publish(ctx, m.cfg.pubSubChannel, payload)
			_, pErr := pipe.Exec(ctx)
			return nil, pErr
		})
		return err
	}

	pipe := m.cfg.redisClient.TxPipeline()
	pipe.Del(ctx, redisKey)
	pipe.Publish(ctx, m.cfg.pubSubChannel, payload)
	_, err := pipe.Exec(ctx)
	return err
}

func (m *manager) Close() error {
	close(m.closeCh)
	return nil
}
