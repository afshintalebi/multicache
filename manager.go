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
				[]string{"layer", "status"}, // layer: l1, l2, db | status: hit, miss, fetched
			),
			Errors: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: m.cfg.metricsPrefix + "_errors_total",
					Help: "Total number of errors partitioned by type",
				},
				[]string{"type"}, // type: redis, db, internal
			),
		}
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

	// ==========================================
	// 1. FAST PATH: Check L1
	// ==========================================
	if data, found := m.l1.Get(key); found {
		rawBytes = data
		m.incReq("l1", "hit") // METRIC: L1 Hit
	} else {
		// ==========================================
		// 2. L1 MISS: Singleflight for L2 & DB
		// ==========================================
		m.incReq("l1", "miss") // METRIC: L1 Miss

		sfKey := "sf:" + key
		result, err, _ := m.reqGroup.Do(sfKey, func() (interface{}, error) {
			redisKey := m.cfg.prefix + key

			// Check L2
			l2Data, err := m.cfg.redisClient.Get(ctx, redisKey).Bytes()
			if err == nil {
				m.l1.Set(key, l2Data, ttl)
				m.incReq("l2", "hit") // METRIC: L2 Hit
				return l2Data, nil
			} else if err != redis.Nil {
				m.incErr("redis") // METRIC: Redis Error
				log.Printf("multicache: L2 read error for %s: %v", key, err)
			} else {
				m.incReq("l2", "miss") // METRIC: L2 Miss
			}

			// DB Fallback
			return m.fetchAndStore(ctx, key, ttl, fetchFunc)
		})

		if err != nil {
			return err
		}
		rawBytes = result.([]byte)
	}

	// ==========================================
	// 3. DECODE BINARY FORMAT & DECOMPRESS
	// ==========================================
	if len(rawBytes) < binaryHeaderSize {
		m.incErr("internal_corruption")
		return fmt.Errorf("multicache: invalid cache data corruption")
	}

	staleAfter := int64(binary.LittleEndian.Uint64(rawBytes[0:8]))
	isCompressed := rawBytes[8] == 1
	payload := rawBytes[binaryHeaderSize:]

	// Trigger Background Refresh if stale
	if time.Now().Unix() > staleAfter {
		go m.backgroundRefresh(key, ttl, fetchFunc)
	}

	// Decompress if needed
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

	// Prevent multiple background refreshes for the same key at the same time
	m.bgGroup.Do(bgKey, func() (interface{}, error) {

		// CRITICAL: We cannot use the user's `ctx` here.
		// If the user's HTTP request finishes, their `ctx` gets canceled.
		// We create a new detached context with a timeout for the background task.
		bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := m.fetchAndStore(bgCtx, key, ttl, fetchFunc)
		if err != nil {
			log.Printf("multicache: background refresh failed for key %s: %v", key, err)
			return nil, err
		}

		// Broadcast invalidation so other instances drop their L1 stale data
		// and load the newly refreshed L2 data on their next request.
		msg := invalidationMessage{Key: key}
		payload, _ := json.Marshal(msg)
		m.cfg.redisClient.Publish(bgCtx, m.cfg.pubSubChannel, payload)

		return nil, nil
	})
}

// fetchAndStore calls the DB, wraps the data, stores in L1/L2.
func (m *manager) fetchAndStore(ctx context.Context, key string, ttl time.Duration, fetchFunc FetchFunc) ([]byte, error) {
	// 1. Fetch DB
	dbData, err := fetchFunc(ctx)
	if err != nil {
		m.incErr("db") // METRIC: DB Error
		return nil, fmt.Errorf("fetchFunc failed: %w", err)
	}
	m.incReq("db", "fetched") // METRIC: DB Fetched (Cache Miss resolved)

	// 2. Marshal to JSON
	userJSON, err := json.Marshal(dbData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal db data: %w", err)
	}

	// 3. Compress
	payload := userJSON
	isCompressed := false
	if m.cfg.enableMetrics {
		// Just a structural condition, actual compression is based on cfg.enableCompression
	}
	if m.cfg.enableCompression {
		payload = snappy.Encode(nil, userJSON)
		isCompressed = true
	}

	// 4. Calculate Logical TTL
	staleDuration := time.Duration(float64(ttl) * m.cfg.earlyRefreshRatio)
	staleAfter := uint64(time.Now().Add(staleDuration).Unix())

	// 5. Build Binary Header
	finalBytes := make([]byte, binaryHeaderSize+len(payload))
	binary.LittleEndian.PutUint64(finalBytes[0:8], staleAfter)
	if isCompressed {
		finalBytes[8] = 1
	} else {
		finalBytes[8] = 0
	}
	copy(finalBytes[binaryHeaderSize:], payload)

	// 6. Save to Caches
	redisKey := m.cfg.prefix + key
	if err := m.cfg.redisClient.Set(ctx, redisKey, finalBytes, ttl).Err(); err != nil {
		m.incErr("redis_write") // METRIC: Redis Write Error
	}
	m.l1.Set(key, finalBytes, ttl)

	return finalBytes, nil
}

func (m *manager) Delete(ctx context.Context, key string) error {
	redisKey := m.cfg.prefix + key

	// Remove from L2
	m.cfg.redisClient.Del(ctx, redisKey)

	// Remove from Local L1
	m.l1.Delete(key)

	// Broadcast to cluster to remove from their L1
	msg := invalidationMessage{Key: key}
	payload, _ := json.Marshal(msg)
	m.cfg.redisClient.Publish(ctx, m.cfg.pubSubChannel, payload)

	return nil
}

func (m *manager) Close() error {
	close(m.closeCh)
	return nil
}
