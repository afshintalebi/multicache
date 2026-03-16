package multicache

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/golang/snappy"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const binaryHeaderSize = 9 // 8 bytes for int64 + 1 byte for flag

type manager struct {
	cfg      *config
	l1       *l1Cache
	pubsub   *redis.PubSub
	closeCh  chan struct{}
	reqGroup singleflight.Group // Protection against cache stampede
	bgGroup  singleflight.Group // Protects DB during background refresh
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

// GetOrLoad attempts to fetch the key from L1, then L2.
// It uses Singleflight to prevent cache stampedes and Early Expiration for zero-latency refreshes.
func (m *manager) GetOrLoad(ctx context.Context, key string, ttl time.Duration, dest interface{}, fetchFunc FetchFunc) error {
	var rawBytes []byte

	// 1. FAST PATH: Check L1
	if data, found := m.l1.Get(key); found {
		rawBytes = data
	} else {
		// 2. L1 MISS: Singleflight for L2 & DB
		sfKey := "sf:" + key
		result, err, _ := m.reqGroup.Do(sfKey, func() (interface{}, error) {
			redisKey := m.cfg.prefix + key

			// Check L2
			l2Data, err := m.cfg.redisClient.Get(ctx, redisKey).Bytes()
			if err == nil {
				m.l1.Set(key, l2Data, ttl)
				return l2Data, nil
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
		return fmt.Errorf("multicache: invalid cache data corruption")
	}

	// Extract Header Info
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
		// snappy.Decode decompresses the data quickly
		decompressed, err := snappy.Decode(nil, payload)
		if err != nil {
			return fmt.Errorf("multicache: failed to decompress data: %w", err)
		}
		finalJSON = decompressed
	} else {
		finalJSON = payload
	}

	// Unmarshal to user struct
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

// fetchAndStore calls the DB, wraps the data, stores in L1/L2, and returns the wrapped bytes.
func (m *manager) fetchAndStore(ctx context.Context, key string, ttl time.Duration, fetchFunc FetchFunc) ([]byte, error) {
	// 1. Fetch DB
	dbData, err := fetchFunc(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetchFunc failed: %w", err)
	}

	// 2. Marshal to JSON
	userJSON, err := json.Marshal(dbData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal db data: %w", err)
	}

	// ==========================================
	// 3. COMPRESS DATA (If enabled)
	// ==========================================
	payload := userJSON
	isCompressed := false

	if m.cfg.enableCompression {
		// snappy.Encode creates a compressed copy of the JSON
		payload = snappy.Encode(nil, userJSON)
		isCompressed = true
	}

	// 4. Calculate Logical TTL
	staleDuration := time.Duration(float64(ttl) * m.cfg.earlyRefreshRatio)
	staleAfter := uint64(time.Now().Add(staleDuration).Unix())

	// ==========================================
	// 5. BUILD CUSTOM BINARY WRAPPER
	// ==========================================
	// Allocate exact bytes needed: 9 + len(payload)
	finalBytes := make([]byte, binaryHeaderSize+len(payload))

	// Write Timestamp (Bytes 0-7)
	binary.LittleEndian.PutUint64(finalBytes[0:8], staleAfter)

	// Write Compression Flag (Byte 8)
	if isCompressed {
		finalBytes[8] = 1
	} else {
		finalBytes[8] = 0
	}

	// Write Payload (Bytes 9+)
	copy(finalBytes[binaryHeaderSize:], payload)

	// 6. Save to Caches (Both L1 and L2 now store the highly compressed binary!)
	redisKey := m.cfg.prefix + key
	m.cfg.redisClient.Set(ctx, redisKey, finalBytes, ttl)
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
