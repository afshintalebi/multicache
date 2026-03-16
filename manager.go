package multicache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// cacheWrapper wraps the user's data with a logical expiration time.
type cacheWrapper struct {
	StaleAfter int64           `json:"stale_after"` // Unix timestamp for logical expiration
	Data       json.RawMessage `json:"data"`        // json.RawMessage avoids double unmarshaling
}

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
	var wrappedBytes []byte
	
	// ==========================================
	// 1. FAST PATH: Check L1 (In-Memory)
	// ==========================================
	if data, found := m.l1.Get(key); found {
		wrappedBytes = data
	} else {
		// ==========================================
		// 2. L1 MISS: Use Singleflight for L2 & DB
		// Prevent multiple identical requests going to Redis/DB simultaneously
		// ==========================================
		sfKey := "sf:" + key
		result, err, _ := m.reqGroup.Do(sfKey, func() (interface{}, error) {
			redisKey := m.cfg.prefix + key

			// Check L2 (Redis)
			l2Data, err := m.cfg.redisClient.Get(ctx, redisKey).Bytes()
			if err == nil {
				// Found in L2! Backfill L1 and return the wrapped bytes
				m.l1.Set(key, l2Data, ttl)
				return l2Data, nil
			} else if err != redis.Nil {
				// Log Redis errors in production, but proceed to fallback
				log.Printf("multicache: L2 read error for %s: %v", key, err)
			}

			// Full Cache Miss (Not in L1, Not in L2): Fallback to Database
			return m.fetchAndStore(ctx, key, ttl, fetchFunc)
		})

		if err != nil {
			return err
		}
		
		wrappedBytes = result.([]byte)
	}

	// ==========================================
	// 3. PROCESS DATA & EARLY EXPIRATION
	// ==========================================
	var wrapper cacheWrapper
	if err := json.Unmarshal(wrappedBytes, &wrapper); err != nil {
		return fmt.Errorf("multicache: failed to unmarshal wrapper: %w", err)
	}

	// Check if data is stale (Logical TTL has passed)
	// Note: If data was just fetched freshly from DB, this will safely be false.
	if time.Now().Unix() > wrapper.StaleAfter {
		// Trigger Background Refresh without blocking the user!
		go m.backgroundRefresh(key, ttl, fetchFunc)
	}

	// Unmarshal the actual user data into the destination pointer (Zero-Latency Return)
	return json.Unmarshal(wrapper.Data, dest)
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
	// 1. Fetch from Database
	dbData, err := fetchFunc(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetchFunc failed: %w", err)
	}

	// 2. Marshal user data
	userBytes, err := json.Marshal(dbData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal db data: %w", err)
	}

	// 3. Calculate Stale Time (Logical TTL)
	staleDuration := time.Duration(float64(ttl) * m.cfg.earlyRefreshRatio)
	staleAfter := time.Now().Add(staleDuration).Unix()

	// 4. Create Wrapper and Marshal
	wrapper := cacheWrapper{
		StaleAfter: staleAfter,
		Data:       userBytes, // json.RawMessage handles this without escaping quotes
	}
	finalBytes, err := json.Marshal(wrapper)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal wrapper: %w", err)
	}

	// 5. Save to L2 (Redis)
	redisKey := m.cfg.prefix + key
	if err := m.cfg.redisClient.Set(ctx, redisKey, finalBytes, ttl).Err(); err != nil {
		log.Printf("multicache: failed to save to L2 for %s: %v", key, err)
	}

	// 6. Save to L1 (Memory)
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
