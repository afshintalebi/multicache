package multicache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type manager struct {
	cfg      *config
	l1       *l1Cache
	pubsub   *redis.PubSub
	closeCh  chan struct{}
	reqGroup singleflight.Group // Protection against cache stampede
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

func (m *manager) GetOrLoad(ctx context.Context, key string, ttl time.Duration, dest interface{}, fetchFunc FetchFunc) error {
	// 1. Check L1 (In-Memory)
	if data, found := m.l1.Get(key); found {
		return json.Unmarshal(data, dest)
	}

	sfKey := "sf:" + key
	// Prevent multiple identical requests going to Redis/DB simultaneously
	result, err, _ := m.reqGroup.Do(sfKey, func() (interface{}, error) {
		redisKey := m.cfg.prefix + key

		// 2. Check L2 (Redis)
		l2Data, err := m.cfg.redisClient.Get(ctx, redisKey).Bytes()
		if err == nil {
			// Backfill L1
			m.l1.Set(key, l2Data, ttl)
			return l2Data, nil
		}

		// 3. Fallback to Database
		dbData, err := fetchFunc(ctx)
		if err != nil {
			return nil, err
		}

		// Serialize to []byte
		jsonData, err := json.Marshal(dbData)
		if err != nil {
			return nil, err
		}

		// 4. Set in L2 (Redis)
		m.cfg.redisClient.Set(ctx, redisKey, jsonData, ttl)

		// 5. Set in L1 (Memory)
		m.l1.Set(key, jsonData, ttl)

		return jsonData, nil
	})

	if err != nil {
		return err
	}

	return json.Unmarshal(result.([]byte), dest)
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
