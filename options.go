package multicache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// FetchFunc loads data from the database if missing in caches
type FetchFunc func(ctx context.Context) (interface{}, error)

// MultiCache is the core interface
type MultiCache interface {
	GetOrLoad(ctx context.Context, key string, ttl time.Duration, dest interface{}, fetchFunc FetchFunc) error
	Delete(ctx context.Context, key string) error
	Close() error
}

type config struct {
	redisClient   *redis.Client
	pubSubChannel string
	l1MaxItems    int
	l1MaxMemoryMB uint64
	prefix        string
}

type Option func(*config)

func WithRedis(client *redis.Client) Option   { return func(c *config) { c.redisClient = client } }
func WithPubSubChannel(channel string) Option { return func(c *config) { c.pubSubChannel = channel } }
func WithL1MaxItems(maxItems int) Option      { return func(c *config) { c.l1MaxItems = maxItems } }
func WithL1MaxMemoryMB(mb uint64) Option      { return func(c *config) { c.l1MaxMemoryMB = mb } }
func WithKeyPrefix(prefix string) Option      { return func(c *config) { c.prefix = prefix } }

func defaultConfig() *config {
	return &config{
		pubSubChannel: "multicache:sync",
		l1MaxItems:    10000,
		l1MaxMemoryMB: 100,
		prefix:        "go:multicache:",
	}
}
