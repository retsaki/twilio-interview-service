package activity

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Clock interface{ Now() time.Time }
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type Redis interface {
	Pipeline() redis.Pipeliner
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
}

type Config struct {
	DefaultStatusWindowHours int
	DefaultThreshold         int
	CountersTTLHours         int
}

type Service struct {
	rdb    Redis
	clock  Clock
	config Config
}

func NewService(rdb *redis.Client, cfg Config) *Service {
	return NewServiceWithClock(rdb, cfg, SystemClock{})
}
func NewServiceWithClock(r Redis, cfg Config, clk Clock) *Service {
	if cfg.DefaultStatusWindowHours <= 0 {
		cfg.DefaultStatusWindowHours = 1
	}
	if cfg.DefaultThreshold <= 0 {
		cfg.DefaultThreshold = 10
	}
	if cfg.CountersTTLHours <= 0 {
		cfg.CountersTTLHours = 48
	}
	if clk == nil {
		clk = SystemClock{}
	}
	return &Service{rdb: r, clock: clk, config: cfg}
}

type Action struct {
	UserID     string `json:"userID"`
	ActionType string `json:"actionType"`
	Timestamp  int64  `json:"timestamp,omitempty"`
}

const (
	prefixCounter     = "ua:cnt"
	prefixTypeSet     = "ua:types"
	prefixStatus      = "ua:status"
	prefixSortedTimes = "ua:ts"
)

func bucketKey(userID, typ string, t time.Time) string {
	return fmt.Sprintf("%s:%s:%s:%s", prefixCounter, userID, typ, t.UTC().Format("2006010215"))
}
func typesKey(userID string) string { return fmt.Sprintf("%s:%s", prefixTypeSet, userID) }
func zsetKey(userID, typ string) string {
	return fmt.Sprintf("%s:%s:%s", prefixSortedTimes, userID, typ)
}
func statusKey(userID string) string { return fmt.Sprintf("%s:%s", prefixStatus, userID) }

func (s *Service) RecordAction(ctx context.Context, a Action) error {
	if a.UserID == "" || a.ActionType == "" {
		return fmt.Errorf("missing userID or actionType")
	}
	now := s.clock.Now()
	if a.Timestamp > 0 {
		now = time.Unix(a.Timestamp, 0).UTC()
	}
	hourBucket := bucketKey(a.UserID, a.ActionType, now)
	pipe := s.rdb.Pipeline()
	pipe.Incr(ctx, hourBucket)
	pipe.Expire(ctx, hourBucket, time.Duration(s.config.CountersTTLHours)*time.Hour)
	pipe.SAdd(ctx, typesKey(a.UserID), a.ActionType)
	pipe.Expire(ctx, typesKey(a.UserID), time.Duration(s.config.CountersTTLHours)*time.Hour)
	pipe.ZAdd(ctx, zsetKey(a.UserID, a.ActionType), redis.Z{Score: float64(now.Unix()), Member: now.Unix()})
	pipe.Expire(ctx, zsetKey(a.UserID, a.ActionType), time.Duration(s.config.CountersTTLHours)*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Service) CountAction(ctx context.Context, userID, actionType string, hours int) (int64, error) {
	if userID == "" || actionType == "" {
		return 0, fmt.Errorf("missing userID or actionType")
	}
	if hours <= 0 {
		hours = s.config.DefaultStatusWindowHours
	}
	now := s.clock.Now().Truncate(time.Hour)
	keys := make([]string, 0, hours)
	for i := 0; i < hours; i++ {
		keys = append(keys, bucketKey(userID, actionType, now.Add(-time.Duration(i)*time.Hour)))
	}
	res, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, v := range res {
		if v == nil {
			continue
		}
		switch vv := v.(type) {
		case string:
			if n, err := strconv.ParseInt(vv, 10, 64); err == nil {
				total += n
			}
		case int64:
			total += vv
		}
	}
	return total, nil
}

func (s *Service) UserStatus(ctx context.Context, userID string, hours, threshold int) (string, int64, error) {
	if userID == "" {
		return "", 0, fmt.Errorf("missing userID")
	}
	if hours <= 0 {
		hours = s.config.DefaultStatusWindowHours
	}
	if threshold <= 0 {
		threshold = s.config.DefaultThreshold
	}
	types, err := s.rdb.SMembers(ctx, typesKey(userID)).Result()
	if err != nil {
		return "", 0, err
	}
	var sum int64
	for _, t := range types {
		n, err := s.CountAction(ctx, userID, t, hours)
		if err != nil {
			return "", 0, err
		}
		sum += n
	}
	status := "inactive"
	if sum > int64(threshold) {
		status = "active"
	}
	_ = s.rdb.Set(ctx, statusKey(userID), status, time.Duration(hours)*time.Hour).Err()
	return status, sum, nil
}
