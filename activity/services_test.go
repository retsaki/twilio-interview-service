package activity

import (
	"context"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestService(t *testing.T) (*Service, func()) {
	t.Helper()
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	svc := NewService(rdb, Config{
		DefaultStatusWindowHours: 2,
		DefaultThreshold:         3,
		CountersTTLHours:         4,
	})
	cleanup := func() {
		rdb.Close()
		s.Close()
	}
	return svc, cleanup
}

func TestRecordAndCount(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()
	ctx := context.Background()
	u := "u1"
	typ := "click"
	now := time.Now().UTC()
	_ = svc.RecordAction(ctx, Action{UserID: u, ActionType: typ, Timestamp: now.Unix()})
	_ = svc.RecordAction(ctx, Action{UserID: u, ActionType: typ, Timestamp: now.Unix()})
	_ = svc.RecordAction(ctx, Action{UserID: u, ActionType: typ, Timestamp: now.Add(-1 * time.Hour).Unix()})
	n, err := svc.CountAction(ctx, u, typ, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2, got %d", n)
	}
	n2, err := svc.CountAction(ctx, u, typ, 2)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 3 {
		t.Fatalf("want 3, got %d", n2)
	}
}

func TestUserStatus(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()
	ctx := context.Background()
	u := "u2"
	_ = svc.RecordAction(ctx, Action{UserID: u, ActionType: "click"})
	_ = svc.RecordAction(ctx, Action{UserID: u, ActionType: "click"})
	_ = svc.RecordAction(ctx, Action{UserID: u, ActionType: "view"})
	_ = svc.RecordAction(ctx, Action{UserID: u, ActionType: "view"})
	status, total, err := svc.UserStatus(ctx, u, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if status != "active" || total != 4 {
		t.Fatalf("want (active,4), got (%s,%d)", status, total)
	}
}

type fixedClock struct{ t time.Time }

func (f fixedClock) Now() time.Time { return f.t }

func newSvcWithMiniAt(t *testing.T, at time.Time, cfg Config) (*Service, *miniredis.Miniredis, func()) {
	t.Helper()
	s, _ := miniredis.Run()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	svc := NewServiceWithClock(rdb, cfg, fixedClock{t: at.UTC()})
	return svc, s, func() { rdb.Close(); s.Close() }
}

func TestRecordAction_validatesInput(t *testing.T) {
	now := time.Now().UTC()
	cfg := Config{DefaultStatusWindowHours: 1, DefaultThreshold: 10, CountersTTLHours: 48}
	svc, _, cleanup := newSvcWithMiniAt(t, now, cfg)
	defer cleanup()
	ctx := context.Background()
	if err := svc.RecordAction(ctx, Action{UserID: "", ActionType: "x"}); err == nil {
		t.Fatalf("expected error")
	}
	if err := svc.RecordAction(ctx, Action{UserID: "u", ActionType: ""}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestCountAction_validatesInput(t *testing.T) {
	now := time.Now().UTC()
	cfg := Config{DefaultStatusWindowHours: 1, DefaultThreshold: 10, CountersTTLHours: 48}
	svc, _, cleanup := newSvcWithMiniAt(t, now, cfg)
	defer cleanup()
	ctx := context.Background()
	if _, err := svc.CountAction(ctx, "", "x", 1); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := svc.CountAction(ctx, "u", "", 1); err == nil {
		t.Fatalf("expected error")
	}
}

func TestRecordAction_setsTTLsAndSets(t *testing.T) {
	now := time.Date(2025, 8, 27, 11, 15, 0, 0, time.UTC)
	cfg := Config{DefaultStatusWindowHours: 2, DefaultThreshold: 3, CountersTTLHours: 4}
	svc, s, cleanup := newSvcWithMiniAt(t, now, cfg)
	defer cleanup()
	ctx := context.Background()
	if err := svc.RecordAction(ctx, Action{UserID: "u1", ActionType: "click"}); err != nil {
		t.Fatal(err)
	}
	bkt := bucketKey("u1", "click", now.Truncate(time.Hour))
	if v, err := s.Get(bkt); err != nil || v != "1" {
		t.Fatalf("bucket not incremented")
	}
	if s.TTL(bkt) <= 0 {
		t.Fatalf("bucket ttl missing")
	}
	members, err := s.SMembers(typesKey("u1"))
	if err != nil {
		t.Fatalf("smembers err: %v", err)
	}
	found := false
	for _, m := range members {
		if m == "click" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("types set missing member")
	}
	if s.TTL(typesKey("u1")) <= 0 {
		t.Fatalf("types ttl missing")
	}
	if s.TTL(zsetKey("u1", "click")) <= 0 {
		t.Fatalf("zset ttl missing")
	}
}

func TestRecordAction_honorsProvidedTimestampHour(t *testing.T) {
	base := time.Date(2025, 8, 27, 12, 0, 0, 0, time.UTC)
	cfg := Config{DefaultStatusWindowHours: 2, DefaultThreshold: 3, CountersTTLHours: 48}
	svc, s, cleanup := newSvcWithMiniAt(t, base, cfg)
	defer cleanup()
	ctx := context.Background()
	tsPast := base.Add(-2 * time.Hour).Unix()
	_ = svc.RecordAction(ctx, Action{UserID: "u2", ActionType: "view", Timestamp: tsPast})
	pastKey := bucketKey("u2", "view", time.Unix(tsPast, 0).UTC().Truncate(time.Hour))
	if v, err := s.Get(pastKey); err != nil || v != "1" {
		t.Fatalf("past hour bucket not incremented")
	}
}

func TestUserStatus_thresholdBoundary(t *testing.T) {
	now := time.Now().UTC()
	cfg := Config{DefaultStatusWindowHours: 1, DefaultThreshold: 10, CountersTTLHours: 48}
	svc, s, cleanup := newSvcWithMiniAt(t, now, cfg)
	defer cleanup()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_ = svc.RecordAction(ctx, Action{UserID: "u3", ActionType: "click"})
	}
	status, total, err := svc.UserStatus(ctx, "u3", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if status != "inactive" || total != 10 {
		t.Fatalf("want inactive at boundary")
	}
	if v, err := s.Get(statusKey("u3")); err != nil || v != "inactive" {
		t.Fatalf("status not stored")
	}
	if s.TTL(statusKey("u3")) <= 0 {
		t.Fatalf("status ttl missing")
	}
}

func TestCountAction_defaultsHoursFromConfig(t *testing.T) {
	now := time.Date(2025, 8, 27, 11, 0, 0, 0, time.UTC)
	cfg := Config{DefaultStatusWindowHours: 3, DefaultThreshold: 3, CountersTTLHours: 48}
	svc, s, cleanup := newSvcWithMiniAt(t, now, cfg)
	defer cleanup()
	ctx := context.Background()
	s.Set(bucketKey("u4", "click", now), "1")
	s.Set(bucketKey("u4", "click", now.Add(-1*time.Hour)), "1")
	n, err := svc.CountAction(ctx, "u4", "click", 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2 got %d", n)
	}
}

func TestRecordAction_concurrentIncrements(t *testing.T) {
	now := time.Date(2025, 8, 27, 14, 0, 0, 0, time.UTC)
	cfg := Config{DefaultStatusWindowHours: 1, DefaultThreshold: 10, CountersTTLHours: 48}
	svc, _, cleanup := newSvcWithMiniAt(t, now, cfg)
	defer cleanup()
	ctx := context.Background()
	var wg sync.WaitGroup
	N := 200
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			_ = svc.RecordAction(ctx, Action{UserID: "cx", ActionType: "tap"})
			wg.Done()
		}()
	}
	wg.Wait()
	n, err := svc.CountAction(ctx, "cx", "tap", 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(N) {
		t.Fatalf("want %d got %d", N, n)
	}
}
