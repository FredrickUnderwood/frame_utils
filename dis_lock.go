package frame_utils

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	ErrLockNotAcquired = errors.New("failed to acquire lock")
	ErrLockNotHeld     = errors.New("lock is not held by this instance")
	ErrLockAlreadyHeld = errors.New("lock is already held by this instance")
	RetryWaitTimeout   = errors.New("timeout waiting for lock to retry")
)

const (
	defaultTTL         = 30 * time.Second
	defaultWaitTimeout = 10 * time.Second
)

type DisLock struct {
	client    *redis.Client
	namespace string
}

func (d *DisLock) buildKey(lockName string) string {
	if d.namespace == "" {
		return "dis_lock:" + lockName
	}
	return d.namespace + ":dis_lock:" + lockName
}

func NewDisLock(client *redis.Client, namespace string) *DisLock {
	return &DisLock{
		client:    client,
		namespace: namespace,
	}
}

type LockOption func(*lockConfig)

type lockConfig struct {
	ttl                 time.Duration
	waitTimeout         time.Duration
	watchDogReleaseTime time.Duration
	watchDog            bool
}

func WithTTL(ttl time.Duration) LockOption {
	return func(config *lockConfig) {
		config.ttl = ttl
	}
}

func WithWaitTimeout(timeout time.Duration) LockOption {
	return func(config *lockConfig) {
		config.waitTimeout = timeout
	}
}

func WithReleaseTime(watchDogReleaseTime time.Duration) LockOption {
	return func(config *lockConfig) {
		config.watchDogReleaseTime = watchDogReleaseTime
	}
}

func WithWatchDog() LockOption {
	return func(config *lockConfig) {
		config.watchDog = true
	}
}

func (d *DisLock) GetDisLock(lockName string, opts ...LockOption) *DisLockInstance {
	option := &lockConfig{
		ttl:                 defaultTTL,
		waitTimeout:         defaultWaitTimeout,
		watchDogReleaseTime: defaultTTL * 3,
		watchDog:            false,
	}
	if len(opts) > 0 {
		for _, opt := range opts {
			opt(option)
		}
	}
	return &DisLockInstance{
		client:              d.client,
		key:                 d.buildKey(lockName),
		randomValue:         generateRandomValue(),
		watchDog:            make(chan struct{}),
		ttl:                 option.ttl,
		waitTimeout:         option.waitTimeout,
		needWatchDog:        option.watchDog,
		mu:                  sync.Mutex{},
		watchDogReleaseTime: option.watchDogReleaseTime,
	}
}

type DisLockInstance struct {
	client              *redis.Client
	key                 string
	randomValue         string
	isLocked            bool
	watchDog            chan struct{}
	ttl                 time.Duration
	waitTimeout         time.Duration
	mu                  sync.Mutex
	needWatchDog        bool
	watching            bool
	watchDogReleaseTime time.Duration
}

func (l *DisLockInstance) tryAcquire(ctx context.Context, key, value string, ttl time.Duration, needWatchDog bool) (bool, error) {
	result, err := l.compareAndSwapNX(ctx)
	if err != nil {
		_, _ = l.compareAndDelete(ctx)
		return false, err
	}
	if !result {
		return false, ErrLockNotAcquired
	}
	l.isLocked = true
	if needWatchDog {

	}
	return true, nil
}

// TryLock try lock with default ttl, fail => give up, have watch dog
func (l *DisLockInstance) TryLock(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tryAcquire(ctx, l.key, l.randomValue, l.ttl, true)
}

// TryLockWithTTL try lock with ttl, fail => give up, no watch dog
func (l *DisLockInstance) TryLockWithTTL(ctx context.Context, ttl time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ttl > 0 {
		l.ttl = ttl
	}
	return l.tryAcquire(ctx, l.key, l.randomValue, l.ttl, false)
}

// Lock try lock with default ttl, fail => retry till waitTimeout, have watch dog
func (l *DisLockInstance) Lock(ctx context.Context, waitTime time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if waitTime > 0 {
		l.waitTimeout = waitTime
	}
	res, err := l.tryAcquire(ctx, l.key, l.randomValue, l.ttl, true)
	if err != nil {
		return false, err
	}
	if res {
		return true, nil
	}
	return l.retry(ctx)
}

// LockWithTTL try lock with ttl, fail => retry till waitTimeout, no watch dog
func (l *DisLockInstance) LockWithTTL(ctx context.Context, waitTime, ttl time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if waitTime > 0 {
		l.waitTimeout = waitTime
	}
	if ttl > 0 {
		l.ttl = ttl
	}
	res, err := l.tryAcquire(ctx, l.key, l.randomValue, l.ttl, false)
	if err != nil {
		return false, err
	}
	if res {
		return true, nil
	}
	return l.retry(ctx)
}

func (l *DisLockInstance) Unlock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	res, err := l.compareAndDelete(ctx)
	if err != nil {
		return err
	}
	if !res {
		return ErrLockNotHeld
	}
	l.isLocked = false
	close(l.watchDog)
	return nil
}

func (l *DisLockInstance) compareAndDelete(ctx context.Context) (bool, error) {
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	res, err := script.Run(ctx, l.client, []string{l.key}, l.randomValue).Result()
	if err != nil {
		return false, err
	}
	if res.(int64) == 0 {
		return false, nil
	}
	return true, nil
}

func (l *DisLockInstance) compareAndSwapNX(ctx context.Context) (bool, error) {
	script := redis.NewScript(`
		local current = redis.call('get', KEYS[1])
		if current == false or current == ARGV[1] then
			return redis.call('setex', KEYS[1], ARGV[3], ARGV[2])
		end
		return 0
	`)
	res, err := script.Run(ctx, l.client, []string{l.key}, l.randomValue, l.randomValue, l.ttl).Result()
	if err != nil {
		return false, err
	}
	if res.(int64) == 0 {
		return false, nil
	}
	return true, nil
}

func (l *DisLockInstance) IsLocked() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isLocked
}

func generateRandomValue() string {
	rand.Seed(time.Now().UnixNano())
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const length = 32

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func (l *DisLockInstance) watch(ctx context.Context) {
	if l.watching {
		return
	}
	l.watching = true
	if !l.isLocked {
		return
	}

	stopTime := int64(math.MaxInt)
	if l.watchDogReleaseTime > l.ttl {
		stopTime = time.Now().Add(l.watchDogReleaseTime).UnixNano()
	}
	timer := time.NewTicker(l.ttl / 3)
	defer timer.Stop()
	for {
		select {
		case <-l.watchDog:
			return

		case <-timer.C:
			if time.Now().UnixNano() > stopTime {
				_ = l.Unlock(ctx)
				return
			}
			cancelCtx, cancel := context.WithTimeout(ctx, l.ttl/3*2)
			res, err := l.client.Expire(cancelCtx, l.key, l.ttl).Result()
			cancel()
			if err != nil || !res {
				return
			}
		}
	}
}

func (l *DisLockInstance) retry(ctx context.Context) (bool, error) {
	endTime := time.Now().Add(l.waitTimeout)
	ctx, cancel := context.WithDeadline(ctx, endTime)
	defer cancel()
	retryInterval := l.waitTimeout / 3
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false, RetryWaitTimeout
		case <-ticker.C:
			res, err := l.tryAcquire(ctx, l.key, l.randomValue, l.ttl, l.needWatchDog)
			if err != nil {
				return false, err
			}
			if res {
				return true, nil
			}
		}
	}
}
