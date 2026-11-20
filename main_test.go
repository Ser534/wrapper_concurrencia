package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func setupTestDB(t *testing.T) string {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_concurrency.db")

	db, err := initDBTest(dbPath)
	if err != nil {
		t.Fatalf("Failed to initialize test DB: %v", err)
	}
	db.Close()

	return dbPath
}

func initDBTest(dbPath string) (*sql.DB, error) {
	connStr := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS outbox_tasks (
			id VARCHAR(64) PRIMARY KEY,
			payload TEXT NOT NULL,
			status VARCHAR(32) NOT NULL DEFAULT 'pending',
			retry_count INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_tasks(status);
	`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func TestShardedCacheConcurrency(t *testing.T) {
	cache := NewShardedCache(16)
	var wg sync.WaitGroup

	numGoroutines := 100
	opsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", routineID, j)
				val := fmt.Sprintf("val_%d", j)
				cache.Set(key, val, 1*time.Minute)

				got, found := cache.Get(key)
				if !found || got != val {
					t.Errorf("Cache mismatch for key %s: expected %s, got %s (found: %v)", key, val, got, found)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestWorkerPoolExecution(t *testing.T) {
	dbPath := setupTestDB(t)
	db, err := initDB(dbPath)
	if err != nil {
		t.Fatalf("Error opening test DB: %v", err)
	}
	defer db.Close()

	cache := NewShardedCache(8)
	wp := NewWorkerPool(4, 16, 100, cache, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp.Start(ctx)

	numJobs := 50
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("test_job_%d", i)
		if err := insertOutboxTask(db, jobID, "payload_test"); err != nil {
			t.Fatalf("Failed inserting task: %v", err)
		}
		if !wp.Submit(Job{ID: jobID, Payload: "payload_test", CreatedAt: time.Now()}) {
			t.Fatalf("Worker pool rejected job %d", i)
		}
	}

	// Wait for workers to process jobs
	time.Sleep(1 * time.Second)

	pending, completed, err := getOutboxMetrics(db)
	if err != nil {
		t.Fatalf("Error getting outbox metrics: %v", err)
	}

	if completed == 0 {
		t.Errorf("Expected completed jobs > 0, got pending=%d, completed=%d", pending, completed)
	}

	wp.Stop()
}
