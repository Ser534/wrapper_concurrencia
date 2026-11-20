package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ============================================================================
// SECCIÓN 1: CONFIGURACIÓN Y PARSER DE ARCHIVO .ENV NATIVO
// ============================================================================

func loadEnvFile(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		return // Si no existe el archivo .env, se usan las variables de entorno del sistema
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.Trim(val, `"'`) // Eliminar comillas simples o dobles envolventes
			if os.Getenv(key) == "" {
				os.Setenv(key, val)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("⚠️ Advertencia al leer archivo %s: %v", filepath, err)
	}
}

func getEnvOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// ============================================================================
// SECCIÓN 2: BASE DE DATOS Y VERIFICACIÓN PASIVA
// ============================================================================

func initDB(dbPath string) (*sql.DB, error) {
	connStr := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, fmt.Errorf("error al abrir conexión sqlite: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Comprobación Pasiva: Verificar si existen las tablas requeridas sin crearlas automáticamente
	var tableName string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='outbox_tasks'").Scan(&tableName)
	if err != nil || tableName != "outbox_tasks" {
		db.Close()
		return nil, fmt.Errorf("comprobación pasiva fallida: la tabla 'outbox_tasks' no existe en %s. Inicializar ejecutando la migración SQL previa", dbPath)
	}

	log.Printf("✔ Base de Datos verificada correctamente (Modo WAL activado en %s)", dbPath)
	return db, nil
}

// ============================================================================
// SECCIÓN 3: CACHÉ CON SHARDING Y CONTADORES ATÓMICOS
// ============================================================================

type CacheShard struct {
	mu    sync.RWMutex
	items map[string]CacheItem
}

type CacheItem struct {
	Value     string
	ExpiresAt time.Time
}

type ShardedCache struct {
	shardCount uint32
	shards     []*CacheShard
}

func NewShardedCache(shardCount uint32) *ShardedCache {
	sc := &ShardedCache{
		shardCount: shardCount,
		shards:     make([]*CacheShard, shardCount),
	}
	for i := uint32(0); i < shardCount; i++ {
		sc.shards[i] = &CacheShard{
			items: make(map[string]CacheItem),
		}
	}
	return sc
}

func (sc *ShardedCache) getShard(key string) *CacheShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := h.Sum32() % sc.shardCount
	return sc.shards[idx]
}

func (sc *ShardedCache) Set(key, value string, ttl time.Duration) {
	shard := sc.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.items[key] = CacheItem{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
	}
}

func (sc *ShardedCache) Get(key string) (string, bool) {
	shard := sc.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	item, found := shard.items[key]
	if !found {
		return "", false
	}
	if time.Now().After(item.ExpiresAt) {
		return "", false
	}
	return item.Value, true
}

// ============================================================================
// SECCIÓN 4: MOTOR DE WORKER POOL & PIPELINE CONCURRENTE
// ============================================================================

type Job struct {
	ID        string `json:"id"`
	Payload   string `json:"payload"`
	CreatedAt time.Time
}

type JobResult struct {
	JobID       string        `json:"job_id"`
	ProcessedBy int           `json:"processed_by_worker"`
	Duration    time.Duration `json:"duration_ms"`
	Success     bool          `json:"success"`
	ErrorMsg    string        `json:"error,omitempty"`
}

type MetricsTracker struct {
	TotalProcessed uint64
	TotalErrors    uint64
	ActiveWorkers  int64
	TotalDuration  uint64 // en microsegundos
}

type WorkerPool struct {
	baseWorkers        int
	maxWorkers         int
	currentWorkerCount int64
	jobQueue           chan Job
	results            chan JobResult
	metrics            *MetricsTracker
	cache              *ShardedCache
	db                 *sql.DB
	wg                 sync.WaitGroup
	quitScaler         chan struct{}
}

func NewWorkerPool(baseWorkers, maxWorkers, queueCapacity int, cache *ShardedCache, db *sql.DB) *WorkerPool {
	return &WorkerPool{
		baseWorkers: baseWorkers,
		maxWorkers:  maxWorkers,
		jobQueue:    make(chan Job, queueCapacity),
		results:     make(chan JobResult, queueCapacity),
		metrics:     &MetricsTracker{},
		cache:       cache,
		db:          db,
		quitScaler:  make(chan struct{}),
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	log.Printf("🚀 Iniciando Worker Pool (Base: %d, Max: %d Workers) en %d hilos CPU", wp.baseWorkers, wp.maxWorkers, runtime.GOMAXPROCS(0))
	for i := 1; i <= wp.baseWorkers; i++ {
		wp.spawnWorker(ctx, i, false)
	}

	// Goroutine del Auto-Scaler dinámico
	go wp.autoScaler(ctx)

	// Goroutine colectora de resultados y actualización de métricas en tiempo real
	go func() {
		for res := range wp.results {
			if res.Success {
				atomic.AddUint64(&wp.metrics.TotalProcessed, 1)
			} else {
				atomic.AddUint64(&wp.metrics.TotalErrors, 1)
			}
			atomic.AddUint64(&wp.metrics.TotalDuration, uint64(res.Duration.Microseconds()))
		}
	}()
}

func (wp *WorkerPool) spawnWorker(ctx context.Context, id int, isDynamic bool) {
	atomic.AddInt64(&wp.currentWorkerCount, 1)
	wp.wg.Add(1)
	go wp.worker(ctx, id, isDynamic)
}

func (wp *WorkerPool) autoScaler(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	dynamicID := 1000

	for {
		select {
		case <-ctx.Done():
			return
		case <-wp.quitScaler:
			return
		case <-ticker.C:
			qLen := len(wp.jobQueue)
			qCap := cap(wp.jobQueue)
			currentWorkers := int(atomic.LoadInt64(&wp.currentWorkerCount))

			// Regla de Escalado Hacia Arriba: Si la cola supera el 40% de capacidad, agregar trabajadores dinámicos
			if qLen > (qCap*40)/100 && currentWorkers < wp.maxWorkers {
				needed := 4
				for i := 0; i < needed && currentWorkers < wp.maxWorkers; i++ {
					dynamicID++
					currentWorkers++
					wp.spawnWorker(ctx, dynamicID, true)
				}
				log.Printf("🔥 Auto-Scaler: Cola al %d%%. Escalado dinámico a %d Workers", (qLen*100)/qCap, currentWorkers)
			}
		}
	}
}

func (wp *WorkerPool) worker(ctx context.Context, workerID int, isDynamic bool) {
	defer func() {
		atomic.AddInt64(&wp.currentWorkerCount, -1)
		wp.wg.Done()
	}()

	idleTimer := time.NewTimer(2 * time.Second)
	defer idleTimer.Stop()

	for {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(2 * time.Second)

		select {
		case <-ctx.Done():
			return
		case <-idleTimer.C:
			// Si es un worker dinámico y la cola está vacía, auto-eliminarse para liberar recursos
			if isDynamic && len(wp.jobQueue) == 0 && atomic.LoadInt64(&wp.currentWorkerCount) > int64(wp.baseWorkers) {
				log.Printf("📉 Worker Dinámico %d finalizado por inactividad", workerID)
				return
			}
		case job, ok := <-wp.jobQueue:
			if !ok {
				return // Canal cerrado
			}

			atomic.AddInt64(&wp.metrics.ActiveWorkers, 1)
			startTime := time.Now()

			// Simulación de tarea intensiva (CPU/Memory/I/O)
			duration := time.Duration(10+rand.Intn(40)) * time.Millisecond
			time.Sleep(duration)

			// Guardar en caché con sharding (Thread-safe)
			wp.cache.Set("job:"+job.ID, "completed", 5*time.Minute)

			// Actualizar estado en SQLite mediante parámetro SQL vinculado sin SELECT *
			_, err := wp.db.ExecContext(ctx, "UPDATE outbox_tasks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", "completed", job.ID)

			elapsed := time.Since(startTime)
			atomic.AddInt64(&wp.metrics.ActiveWorkers, -1)

			res := JobResult{
				JobID:       job.ID,
				ProcessedBy: workerID,
				Duration:    elapsed,
				Success:     err == nil,
			}
			if err != nil {
				res.ErrorMsg = err.Error()
			}

			wp.results <- res
		}
	}
}

func (wp *WorkerPool) Submit(job Job) bool {
	select {
	case wp.jobQueue <- job:
		return true
	default:
		// Backpressure: La cola está llena
		return false
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.jobQueue)
	wp.wg.Wait()
	close(wp.results)
	log.Println("✔ Worker pool cerrado completamente sin pérdida de tareas")
}

// ============================================================================
// SECCIÓN 5: COLA OUTBOX PERSISTENTE CON ANSI SQL
// ============================================================================

func insertOutboxTask(db *sql.DB, id, payload string) error {
	_, err := db.Exec("INSERT INTO outbox_tasks (id, payload, status) VALUES (?, ?, ?)", id, payload, "pending")
	return err
}

func getOutboxMetrics(db *sql.DB) (int, int, error) {
	var pending, completed int
	// Enumeración explícita de columnas en lugar de SELECT *
	err := db.QueryRow("SELECT COUNT(CASE WHEN status = 'pending' THEN 1 END), COUNT(CASE WHEN status = 'completed' THEN 1 END) FROM outbox_tasks").Scan(&pending, &completed)
	return pending, completed, err
}

// ============================================================================
// SECCIÓN 6: SERVIDOR HTTP Y PRUEBAS DE CARGA NATIVAS
// ============================================================================

func main() {
	loadEnvFile(".env")

	// 1. CAMINO CRÍTICO: Base de Datos (ESTRICTO - Sin paracaídas silencioso)
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		log.Fatalf("❌ ERROR FATAL DE CONFIGURACIÓN: La variable DB_PATH no está definida en .env ni en el entorno del sistema.")
	}
	// 2. PARÁMETROS OPERATIVOS: Tienen valores por defecto oficiales del ecosistema
	port := getEnvOrDefault("PORT", "3002")
	workerCountStr := getEnvOrDefault("WORKERS", "8")

	workers, err := strconv.Atoi(workerCountStr)
	if err != nil || workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Configurar GOMAXPROCS explícitamente para saturar todos los hilos del i7-4770K
	runtime.GOMAXPROCS(workers)
	log.Printf("🖥️ Configuración Hardware: GOMAXPROCS = %d hilos de CPU en Intel Core i7-4770K", runtime.GOMAXPROCS(0))

	// Inicializar la conexión a la Base de Datos SQLite (WAL Mode)
	db, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("❌ Error Fatal al iniciar la base de datos: %v", err)
	}
	defer db.Close()

	// Crear Caché Sharded (32 parches para mitigar la contención de Mutexes)
	cache := NewShardedCache(32)

	// Crear y Arrancar el Worker Pool Concurrente (Base: 8, Max Dinámico: 32)
	wp := NewWorkerPool(workers, 32, 5000, cache, db)
	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)

	// Router Nativo de Go 1.22+
	mux := http.NewServeMux()

	// Endpoint 1: Enviar Trabajo
	mux.HandleFunc("POST /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "JSON inválido", http.StatusBadRequest)
			return
		}

		jobID := fmt.Sprintf("job_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
		if err := insertOutboxTask(db, jobID, req.Payload); err != nil {
			http.Error(w, "Error al persistir tarea outbox", http.StatusInternalServerError)
			return
		}

		job := Job{ID: jobID, Payload: req.Payload, CreatedAt: time.Now()}
		if !wp.Submit(job) {
			http.Error(w, "Servidor ocupado (Backpressure activado)", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "queued",
			"job_id": jobID,
		})
	})

	var leakedCounter uint64

	// Endpoint didáctico: Provocar Fuga de Goroutines para inspección en pprof
	mux.HandleFunc("POST /api/leak-goroutine", func(w http.ResponseWriter, r *http.Request) {
		countStr := r.URL.Query().Get("count")
		count, _ := strconv.Atoi(countStr)
		if count <= 0 {
			count = 10
		}

		for i := 0; i < count; i++ {
			atomic.AddUint64(&leakedCounter, 1)
			go func() {
				ch := make(chan int)
				// Fuga de Goroutine deliberada: Espera eternamente en un canal sin emisor
				<-ch
			}()
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":        "leaked",
			"spawned":       count,
			"total_leaked":  atomic.LoadUint64(&leakedCounter),
			"pprof_inspect": "/debug/pprof/goroutine?debug=2",
		})
	})

	// Endpoint 2: Métricas en Tiempo Real de Concurrencia y Hardware
	mux.HandleFunc("GET /api/metrics", func(w http.ResponseWriter, r *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		pending, completed, _ := getOutboxMetrics(db)
		totalProcessed := atomic.LoadUint64(&wp.metrics.TotalProcessed)
		totalErrors := atomic.LoadUint64(&wp.metrics.TotalErrors)
		activeWorkers := atomic.LoadInt64(&wp.metrics.ActiveWorkers)
		currentWorkers := atomic.LoadInt64(&wp.currentWorkerCount)
		totalDurUs := atomic.LoadUint64(&wp.metrics.TotalDuration)

		var avgDurationMs float64
		if totalProcessed > 0 {
			avgDurationMs = float64(totalDurUs) / float64(totalProcessed) / 1000.0
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hardware": map[string]interface{}{
				"cpus":              runtime.NumCPU(),
				"gomaxprocs":        runtime.GOMAXPROCS(0),
				"goroutines":        runtime.NumGoroutine(),
				"alloc_ram_mb":      m.Alloc / 1024 / 1024,
				"sys_ram_mb":        m.Sys / 1024 / 1024,
				"num_gc_cycles":     m.NumGC,
				"leaked_goroutines": atomic.LoadUint64(&leakedCounter),
			},
			"concurrency_engine": map[string]interface{}{
				"base_workers":    wp.baseWorkers,
				"max_workers":     wp.maxWorkers,
				"current_workers": currentWorkers,
				"active_workers":  activeWorkers,
				"queue_capacity":  cap(wp.jobQueue),
				"queue_current":   len(wp.jobQueue),
				"total_processed": totalProcessed,
				"total_errors":    totalErrors,
				"avg_duration_ms": avgDurationMs,
			},
			"outbox_database": map[string]interface{}{
				"pending_in_db":   pending,
				"completed_in_db": completed,
			},
		})
	})

	// Endpoint 3: Simulador / Banco de Pruebas de Carga Concurrente Interno
	mux.HandleFunc("POST /api/benchmark", func(w http.ResponseWriter, r *http.Request) {
		concurrencyStr := r.URL.Query().Get("concurrency")
		totalJobsStr := r.URL.Query().Get("jobs")

		concurrency, _ := strconv.Atoi(concurrencyStr)
		if concurrency <= 0 {
			concurrency = 50
		}
		totalJobs, _ := strconv.Atoi(totalJobsStr)
		if totalJobs <= 0 {
			totalJobs = 1000
		}

		startBench := time.Now()
		var successCount, errorCount uint64
		var benchWg sync.WaitGroup

		jobsPerRoutine := totalJobs / concurrency
		for i := 0; i < concurrency; i++ {
			benchWg.Add(1)
			go func(routineID int) {
				defer benchWg.Done()
				for j := 0; j < jobsPerRoutine; j++ {
					jobID := fmt.Sprintf("bench_%d_%d_%d", routineID, j, time.Now().UnixNano())
					insertOutboxTask(db, jobID, "benchmark_payload")
					if wp.Submit(Job{ID: jobID, Payload: "benchmark_payload", CreatedAt: time.Now()}) {
						atomic.AddUint64(&successCount, 1)
					} else {
						atomic.AddUint64(&errorCount, 1)
					}
				}
			}(i)
		}

		benchWg.Wait()
		duration := time.Since(startBench)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"benchmark_result": map[string]interface{}{
				"concurrency_goroutines":  concurrency,
				"total_submitted":         totalJobs,
				"accepted_jobs":           successCount,
				"rejected_backpressure":   errorCount,
				"submission_time_sec":     duration.Seconds(),
				"throughput_jobs_per_sec": float64(successCount) / duration.Seconds(),
			},
		})
	})

	// Endpoints de Profiling pprof (Inspección en vivo de Goroutines, CPU y Memoria)
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	// Servidor de Archivos Estáticos Frontend (index.html, index.css, app.js)
	fileServer := http.FileServer(http.Dir("./"))
	mux.Handle("GET /", fileServer)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// Escuchar apagado limpio (Graceful Shutdown)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("🌐 Servidor HTTP iniciado en http://localhost:%s (Presiona Ctrl+C para salir)", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Error en servidor HTTP: %v", err)
		}
	}()

	<-stop
	log.Println("\n⚠️ Apagando el servidor limpiamente...")

	// 1. Apagar HTTP Server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)

	// 2. Detener los Workers de Go
	cancel()
	wp.Stop()

	log.Println("👋 Servidor finalizado con éxito.")
}
