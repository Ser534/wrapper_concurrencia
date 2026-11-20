document.addEventListener('DOMContentLoaded', () => {
    // Elements
    const inputConcurrency = document.getElementById('input-concurrency');
    const inputJobs = document.getElementById('input-jobs');
    const lblConcurrency = document.getElementById('lbl-concurrency');
    const lblJobs = document.getElementById('lbl-jobs');
    const btnRunBenchmark = document.getElementById('btn-run-benchmark');
    
    // Stats Elements
    const statCpus = document.getElementById('stat-cpus');
    const statGoroutines = document.getElementById('stat-goroutines');
    const statRam = document.getElementById('stat-ram');
    const statGc = document.getElementById('stat-gc');
    const statActiveWorkers = document.getElementById('stat-active-workers');
    const statCurrentWorkers = document.getElementById('stat-current-workers');
    const statMaxWorkers = document.getElementById('stat-max-workers');
    const scalerStatusTag = document.getElementById('scaler-status-tag');
    const statQueueLevel = document.getElementById('stat-queue-level');
    const queueBar = document.getElementById('queue-bar');
    const statTotalProcessed = document.getElementById('stat-total-processed');
    const statDbCompleted = document.getElementById('stat-db-completed');
    const gomaxprocsVal = document.getElementById('gomaxprocs-val');
    const btnLeakGoroutine = document.getElementById('btn-leak-goroutine');
    
    // Results Elements
    const benchmarkResultPanel = document.getElementById('benchmark-result-panel');
    const resThroughput = document.getElementById('res-throughput');
    const resTime = document.getElementById('res-time');
    const resAccepted = document.getElementById('res-accepted');
    const resRejected = document.getElementById('res-rejected');

    // Update slider labels dynamically
    inputConcurrency.addEventListener('input', (e) => {
        lblConcurrency.textContent = e.target.value;
    });

    inputJobs.addEventListener('input', (e) => {
        lblJobs.textContent = e.target.value;
    });

    // Telemetry Polling function
    async function fetchTelemetry() {
        try {
            const res = await fetch('/api/metrics');
            if (!res.ok) return;
            const data = await res.json();

            // Hardware Telemetry
            statCpus.textContent = data.hardware.cpus;
            gomaxprocsVal.textContent = data.hardware.gomaxprocs;
            statGoroutines.textContent = data.hardware.goroutines;
            statRam.textContent = `${data.hardware.alloc_ram_mb} MB`;
            statGc.textContent = data.hardware.num_gc_cycles;

            // Engine & Outbox Telemetry
            statActiveWorkers.textContent = data.concurrency_engine.active_workers;
            if (statCurrentWorkers) statCurrentWorkers.textContent = data.concurrency_engine.current_workers;
            if (statMaxWorkers) statMaxWorkers.textContent = data.concurrency_engine.max_workers;
            
            if (scalerStatusTag) {
                if (data.concurrency_engine.current_workers > data.concurrency_engine.base_workers) {
                    scalerStatusTag.textContent = `🔥 Auto-Scaled (${data.concurrency_engine.current_workers}/${data.concurrency_engine.max_workers} W)`;
                    scalerStatusTag.style.background = 'rgba(245, 158, 11, 0.2)';
                    scalerStatusTag.style.color = '#f59e0b';
                } else {
                    scalerStatusTag.textContent = `Estable (${data.concurrency_engine.current_workers}/${data.concurrency_engine.max_workers} W)`;
                    scalerStatusTag.style.background = 'rgba(59, 130, 246, 0.15)';
                    scalerStatusTag.style.color = '#3b82f6';
                }
            }

            const qCurrent = data.concurrency_engine.queue_current;
            const qCap = data.concurrency_engine.queue_capacity;
            statQueueLevel.textContent = `${qCurrent} / ${qCap}`;
            
            const qPct = Math.min(100, Math.round((qCurrent / qCap) * 100));
            queueBar.style.width = `${qPct}%`;

            statTotalProcessed.textContent = data.concurrency_engine.total_processed.toLocaleString();
            statDbCompleted.textContent = data.outbox_database.completed_in_db.toLocaleString();
        } catch (err) {
            console.error('Error al actualizar telemetría:', err);
        }
    }

    // Goroutine Leak Simulator Listener
    if (btnLeakGoroutine) {
        btnLeakGoroutine.addEventListener('click', async () => {
            try {
                const res = await fetch('/api/leak-goroutine?count=20', { method: 'POST' });
                if (res.ok) {
                    const data = await res.json();
                    alert(`🧪 Se provocó una fuga deliberada de +20 Goroutines.\n\nTotal fugadas: ${data.total_leaked}\nPuedes observarlas en vivo en el contador de Goroutines o inspeccionando pprof.`);
                    fetchTelemetry();
                }
            } catch (err) {
                console.error('Error al simular fuga:', err);
            }
        });
    }

    // Benchmark Launcher
    btnRunBenchmark.addEventListener('click', async () => {
        const concurrency = inputConcurrency.value;
        const jobs = inputJobs.value;

        btnRunBenchmark.disabled = true;
        btnRunBenchmark.querySelector('.btn-text').textContent = '⚡ Ejecutando Prueba...';

        try {
            const res = await fetch(`/api/benchmark?concurrency=${concurrency}&jobs=${jobs}`, {
                method: 'POST'
            });
            if (!res.ok) throw new Error('Error al ejecutar la prueba de carga');
            
            const data = await res.json();
            const bench = data.benchmark_result;

            // Render Benchmark Results
            resThroughput.textContent = Math.round(bench.throughput_jobs_per_sec).toLocaleString();
            resTime.textContent = bench.submission_time_sec.toFixed(3);
            resAccepted.textContent = bench.accepted_jobs.toLocaleString();
            resRejected.textContent = bench.rejected_backpressure.toLocaleString();

            benchmarkResultPanel.classList.remove('hidden');
        } catch (err) {
            alert(`Falló el benchmark: ${err.message}`);
        } finally {
            btnRunBenchmark.disabled = false;
            btnRunBenchmark.querySelector('.btn-text').textContent = '🔥 Lanzar Prueba de Carga';
            fetchTelemetry();
        }
    });

    // Start polling every 1 second
    fetchTelemetry();
    setInterval(fetchTelemetry, 1000);
});
