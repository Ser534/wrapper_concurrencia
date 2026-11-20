# Reglas del Proyecto: Servicio API Wrapper de Concurrencia y Respaldo Outbox

Este documento define las reglas de diseño, arquitectura, concurrencia y comportamiento para cualquier agente de IA que trabaje en esta base de código (`/home/ser/534/wrapper_concurrencia/`). Se cargan automáticamente al iniciar cualquier sesión en este espacio de trabajo.

## 🏗️ Directrices de Arquitectura

1. **Camino del "No-Framework"**:
   - Depender únicamente del compilador estándar de Go (sin frameworks de backend como Gin/Echo ni routers externos; usar `http.ServeMux` nativo de Go 1.22+).
   - Servidor HTTP independiente escuchando en puerto dedicado (`PORT=3002` por defecto en `.env`).
   - Sin ORM ni abstracciones SQL (no usar GORM, SQLX ni Ent). Usar consultas SQL puras con el paquete estándar `database/sql` y driver `github.com/mattn/go-sqlite3`.
   - Interfaz de panel en Vanilla HTML, CSS y JS, consumiendo directamente APIs nativas del navegador (sin bundlers como Vite o Webpack).

2. **Monolito Estricto y Archivos Auxiliares**:
   - El backend completo del wrapper reside en un único archivo [main.go](file:///home/ser/534/wrapper_concurrencia/main.go) organizado mediante comentarios-carteles separadores por secciones.
   - Las pruebas unitarias e integración de concurrencia residen en [main_test.go](file:///home/ser/534/wrapper_concurrencia/main_test.go).
   - El frontend del panel de control reside en [index.html](file:///home/ser/534/wrapper_concurrencia/index.html), [index.css](file:///home/ser/534/wrapper_concurrencia/index.css), y [app.js](file:///home/ser/534/wrapper_concurrencia/app.js).

3. **Base de Datos SQLite Concurrente y Segura**:
   - Archivo único de base de datos compartida ubicado en `/home/ser/534/db/534.db`.
   - La conexión en Go debe habilitar el registro en diario de tipo WAL y un timeout largo de espera:
     `sql.Open("sqlite3", "/home/ser/534/db/534.db?_journal_mode=WAL&_busy_timeout=5000")`
   - **Comprobación Pasiva:** En el arranque normal del servidor Go (`initDB`), no se crean ni alteran tablas de forma automática. El programa verifica que la base de datos contenga la tabla `outbox_tasks`. Si no existe, termina con un error fatal indicando que se debe ejecutar la migración externa previa.
   - **Migración Externa:** Los cambios y esquemas se gestionan en scripts centralizados dentro de `/home/ser/534/migrations/` (ej. `/home/ser/534/migrations/005_concurrency_outbox.sql`).

## ⚡ Motor de Concurrencia y Patrón Outbox

1. **Caché Fragmentada en Memoria (Sharded Cache)**:
   - Implementación de `ShardedCache` dividida en 32 parches (`CacheShard`) indexados mediante hash FNV-1a para eliminar la contención de cerrojos (`RWMutex`) bajo cargas extremas.

2. **Worker Pool Dinámico con Auto-Scaler**:
   - **Workers Base:** Arranca con un pool fijo (configurable vía `WORKERS` o `runtime.NumCPU()`).
   - **Auto-Escalado Dinámico:** Si la cola en memoria `jobQueue` supera el 40% de su capacidad (`queueCapacity=5000`), el auto-scaler genera goroutines dinámicas hasta un techo máximo (`maxWorkers=32`).
   - **Scale-Down Automático:** Los workers dinámicos se auto-eliminan tras 2 segundos de inactividad si la cola está vacía.

3. **Garantía Outbox y Respaldo Persistente**:
   - Toda tarea entrante se persiste en SQLite en estado `pending` antes de ingresar al canal de ejecución.
   - Tras el procesamiento exitoso o fallido por parte del worker pool, se actualiza el estado en `outbox_tasks` mediante SQL con parámetro vinculado (`?`).

4. **Backpressure**:
   - Si la capacidad de la canal de entrada está saturada, se responde con código HTTP `503 Service Unavailable` indicando sobrecarga.

5. **Graceful Shutdown**:
   - Ante señales del sistema (`SIGINT`, `SIGTERM`), el servidor detiene primero la escucha de nuevas peticiones HTTP, vacía los canales de trabajo activos, detiene el pool de workers limpiamente y cierra la conexión a SQLite.

## ⚙️ Configuración y Variables de Entorno

1. **Gestión Nivel Entorno**:
   - No se usan librerías externas de terceros (como `godotenv`).
   - Parser simple en [main.go](file:///home/ser/534/wrapper_concurrencia/main.go) (`loadEnvFile`) que lee `.env` si existe y aplica `os.Setenv()`.
   - **Camino Crítico Estricto (Fail-Fast)**: `DB_PATH` debe estar obligatoriamente definido en `.env` o en las variables de entorno del sistema. Prohibido usar paracaídas o rutas por defecto silenciosas para la base de datos para evitar la corrupción o duplicación accidental de datos. Si falta `DB_PATH`, el programa debe detenerse inmediatamente con `log.Fatalf`.
   - Parámetros soportados:
     - `PORT`: Puerto donde escucha el servidor web (por defecto `3002` si no se especifica).
     - `DB_PATH`: Ruta al archivo SQLite compartido (**Requerido / Estricto**, ej: `/home/ser/534/db/534.db`).
     - `WORKERS`: Número de workers iniciales en el pool (por defecto `8` o número de núcleos CPU).

## 📡 Endpoints de la API y Profiling

1. **Endpoints Principales**:
   - `POST /api/jobs`: Recibe payload JSON, genera ID único, persiste la tarea en `outbox_tasks` y la envía al Worker Pool.
   - `GET /api/metrics`: Retorna métricas en vivo de CPU, Goroutines, RAM, GC cycles, rendimiento del Worker Pool y conteos de la tabla `outbox_tasks`.
   - `POST /api/benchmark`: Banco de pruebas interno de carga concurrente.
   - `POST /api/leak-goroutine`: Endpoint educativo para forzar fugas de goroutines e inspeccionarlas en vivo.

2. **Endpoints de Profiling pprof**:
   - Rutas nativas habilitadas en `http.ServeMux`: `/debug/pprof/`, `/debug/pprof/cmdline`, `/debug/pprof/profile`, `/debug/pprof/symbol`, `/debug/pprof/trace`.

## 🔄 Estrategia de Migraciones y Consultas SQL

1. **Prohibición Estricta de `SELECT *` en Go**:
   - **NUNCA** usar `SELECT *` en las consultas de Go. Enumerar explícitamente todas las columnas requeridas (`SELECT id, payload, status FROM outbox_tasks`).
   - Esto evita fallos de parseo (`Scan()`) al añadir columnas con valores por defecto en producción.

2. **Parameter Binding Obligatorio**:
   - **100% de las consultas SQL** deben utilizar parámetros vinculados (`?` en SQLite).
   - Prohibido concatenar variables en cadenas SQL (`fmt.Sprintf`).

## 📜 Registro de Conversaciones ("Bitácora de Sesiones")

1. **Transcripción Literal y Completa (Copia Textual Idéntica)**:
   - Para cada sesión o charla de trabajo, la IA debe generar y guardar una copia textual, literal e idéntica de la conversación en `docs/conversations/YYYY-MM-DD_transcripcion_literal.md`.
   - **Prohibido resumir o editar:** Cada mensaje del usuario y cada respuesta de la IA debe guardarse tal cual se realizó en la charla.

2. **Formato de Transcripción**:
   - Encabezado con la fecha de la sesión.
   - Bloques diferenciados `### 👤 USER:` y `### 🤖 ASSISTANT:` respetando íntegramente todo el texto intercambiado.

## 📁 Control de Versiones (.gitignore)

Configurado para excluir:
- Archivos SQLite de base de datos (`*.db`, `*.db-wal`, `*.db-shm`).
- Archivos de configuración local (`.env`, `.env.*`).
- Binarios compilados de Go (`main`, `*.exe`).
