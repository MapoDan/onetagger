use std::{
    collections::VecDeque,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    process::Command,
    sync::{mpsc, Mutex},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// OneTagger Worker runtime configuration.
///
/// Values are provided via CLI flags or environment variables to support
/// container-first deployment patterns (Docker/Portainer/Kubernetes).
#[derive(Parser, Debug, Clone)]
struct Cli {
    /// Bind address for the worker HTTP API.
    #[arg(long, env = "ONETAGGER_WORKER_BIND", default_value = "0.0.0.0:8080")]
    bind: String,

    /// Path to onetagger-cli binary invoked by worker jobs.
    #[arg(long, env = "ONETAGGER_CLI_BIN", default_value = "onetagger-cli")]
    cli_bin: String,

    /// Directory used to store/read externalized configuration.
    #[arg(long, env = "ONETAGGER_CONFIG_DIR", default_value = "/config")]
    config_dir: PathBuf,

    /// Optional path automatically queued once at worker startup.
    ///
    /// Leave unset for pure API-driven operation. This is useful for simple
    /// deployments that want the container to process a mounted folder on boot.
    #[arg(long, env = "ONETAGGER_STARTUP_PATH")]
    startup_path: Option<PathBuf>,
}

/// Payload accepted by `POST /jobs`.
#[derive(Debug, Deserialize, Clone)]
struct JobRequest {
    /// File, folder, or playlist path consumed by `onetagger-cli autotagger --path`.
    file: PathBuf,
    /// Optional explicit autotagger config path.
    /// Falls back to `<config_dir>/autotagger.json` when omitted.
    config: Option<PathBuf>,
    /// Optional additional CLI arguments forwarded to `onetagger-cli autotagger`.
    extra_args: Option<Vec<String>>,
}

/// Response emitted when a job is accepted and queued.
#[derive(Debug, Serialize, Clone)]
struct JobAccepted {
    id: Uuid,
    queue_position: usize,
}

/// Operational snapshot exposed by `GET /status`.
#[derive(Debug, Serialize, Clone)]
struct StatusResponse {
    running: Option<Uuid>,
    queued: Vec<Uuid>,
}

/// Internal queue item.
#[derive(Debug, Clone)]
struct Job {
    id: Uuid,
    req: JobRequest,
}

/// Queue tracking state used to provide visibility via `/status`.
#[derive(Default)]
struct QueueState {
    running: Option<Uuid>,
    queued: VecDeque<Uuid>,
}

const PLAYLIST_EXTENSIONS: [&str; 2] = ["m3u", "m3u8"];

#[derive(Clone)]
struct AppState {
    tx: mpsc::Sender<Job>,
    queue_state: Arc<Mutex<QueueState>>,
    config_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    info!(
        bind = %cli.bind,
        cli_bin = %cli.cli_bin,
        config_dir = %cli.config_dir.display(),
        startup_path = ?cli.startup_path,
        "starting onetagger worker"
    );

    tokio::fs::create_dir_all(&cli.config_dir)
        .await
        .context("create config dir")?;
    info!(config_dir = %cli.config_dir.display(), "ensured config directory exists");

    ensure_default_config(&cli).await?;

    // Single consumer with buffered producer channel guarantees serialized execution.
    let (tx, rx) = mpsc::channel::<Job>(1024);
    let queue_state = Arc::new(Mutex::new(QueueState::default()));

    tokio::spawn(worker_loop(rx, queue_state.clone(), cli.clone()));

    let state = AppState {
        tx,
        queue_state,
        config_dir: cli.config_dir.clone(),
    };

    if let Some(startup_path) = &cli.startup_path {
        info!(path = %startup_path.display(), "startup path configured, enqueueing initial job");
        enqueue_startup_job(&state, startup_path.clone()).await;
    } else {
        info!("no startup path configured; worker is idle and waiting for POST /jobs requests");
    }
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/status", get(status_handler))
        .route("/jobs", post(enqueue_job))
        .with_state(state);

    let addr: SocketAddr = cli.bind.parse().context("invalid bind address")?;
    info!(%addr, "onetagger worker listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("worker shutdown completed");
    Ok(())
}

async fn enqueue_startup_job(state: &AppState, file: PathBuf) {
    let req = JobRequest {
        file,
        config: None,
        extra_args: None,
    };

    match prepare_job(state, req).await {
        Ok((job, queue_position)) => {
            let job_id = job.id;
            if let Err(e) = state.tx.send(job).await {
                let mut guard = state.queue_state.lock().await;
                guard.queued.retain(|x| *x != job_id);
                error!(job_id = %job_id, error = %e, "startup job queue send failed");
                return;
            }

            info!(job_id = %job_id, queue_position, "startup job queued");
        }
        Err((status, body)) => {
            error!(status = %status, error = %body, "startup job rejected");
        }
    }
}

async fn prepare_job(
    state: &AppState,
    req: JobRequest,
) -> Result<(Job, usize), (StatusCode, serde_json::Value)> {
    info!(payload = ?req, "received enqueue request payload");

    if !req.file.exists() {
        error!(path = %req.file.display(), "job rejected: input path does not exist");
        return Err((
            StatusCode::BAD_REQUEST,
            serde_json::json!({
                "error": "input path does not exist",
                "path": req.file.display().to_string()
            }),
        ));
    }

    let resolved_config = req
        .config
        .clone()
        .unwrap_or_else(|| state.config_dir.join("autotagger.json"));
    if !resolved_config.exists() {
        error!(
            config = %resolved_config.display(),
            "job rejected: config path does not exist"
        );
        return Err((
            StatusCode::BAD_REQUEST,
            serde_json::json!({
                "error": "config path does not exist",
                "config": resolved_config.display().to_string(),
                "hint": "mount /config and provide autotagger.json or pass explicit config in payload"
            }),
        ));
    }

    let id = Uuid::new_v4();

    let resolved_job_file = normalize_cli_input_path(&state.config_dir, id, &req.file)
        .await
        .map_err(|e| {
            error!(job_id = %id, path = %req.file.display(), error = %e, "job rejected: invalid input path for cli");
            (
                StatusCode::BAD_REQUEST,
                serde_json::json!({
                    "error": format!("invalid input path: {e}"),
                    "path": req.file.display().to_string()
                }),
            )
        })?;

    let mut req = req;
    req.file = resolved_job_file;

    let job = Job { id, req };

    let queue_position = {
        let mut guard = state.queue_state.lock().await;
        guard.queued.push_back(id);
        guard.queued.len()
    };

    info!(
        job_id = %id,
        path = %job.req.file.display(),
        queue_position,
        has_custom_config = job.req.config.is_some(),
        extra_args = job.req.extra_args.as_ref().map(|a| a.len()).unwrap_or(0),
        "job accepted"
    );

    Ok((job, queue_position))
}

async fn enqueue_job(
    State(state): State<AppState>,
    Json(req): Json<JobRequest>,
) -> impl IntoResponse {
    let (job, queue_position) = match prepare_job(&state, req).await {
        Ok(prepared) => prepared,
        Err((status, body)) => return (status, Json(body)).into_response(),
    };

    let id = job.id;
    if let Err(e) = state.tx.send(job).await {
        let mut guard = state.queue_state.lock().await;
        guard.queued.retain(|x| *x != id);
        error!(job_id = %id, error = %e, "queue send failed");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "queue is unavailable"})),
        )
            .into_response();
    }

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!(JobAccepted { id, queue_position })),
    )
        .into_response()
}

async fn status_handler(State(state): State<AppState>) -> Json<StatusResponse> {
    let guard = state.queue_state.lock().await;
    let status = StatusResponse {
        running: guard.running,
        queued: guard.queued.iter().copied().collect(),
    };
    debug!(running = ?status.running, queued = status.queued.len(), "status requested");
    Json(status)
}

/// Long-running consumer loop that executes exactly one job at a time.
async fn worker_loop(mut rx: mpsc::Receiver<Job>, queue_state: Arc<Mutex<QueueState>>, cli: Cli) {
    info!("worker loop started");

    while let Some(job) = rx.recv().await {
        {
            let mut s = queue_state.lock().await;
            s.queued.retain(|id| *id != job.id);
            s.running = Some(job.id);
            info!(job_id = %job.id, remaining_queue = s.queued.len(), "job started");
        }

        match run_job(&cli, &job).await {
            Ok(()) => info!(job_id = %job.id, "job completed"),
            Err(e) => error!(job_id = %job.id, error = %e, "job failed"),
        }

        let mut s = queue_state.lock().await;
        s.running = None;
    }

    warn!("worker queue channel closed, no further jobs will be processed");
}

/// Build and execute `onetagger-cli autotagger` command for one queue item.
async fn run_job(cli: &Cli, job: &Job) -> Result<()> {
    let config_path = job
        .req
        .config
        .clone()
        .unwrap_or_else(|| cli.config_dir.join("autotagger.json"));

    let mut cmd = Command::new(&cli.cli_bin);
    cmd.arg("autotagger")
        .arg("--path")
        .arg(&job.req.file)
        .arg("--config")
        .arg(&config_path);

    if let Some(extra_args) = &job.req.extra_args {
        cmd.args(extra_args);
    }

    info!(
        job_id = %job.id,
        cli = %cli.cli_bin,
        path = %job.req.file.display(),
        config = %config_path.display(),
        extra_args = ?job.req.extra_args,
        "executing onetagger-cli job"
    );

    let output = cmd
        .output()
        .await
        .context("failed to launch onetagger-cli")?;

    if !output.status.success() {
        return Err(anyhow!(
            "cli exited with status {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    debug!(
        job_id = %job.id,
        stdout_bytes = output.stdout.len(),
        stderr_bytes = output.stderr.len(),
        "cli process completed successfully"
    );
    Ok(())
}

/// Ensures that `/config/autotagger.json` exists for default worker flow.
///
/// If missing, we generate it by invoking `onetagger-cli --autotagger-config` and
/// writing the output to the configured path.
async fn ensure_default_config(cli: &Cli) -> Result<()> {
    let config_path = cli.config_dir.join("autotagger.json");
    if config_path.exists() {
        info!(config = %config_path.display(), "default autotagger config found");
        return Ok(());
    }

    warn!(
        config = %config_path.display(),
        "default autotagger config missing, generating it"
    );

    let output = Command::new(&cli.cli_bin)
        .arg("--autotagger-config")
        .output()
        .await
        .context("failed to run onetagger-cli --autotagger-config")?;

    if !output.status.success() {
        bail!(
            "failed to generate default config, status={} stderr={} stdout={}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
    }

    fs::write(&config_path, &output.stdout)
        .await
        .with_context(|| format!("failed writing default config to {}", config_path.display()))?;

    info!(config = %config_path.display(), "default autotagger config generated");
    Ok(())
}

/// Normalize API `file` path into a CLI-compatible `--path` value.
///
/// `onetagger-cli autotagger` treats any file path as a playlist file. For single
/// audio file requests, worker creates an ephemeral `.m3u8` file that points to
/// that audio file and passes playlist path to CLI.
async fn normalize_cli_input_path(
    config_dir: &Path,
    job_id: Uuid,
    requested: &Path,
) -> Result<PathBuf> {
    if requested.is_dir() {
        return Ok(requested.to_path_buf());
    }

    if !requested.is_file() {
        bail!("path is neither existing directory nor file");
    }

    let ext = requested
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default();

    if PLAYLIST_EXTENSIONS.iter().any(|e| *e == ext) {
        return Ok(requested.to_path_buf());
    }

    let queue_dir = config_dir.join("queue");
    fs::create_dir_all(&queue_dir)
        .await
        .with_context(|| format!("failed creating queue dir {}", queue_dir.display()))?;

    let playlist_path = queue_dir.join(format!("job-{job_id}.m3u8"));
    let data = format!("#EXTM3U\n{}\n", requested.display());
    fs::write(&playlist_path, data)
        .await
        .with_context(|| format!("failed writing temp playlist {}", playlist_path.display()))?;

    info!(
        job_id = %job_id,
        requested = %requested.display(),
        temp_playlist = %playlist_path.display(),
        "wrapped single file request into temporary playlist for cli compatibility"
    );

    Ok(playlist_path)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received (ctrl-c)");
}
