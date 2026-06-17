use std::{
    collections::VecDeque,
    ffi::OsStr,
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

    /// Directory where successfully tagged single-file jobs are moved by the wrapper.
    #[arg(long, env = "ONETAGGER_TAGGED_DIR", default_value = "/tubetube/Tagged")]
    tagged_dir: PathBuf,

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
    original_path: PathBuf,
    temp_playlist: Option<PathBuf>,
    tagged_dir: PathBuf,
    success_destination: Option<PathBuf>,
}

/// Queue tracking state used to provide visibility via `/status`.
#[derive(Default)]
struct QueueState {
    running: Option<Uuid>,
    queued: VecDeque<Uuid>,
}

const PLAYLIST_EXTENSIONS: [&str; 2] = ["m3u", "m3u8"];

#[derive(Debug)]
struct NormalizedInput {
    cli_path: PathBuf,
    temp_playlist: Option<PathBuf>,
}

#[derive(Debug, Default)]
struct AutotaggerMoveConfig {
    move_success: bool,
    move_success_path: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    tx: mpsc::Sender<Job>,
    queue_state: Arc<Mutex<QueueState>>,
    config_dir: PathBuf,
    tagged_dir: PathBuf,
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
        tagged_dir = %cli.tagged_dir.display(),
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
        tagged_dir: cli.tagged_dir.clone(),
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

    let move_config = load_move_config(&resolved_config).await.map_err(|e| {
        error!(config = %resolved_config.display(), error = %e, "job rejected: failed reading autotagger move configuration");
        (
            StatusCode::BAD_REQUEST,
            serde_json::json!({
                "error": format!("failed reading autotagger move configuration: {e}"),
                "config": resolved_config.display().to_string()
            }),
        )
    })?;

    let id = Uuid::new_v4();
    let original_path = req.file.clone();

    let normalized_input = normalize_cli_input_path(&state.config_dir, id, &req.file)
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
    req.file = normalized_input.cli_path;

    let success_destination = move_config
        .move_success
        .then_some(move_config.move_success_path)
        .flatten();

    let job = Job {
        id,
        req,
        original_path,
        temp_playlist: normalized_input.temp_playlist,
        tagged_dir: state.tagged_dir.clone(),
        success_destination,
    };

    let queue_position = {
        let mut guard = state.queue_state.lock().await;
        guard.queued.push_back(id);
        guard.queued.len()
    };

    info!(
        job_id = %id,
        original_path = %job.original_path.display(),
        cli_path = %job.req.file.display(),
        temp_playlist = ?job.temp_playlist,
        wrapper_tagged_dir = %job.tagged_dir.display(),
        success_destination = ?job.success_destination,
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

        let job_result = async {
            run_job(&cli, &job).await?;
            info!(job_id = %job.id, "onetagger-cli completed successfully");
            move_single_file_after_success(&job).await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match &job_result {
            Ok(()) => info!(job_id = %job.id, "job completed"),
            Err(e) => error!(job_id = %job.id, error = %e, "job failed"),
        }

        cleanup_temp_playlist(&job).await;

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

    let command_preview = build_command_preview(cli, job, &config_path);
    info!(
        job_id = %job.id,
        cli = %cli.cli_bin,
        original_path = %job.original_path.display(),
        cli_path = %job.req.file.display(),
        temp_playlist = ?job.temp_playlist,
        wrapper_tagged_dir = %job.tagged_dir.display(),
        config = %config_path.display(),
        extra_args = ?job.req.extra_args,
        command = ?command_preview,
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
) -> Result<NormalizedInput> {
    if requested.is_dir() {
        return Ok(NormalizedInput {
            cli_path: requested.to_path_buf(),
            temp_playlist: None,
        });
    }

    if !requested.is_file() {
        bail!("path is neither existing directory nor file");
    }

    let ext = requested
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default();

    if PLAYLIST_EXTENSIONS.iter().any(|e| *e == ext) {
        return Ok(NormalizedInput {
            cli_path: requested.to_path_buf(),
            temp_playlist: None,
        });
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

    Ok(NormalizedInput {
        cli_path: playlist_path.clone(),
        temp_playlist: Some(playlist_path),
    })
}

async fn load_move_config(config_path: &Path) -> Result<AutotaggerMoveConfig> {
    let bytes = fs::read(config_path)
        .await
        .with_context(|| format!("failed reading config {}", config_path.display()))?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed parsing config {}", config_path.display()))?;

    let move_success = json
        .get("moveSuccess")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let move_success_path = json
        .get("moveSuccessPath")
        .and_then(|v| v.as_str())
        .filter(|p| !p.trim().is_empty())
        .map(PathBuf::from);

    if move_success {
        match &move_success_path {
            Some(path) if path.exists() => {
                info!(destination = %path.display(), "autotagger success destination configured and mounted");
            }
            Some(path) => {
                warn!(
                    destination = %path.display(),
                    "autotagger success destination is configured but does not exist in the container; verify the host path is mounted or files may appear missing"
                );
            }
            None => {
                warn!("autotagger moveSuccess is enabled but moveSuccessPath is empty; tagged files may remain in the input folder");
            }
        }
    } else {
        warn!("autotagger moveSuccess is disabled; tagged files are expected to remain at the original input path");
    }

    Ok(AutotaggerMoveConfig {
        move_success,
        move_success_path,
    })
}

fn build_command_preview(cli: &Cli, job: &Job, config_path: &Path) -> Vec<String> {
    let mut command = vec![
        cli.cli_bin.clone(),
        "autotagger".to_string(),
        "--path".to_string(),
        job.req.file.display().to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    if let Some(extra_args) = &job.req.extra_args {
        command.extend(extra_args.clone());
    }
    command
}

async fn move_single_file_after_success(job: &Job) -> Result<Option<PathBuf>> {
    if job.temp_playlist.is_none() {
        debug!(job_id = %job.id, "wrapper move skipped for directory/playlist request");
        return Ok(None);
    }

    let original = &job.original_path;
    if !original.exists() {
        bail!(
            "cannot move tagged file because original path no longer exists after tagging: {}",
            original.display()
        );
    }

    ensure_tagged_dir(&job.tagged_dir).await?;
    let final_path = unique_destination_path(&job.tagged_dir, original)?;

    if final_path.file_name() != original.file_name() {
        warn!(
            job_id = %job.id,
            original_path = %original.display(),
            final_path = %final_path.display(),
            "tagged destination filename conflict resolved with suffix"
        );
    }

    move_file(original, &final_path).await.with_context(|| {
        format!(
            "failed moving tagged file from {} to {}",
            original.display(),
            final_path.display()
        )
    })?;

    info!(
        job_id = %job.id,
        original_path = %original.display(),
        final_path = %final_path.display(),
        "tagged file moved by worker"
    );

    Ok(Some(final_path))
}

async fn ensure_tagged_dir(path: &Path) -> Result<()> {
    if path.exists() {
        if path.is_dir() {
            return Ok(());
        }
        bail!(
            "configured tagged destination exists but is not a directory: {}",
            path.display()
        );
    }

    fs::create_dir_all(path)
        .await
        .with_context(|| format!("failed creating tagged destination {}", path.display()))?;
    info!(tagged_dir = %path.display(), "created tagged destination directory");
    Ok(())
}

fn unique_destination_path(destination_dir: &Path, source: &Path) -> Result<PathBuf> {
    let filename = source
        .file_name()
        .ok_or_else(|| anyhow!("source path has no filename: {}", source.display()))?;
    let first = destination_dir.join(filename);
    if !first.exists() {
        return Ok(first);
    }

    let stem = source
        .file_stem()
        .and_then(OsStr::to_str)
        .ok_or_else(|| anyhow!("source path has no valid file stem: {}", source.display()))?;
    let extension = source.extension().and_then(OsStr::to_str);

    for index in 1..10_000 {
        let filename = match extension {
            Some(extension) if !extension.is_empty() => format!("{stem} ({index}).{extension}"),
            _ => format!("{stem} ({index})"),
        };
        let candidate = destination_dir.join(filename);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    bail!(
        "failed finding available destination filename in {} for {}",
        destination_dir.display(),
        source.display()
    )
}

async fn move_file(source: &Path, destination: &Path) -> Result<()> {
    match fs::rename(source, destination).await {
        Ok(()) => Ok(()),
        Err(rename_error) => {
            warn!(
                source = %source.display(),
                destination = %destination.display(),
                error = %rename_error,
                "rename failed, falling back to copy and remove"
            );
            fs::copy(source, destination).await.with_context(|| {
                format!(
                    "failed copying tagged file from {} to {}",
                    source.display(),
                    destination.display()
                )
            })?;
            fs::remove_file(source).await.with_context(|| {
                format!(
                    "failed removing source file after copy fallback: {}",
                    source.display()
                )
            })?;
            Ok(())
        }
    }
}

async fn cleanup_temp_playlist(job: &Job) {
    let Some(path) = &job.temp_playlist else {
        return;
    };

    match fs::remove_file(path).await {
        Ok(()) => {
            info!(job_id = %job.id, temp_playlist = %path.display(), "temporary playlist cleaned up")
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!(job_id = %job.id, temp_playlist = %path.display(), "temporary playlist was already removed")
        }
        Err(e) => {
            warn!(job_id = %job.id, temp_playlist = %path.display(), error = %e, "failed cleaning up temporary playlist")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_path(filename: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("onetagger-worker-test-{nanos}-{filename}"))
    }

    #[tokio::test]
    async fn load_move_config_reads_success_destination() {
        let config_path = unique_test_path("config.json");
        fs::write(
            &config_path,
            r#"{"moveSuccess":true,"moveSuccessPath":"/tubetube/Tagged"}"#,
        )
        .await
        .expect("write config");

        let config = load_move_config(&config_path).await.expect("load config");

        assert!(config.move_success);
        assert_eq!(
            config.move_success_path,
            Some(PathBuf::from("/tubetube/Tagged"))
        );

        let _ = fs::remove_file(config_path).await;
    }

    #[test]
    fn unique_destination_path_adds_suffix_on_conflict() {
        let dir = unique_test_path("tagged-dir");
        std::fs::create_dir_all(&dir).expect("create tagged dir");
        std::fs::write(dir.join("track.mp3"), b"existing").expect("write existing");

        let candidate = unique_destination_path(&dir, Path::new("/input/track.mp3"))
            .expect("unique destination");

        assert_eq!(candidate, dir.join("track (1).mp3"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn move_file_renames_to_destination() {
        let dir = unique_test_path("move-dir");
        let source = unique_test_path("move-source.mp3");
        fs::create_dir_all(&dir).await.expect("create dir");
        fs::write(&source, b"tagged").await.expect("write source");
        let destination = dir.join("move-source.mp3");

        move_file(&source, &destination).await.expect("move file");

        assert!(!source.exists());
        assert_eq!(fs::read(&destination).await.expect("read dest"), b"tagged");

        let _ = fs::remove_dir_all(dir).await;
    }

    #[tokio::test]
    async fn normalize_single_file_creates_temp_playlist() {
        let config_dir = unique_test_path("config-dir");
        let input_file = unique_test_path("track.mp3");
        fs::create_dir_all(&config_dir)
            .await
            .expect("create config dir");
        fs::write(&input_file, b"fake mp3")
            .await
            .expect("write input");

        let normalized = normalize_cli_input_path(&config_dir, Uuid::new_v4(), &input_file)
            .await
            .expect("normalize input");

        assert!(normalized.cli_path.exists());
        assert_eq!(normalized.temp_playlist, Some(normalized.cli_path.clone()));

        let playlist = fs::read_to_string(&normalized.cli_path)
            .await
            .expect("read playlist");
        assert!(playlist.contains("#EXTM3U"));
        assert!(playlist.contains(&input_file.display().to_string()));

        let _ = fs::remove_file(&normalized.cli_path).await;
        let _ = fs::remove_file(input_file).await;
        let _ = fs::remove_dir_all(config_dir).await;
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received (ctrl-c)");
}
