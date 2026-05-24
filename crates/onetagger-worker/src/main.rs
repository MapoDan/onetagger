use std::{collections::VecDeque, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
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
    process::Command,
    sync::{mpsc, Mutex},
};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Parser, Debug, Clone)]
struct Cli {
    #[arg(long, env = "ONETAGGER_WORKER_BIND", default_value = "0.0.0.0:8080")]
    bind: String,
    #[arg(long, env = "ONETAGGER_CLI_BIN", default_value = "onetagger-cli")]
    cli_bin: String,
    #[arg(long, env = "ONETAGGER_CONFIG_DIR", default_value = "/config")]
    config_dir: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
struct JobRequest {
    file: PathBuf,
    config: Option<PathBuf>,
    extra_args: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Clone)]
struct JobAccepted {
    id: Uuid,
    queue_position: usize,
}

#[derive(Debug, Serialize, Clone)]
struct StatusResponse {
    running: Option<Uuid>,
    queued: Vec<Uuid>,
}

#[derive(Debug, Clone)]
struct Job {
    id: Uuid,
    req: JobRequest,
}

#[derive(Default)]
struct QueueState {
    running: Option<Uuid>,
    queued: VecDeque<Uuid>,
}

#[derive(Clone)]
struct AppState {
    tx: mpsc::Sender<Job>,
    queue_state: Arc<Mutex<QueueState>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    tokio::fs::create_dir_all(&cli.config_dir)
        .await
        .context("create config dir")?;

    let (tx, rx) = mpsc::channel::<Job>(1024);
    let queue_state = Arc::new(Mutex::new(QueueState::default()));

    tokio::spawn(worker_loop(rx, queue_state.clone(), cli.clone()));

    let state = AppState { tx, queue_state };
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/status", get(status_handler))
        .route("/jobs", post(enqueue_job))
        .with_state(state);

    let addr: SocketAddr = cli.bind.parse().context("invalid bind address")?;
    info!("onetagger worker listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn enqueue_job(
    State(state): State<AppState>,
    Json(req): Json<JobRequest>,
) -> impl IntoResponse {
    let id = Uuid::new_v4();
    let job = Job { id, req };

    let queue_position = {
        let mut guard = state.queue_state.lock().await;
        guard.queued.push_back(id);
        guard.queued.len()
    };

    if let Err(e) = state.tx.send(job).await {
        let mut guard = state.queue_state.lock().await;
        guard.queued.retain(|x| *x != id);
        error!("queue send failed: {e}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "queue is unavailable"})),
        )
            .into_response();
    }

    (
        StatusCode::ACCEPTED,
        Json(JobAccepted { id, queue_position }),
    )
        .into_response()
}

async fn status_handler(State(state): State<AppState>) -> Json<StatusResponse> {
    let guard = state.queue_state.lock().await;
    Json(StatusResponse {
        running: guard.running,
        queued: guard.queued.iter().copied().collect(),
    })
}

async fn worker_loop(mut rx: mpsc::Receiver<Job>, queue_state: Arc<Mutex<QueueState>>, cli: Cli) {
    while let Some(job) = rx.recv().await {
        {
            let mut s = queue_state.lock().await;
            s.queued.retain(|id| *id != job.id);
            s.running = Some(job.id);
        }

        match run_job(&cli, &job).await {
            Ok(()) => info!("job {} completed", job.id),
            Err(e) => error!("job {} failed: {e:#}", job.id),
        }

        let mut s = queue_state.lock().await;
        s.running = None;
    }
}

async fn run_job(cli: &Cli, job: &Job) -> Result<()> {
    let mut cmd = Command::new(&cli.cli_bin);
    cmd.arg("autotagger").arg("--path").arg(&job.req.file);

    let config_path = job
        .req
        .config
        .clone()
        .unwrap_or_else(|| cli.config_dir.join("autotagger.json"));
    cmd.arg("--config").arg(config_path);

    if let Some(extra_args) = &job.req.extra_args {
        cmd.args(extra_args);
    }

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

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
