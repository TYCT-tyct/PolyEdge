mod model;
mod search;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use model::{JobProgress, JobRecord, SearchRequest};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    jobs: Arc<DashMap<String, JobRecord>>,
}

#[derive(serde::Serialize)]
struct HealthResponse {
    status: &'static str,
    job_count: usize,
}

#[derive(serde::Serialize)]
struct CreateJobResponse {
    job_id: String,
    status_url: String,
}

#[derive(serde::Serialize)]
struct ApiError {
    error: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "polyedge_hpo=info,info".to_string()),
        )
        .with_target(false)
        .compact()
        .init();

    let bind = std::env::var("POLYEDGE_HPO_BIND").unwrap_or_else(|_| "0.0.0.0:9820".to_string());
    let addr: SocketAddr = bind
        .parse()
        .with_context(|| format!("invalid POLYEDGE_HPO_BIND value: {bind}"))?;

    let state = AppState {
        jobs: Arc::new(DashMap::new()),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/hpo/search", post(search_once))
        .route("/api/hpo/jobs", post(create_job).get(list_jobs))
        .route("/api/hpo/jobs/{job_id}", get(get_job))
        .with_state(state);

    info!(bind = %addr, "polyedge hpo service started");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        job_count: state.jobs.len(),
    })
}

async fn search_once(Json(req): Json<SearchRequest>) -> impl IntoResponse {
    match req.clone().resolve() {
        Ok(resolved) => match search::run_search(resolved, search::SearchHooks::no_op()).await {
            Ok(result) => (StatusCode::OK, Json(result)).into_response(),
            Err(err) => {
                error!(?err, "sync hpo search failed");
                (
                    StatusCode::BAD_GATEWAY,
                    Json(ApiError {
                        error: err.to_string(),
                    }),
                )
                    .into_response()
            }
        },
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(ApiError {
                error: err.to_string(),
            }),
        )
            .into_response(),
    }
}

async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<SearchRequest>,
) -> impl IntoResponse {
    let job_id = Uuid::new_v4().to_string();
    state
        .jobs
        .insert(job_id.clone(), JobRecord::new(job_id.clone(), req.clone()));
    let state_for_worker = state.clone();
    let job_id_for_worker = job_id.clone();
    tokio::spawn(async move {
        run_job(state_for_worker, job_id_for_worker, req).await;
    });
    let status_url = format!("/api/hpo/jobs/{job_id}");
    (
        StatusCode::ACCEPTED,
        Json(CreateJobResponse { job_id, status_url }),
    )
}

async fn list_jobs(State(state): State<AppState>) -> impl IntoResponse {
    let mut jobs = state
        .jobs
        .iter()
        .map(|j| j.value().clone())
        .collect::<Vec<JobRecord>>();
    jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(jobs)
}

async fn get_job(Path(job_id): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
    match state.jobs.get(&job_id) {
        Some(job) => (StatusCode::OK, Json(job.clone())).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("job not found: {job_id}"),
            }),
        )
            .into_response(),
    }
}

async fn run_job(state: AppState, job_id: String, req: SearchRequest) {
    set_job_status(&state, &job_id, "running", "running", 0, 0, 0, None, None);
    let resolved = match req.clone().resolve() {
        Ok(v) => v,
        Err(err) => {
            set_job_status(
                &state,
                &job_id,
                "failed",
                "validate",
                0,
                0,
                0,
                None,
                Some(err.to_string()),
            );
            return;
        }
    };
    let total = resolved.search.iterations;
    let state_for_hook = state.clone();
    let id_for_hook = job_id.clone();
    let state_for_hook2 = state.clone();
    let id_for_hook2 = job_id.clone();
    let hooks = search::SearchHooks {
        on_iteration: Some(Box::new(move |it, total_it, evaluated, best| {
            set_job_status(
                &state_for_hook,
                &id_for_hook,
                "running",
                "search",
                it,
                total_it,
                evaluated,
                Some(best),
                None,
            );
        })),
        on_candidate: Some(Box::new(move |it, total_it, evaluated, best| {
            set_job_status(
                &state_for_hook2,
                &id_for_hook2,
                "running",
                "search",
                it,
                total_it,
                evaluated,
                best,
                None,
            );
        })),
    };
    match search::run_search(resolved, hooks).await {
        Ok(result) => {
            if let Some(mut entry) = state.jobs.get_mut(&job_id) {
                entry.status = "completed".to_string();
                entry.updated_at = model::now_utc();
                entry.progress = JobProgress {
                    stage: "completed".to_string(),
                    iteration: total,
                    total_iterations: total,
                    evaluated_candidates: total.saturating_mul(result.population),
                    best_score: Some(result.best.robust_score),
                };
                entry.result = Some(result);
                entry.error = None;
            }
            info!(job_id = %job_id, "hpo job completed");
        }
        Err(err) => {
            warn!(job_id = %job_id, ?err, "hpo job failed");
            set_job_status(
                &state,
                &job_id,
                "failed",
                "search",
                0,
                total,
                0,
                None,
                Some(err.to_string()),
            );
        }
    }
}

fn set_job_status(
    state: &AppState,
    job_id: &str,
    status: &str,
    stage: &str,
    iteration: usize,
    total_iterations: usize,
    evaluated_candidates: usize,
    best_score: Option<f64>,
    error: Option<String>,
) {
    if let Some(mut entry) = state.jobs.get_mut(job_id) {
        entry.status = status.to_string();
        entry.updated_at = model::now_utc();
        entry.progress = JobProgress {
            stage: stage.to_string(),
            iteration,
            total_iterations,
            evaluated_candidates,
            best_score,
        };
        if let Some(err) = error {
            entry.error = Some(err);
        }
    }
}
