use std::sync::Arc;

use rebar::gen_server::{GenServer, GenServerContext, GenServerRef, spawn_gen_server};
use rebar::process::ProcessId;
use rebar::runtime::Runtime;

use crate::s3::S3Object;

// --- Message types ---

#[derive(Debug)]
pub enum CoordinatorCall {
    GetTask,
    GetStats,
    GetProgress,
}

#[derive(Debug)]
pub enum CoordinatorCast {
    TaskComplete { bytes: u64 },
}

#[derive(Debug)]
pub enum CoordinatorReply {
    Task(Option<S3Object>),
    Stats { bytes: u64 },
    Progress { completed: u64, total: u64 },
}

// --- State ---

pub struct CoordinatorState {
    tasks: Vec<S3Object>,
    next_task: usize,
    completed: u64,
    total: u64,
    downloaded_bytes: u64,
}

// --- GenServer impl ---

pub struct Coordinator {
    objects: Vec<S3Object>,
}

impl Coordinator {
    pub fn new(objects: Vec<S3Object>) -> Self {
        Self { objects }
    }
}

#[async_trait::async_trait]
impl GenServer for Coordinator {
    type State = CoordinatorState;
    type Call = CoordinatorCall;
    type Cast = CoordinatorCast;
    type Reply = CoordinatorReply;

    async fn init(&self, _ctx: &GenServerContext) -> Result<Self::State, String> {
        let total = self.objects.len() as u64;
        Ok(CoordinatorState {
            tasks: self.objects.clone(),
            next_task: 0,
            completed: 0,
            total,
            downloaded_bytes: 0,
        })
    }

    async fn handle_call(
        &self,
        msg: Self::Call,
        _from: ProcessId,
        state: &mut Self::State,
        _ctx: &GenServerContext,
    ) -> Self::Reply {
        match msg {
            CoordinatorCall::GetTask => {
                if state.next_task < state.tasks.len() {
                    let obj = state.tasks[state.next_task].clone();
                    state.next_task += 1;
                    CoordinatorReply::Task(Some(obj))
                } else {
                    CoordinatorReply::Task(None)
                }
            }
            CoordinatorCall::GetStats => {
                let bytes = state.downloaded_bytes;
                state.downloaded_bytes = 0;
                CoordinatorReply::Stats { bytes }
            }
            CoordinatorCall::GetProgress => CoordinatorReply::Progress {
                completed: state.completed,
                total: state.total,
            },
        }
    }

    async fn handle_cast(
        &self,
        msg: Self::Cast,
        state: &mut Self::State,
        _ctx: &GenServerContext,
    ) {
        match msg {
            CoordinatorCast::TaskComplete { bytes } => {
                state.completed += 1;
                state.downloaded_bytes += bytes;
            }
        }
    }
}

pub async fn start_coordinator(
    runtime: Arc<Runtime>,
    objects: Vec<S3Object>,
) -> GenServerRef<Coordinator> {
    spawn_gen_server(runtime, Coordinator::new(objects)).await
}
