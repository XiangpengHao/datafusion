// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, CommandGetDbSchemas, CommandGetTables,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    ProstMessageExt, SqlInfo,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use dashmap::DashMap;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::{Stream, TryStreamExt};
use log::{debug, info};
use mimalloc::MiMalloc;
use prost::Message;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use structopt::StructOpt;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(short, long)]
    path: PathBuf,

    #[structopt(long)]
    partitions: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder().format_timestamp(None).init();
    let options = Options::from_args();

    let file_name = options
        .path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string()
        .split('.')
        .next()
        .unwrap()
        .to_string();

    let addr = "0.0.0.0:50051".parse()?;

    let mut session_config = SessionConfig::from_env()
        .map_err(|e| Status::internal(format!("Error building plan: {e}")))?
        .with_information_schema(true)
        .with_target_partitions(options.partitions.unwrap_or(num_cpus::get()));

    session_config
        .options_mut()
        .execution
        .parquet
        .pushdown_filters = true;

    session_config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;

    let ctx = Arc::new(SessionContext::new_with_config(session_config));

    // register parquet file with the execution context
    ctx.register_parquet(
        &file_name,
        &options.path.to_string_lossy(),
        ParquetReadOptions::default(),
    )
    .await
    .map_err(|e| status!("Error registering table", e))?;

    let service = FlightSqlServiceImpl::new(file_name, options.path, ctx);
    info!("Listening on {addr:?}");
    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

pub struct FlightSqlServiceImpl {
    contexts: Arc<DashMap<String, Arc<SessionContext>>>,
    statements: Arc<DashMap<String, LogicalPlan>>,
    results: Arc<DashMap<String, Arc<dyn ExecutionPlan>>>,
    default_ctx: Arc<SessionContext>,
    table_name: String,
    table_path: String,
}

impl FlightSqlServiceImpl {
    fn new(table_name: String, path: PathBuf, default_ctx: Arc<SessionContext>) -> Self {
        Self {
            contexts: Default::default(),
            statements: Default::default(),
            results: Default::default(),
            table_name,
            table_path: path.to_string_lossy().to_string(),
            default_ctx,
        }
    }
}

impl FlightSqlServiceImpl {
    async fn create_ctx(&self) -> Result<String, Status> {
        let uuid = Uuid::new_v4().hyphenated().to_string();

        let session_config = SessionConfig::from_env()
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?
            .with_information_schema(true);
        let ctx = Arc::new(SessionContext::new_with_config(session_config));

        // register parquet file with the execution context
        ctx.register_parquet(
            &self.table_name,
            &self.table_path,
            ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| status!("Error registering table", e))?;

        self.contexts.insert(uuid.clone(), ctx);
        debug!("Created context with uuid: {uuid}");
        Ok(uuid)
    }

    fn get_ctx<T>(&self, req: &Request<T>) -> Result<Arc<SessionContext>, Status> {
        // get the token from the authorization header on Request
        if let Some(auth) = req.metadata().get("authorization") {
            let str = auth
                .to_str()
                .map_err(|e| Status::internal(format!("Error parsing header: {e}")))?;
            let authorization = str.to_string();
            let bearer = "Bearer ";
            if !authorization.starts_with(bearer) {
                Err(Status::internal("Invalid auth header!"))?;
            }
            let auth = authorization[bearer.len()..].to_string();

            if let Some(context) = self.contexts.get(&auth) {
                Ok(context.clone())
            } else {
                Err(Status::internal(format!(
                    "Context handle not found: {auth}"
                )))?
            }
        } else {
            Ok(self.default_ctx.clone())
        }
    }

    fn get_result(&self, handle: &str) -> Result<Arc<dyn ExecutionPlan>, Status> {
        if let Some(result) = self.results.get(handle) {
            Ok(result.clone())
        } else {
            Err(Status::internal(format!(
                "Request handle not found: {handle}"
            )))?
        }
    }

    fn remove_plan(&self, handle: &str) -> Result<(), Status> {
        self.statements.remove(&handle.to_string());
        Ok(())
    }

    fn remove_result(&self, handle: &str) -> Result<(), Status> {
        self.results.remove(&handle.to_string());
        Ok(())
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        info!("do_handshake");
        // no authentication actually takes place here
        // see Ballista implementation for example of basic auth
        // in this case, we simply accept the connection and create a new SessionContext
        // the SessionContext will be re-used within this same connection/session
        let token = self.create_ctx().await?;

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = self
            .default_ctx
            .table_provider(&self.table_name)
            .await
            .unwrap()
            .schema();

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .expect("encoding failed");
        Ok(Response::new(info))
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        if !message.is::<FetchResults>() {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }

        let fetch_results: FetchResults = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected FetchResults but got None!"))?;

        let handle = fetch_results.handle;

        debug!("getting results for {handle}");
        let execution_plan = self.get_result(&handle)?;

        let displayable =
            datafusion::physical_plan::display::DisplayableExecutionPlan::new(
                execution_plan.as_ref(),
            );
        debug!("physical plan:\n{}", displayable.indent(false));

        let ctx = self.get_ctx(&request)?;

        let schema = execution_plan.schema();

        let stream = execution_plan
            .execute(fetch_results.partition as usize, ctx.task_ctx())
            .map_err(|e| status!("Error executing plan", e))?
            .map_err(|e| {
                arrow_flight::error::FlightError::from_external_error(Box::new(e))
            });

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        cmd: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let user_query = cmd.query.as_str();
        info!("running query: {user_query}");

        let ctx = self.get_ctx(&request)?;

        let plan = ctx.sql(user_query).await.expect("Error generating plan");
        let (state, plan) = plan.into_parts();
        let plan = state.optimize(&plan).expect("Error optimizing plan");
        let physical_plan = state
            .create_physical_plan(&plan)
            .await
            .expect("Error creating physical plan");

        let partition_count = physical_plan.output_partitioning().partition_count();

        let schema = physical_plan.schema();

        let handle = Uuid::new_v4().hyphenated().to_string();
        self.results.insert(handle.clone(), physical_plan);

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };

        let mut info = FlightInfo::new()
            .try_with_schema(&schema)
            .expect("encoding failed")
            .with_descriptor(flight_desc);

        for partition in 0..partition_count {
            let fetch = FetchResults {
                handle: handle.clone(),
                partition: partition as u32,
            };
            let buf = fetch.as_any().encode_to_vec().into();
            let ticket = Ticket { ticket: buf };
            let endpoint = FlightEndpoint::new().with_ticket(ticket.clone());
            info = info.with_endpoint(endpoint);
        }

        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_prepared_statement(
        &self,
        _cmd: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_prepared_statement");
        panic!("not implemented");
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_tables");
        panic!("not implemented");
    }

    async fn do_put_prepared_statement_update(
        &self,
        _handle: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        // statements like "CREATE TABLE.." or "SET datafusion.nnn.." call this function
        // and we are required to return some row count here
        Ok(-1)
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let user_query = query.query.as_str();
        info!("do_action_create_prepared_statement: {user_query}");

        let ctx = self.get_ctx(&request)?;

        let plan = ctx
            .sql(user_query)
            .await
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;

        // store a copy of the plan,  it will be used for execution
        let plan_uuid = Uuid::new_v4().hyphenated().to_string();
        self.statements.insert(plan_uuid.clone(), plan.clone());

        let plan_schema = plan.schema();

        let arrow_schema = (&**plan_schema).into();
        let message = SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: plan_uuid.into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(),
        };
        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        handle: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let handle = std::str::from_utf8(&handle.prepared_statement_handle);
        if let Ok(handle) = handle {
            info!("do_action_close_prepared_statement: removing plan and results for {handle}");
            let _ = self.remove_plan(handle);
            let _ = self.remove_result(handle);
        }
        Ok(())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,

    #[prost(uint32, tag = "2")]
    pub partition: u32,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        "type.googleapis.com/datafusion.example.com.sql.FetchResults"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}
