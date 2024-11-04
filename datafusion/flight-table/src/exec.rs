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

//! Execution plan for reading flights from Arrow Flight services

use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use crate::table::{flight_channel, to_df_err, FlightMetadata, FlightProperties};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightClient, FlightEndpoint, Ticket};
use arrow_schema::SchemaRef;
use datafusion::config::ConfigOptions;
use datafusion_common::arrow::datatypes::ToByteSlice;
use datafusion_common::Result;
use datafusion_common::{project_schema, DataFusionError};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tonic::metadata::{AsciiMetadataKey, MetadataMap};

/// Arrow Flight physical plan that maps flight endpoints to partitions
#[derive(Clone, Debug)]
pub(crate) struct FlightExec {
    config: FlightConfig,
    plan_properties: PlanProperties,
    metadata_map: Arc<MetadataMap>,
}

impl FlightExec {
    /// Creates a FlightExec with the provided [FlightMetadata]
    /// and origin URL (used as fallback location as per the protocol spec).
    pub fn try_new(
        logical_schema: SchemaRef,
        metadata: FlightMetadata,
        projection: Option<&Vec<usize>>,
        origin: &str,
        limit: Option<usize>,
    ) -> Result<Self> {
        let partitions = metadata
            .info
            .endpoint
            .iter()
            .map(|endpoint| FlightPartition::new(endpoint, origin.to_string()))
            .collect();
        let schema =
            project_schema(&logical_schema, projection).expect("Error projecting schema");
        let config = FlightConfig {
            origin: origin.into(),
            schema,
            partitions,
            properties: metadata.props,
            limit,
        };
        let exec_mode = if config.properties.unbounded_stream {
            ExecutionMode::Unbounded
        } else {
            ExecutionMode::Bounded
        };
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(config.schema.clone()),
            Partitioning::UnknownPartitioning(config.partitions.len()),
            exec_mode,
        );
        let mut mm = MetadataMap::new();
        for (k, v) in config.properties.grpc_headers.iter() {
            let key =
                AsciiMetadataKey::from_str(k.as_str()).expect("invalid header name");
            let value = v.parse().expect("invalid header value");
            mm.insert(key, value);
        }
        Ok(Self {
            config,
            plan_properties,
            metadata_map: Arc::from(mm),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FlightConfig {
    origin: String,
    schema: SchemaRef,
    partitions: Arc<[FlightPartition]>,
    properties: FlightProperties,
    limit: Option<usize>,
}

/// The minimum information required for fetching a flight stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FlightPartition {
    locations: Arc<[String]>,
    ticket: FlightTicket,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
struct FlightTicket(Arc<[u8]>);

impl From<Option<&Ticket>> for FlightTicket {
    fn from(ticket: Option<&Ticket>) -> Self {
        let bytes = match ticket {
            Some(t) => t.ticket.to_byte_slice().into(),
            None => [].into(),
        };
        Self(bytes)
    }
}

impl Debug for FlightTicket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[..{} bytes..]", self.0.len())
    }
}

impl FlightPartition {
    fn new(endpoint: &FlightEndpoint, fallback_location: String) -> Self {
        let locations = if endpoint.location.is_empty() {
            [fallback_location].into()
        } else {
            endpoint
                .location
                .iter()
                .map(|loc| {
                    if loc.uri.starts_with("arrow-flight-reuse-connection://") {
                        fallback_location.clone()
                    } else {
                        loc.uri.clone()
                    }
                })
                .collect()
        };

        Self {
            locations,
            ticket: endpoint.ticket.as_ref().into(),
        }
    }
}

async fn flight_client(
    source: impl Into<String>,
    grpc_headers: &MetadataMap,
) -> Result<FlightClient> {
    let channel = flight_channel(source).await?;
    let inner_client = FlightServiceClient::new(channel);
    let mut client = FlightClient::new_from_inner(inner_client);
    client.metadata_mut().clone_from(grpc_headers);
    Ok(client)
}

async fn flight_stream(
    partition: FlightPartition,
    schema: SchemaRef,
    grpc_headers: Arc<MetadataMap>,
) -> Result<SendableRecordBatchStream> {
    let mut errors: Vec<Box<dyn Error + Send + Sync>> = vec![];
    for loc in partition.locations.iter() {
        let client = flight_client(loc, grpc_headers.as_ref()).await?;
        match try_fetch_stream(client, &partition.ticket, schema.clone()).await {
            Ok(stream) => return Ok(stream),
            Err(e) => errors.push(Box::new(e)),
        }
    }
    let err = errors.into_iter().last().unwrap_or_else(|| {
        Box::new(FlightError::ProtocolError(format!(
            "No available location for endpoint {:?}",
            partition.locations
        )))
    });
    Err(DataFusionError::External(err))
}

async fn try_fetch_stream(
    mut client: FlightClient,
    ticket: &FlightTicket,
    schema: SchemaRef,
) -> arrow_flight::error::Result<SendableRecordBatchStream> {
    let ticket = Ticket::new(ticket.0.to_vec());
    let stream = client.do_get(ticket).await?.map_err(to_df_err);
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream,
    )))
}

impl DisplayAs for FlightExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(
                f,
                "FlightExec: origin={}, streams={}",
                self.config.origin,
                self.config.partitions.len()
            ),
            DisplayFormatType::Verbose => write!(
                f,
                "FlightExec: origin={}, partitions={:?}, properties={:?}",
                self.config.origin, self.config.partitions, self.config.properties,
            ),
        }
    }
}

impl ExecutionPlan for FlightExec {
    fn name(&self) -> &str {
        "FlightExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let future_stream = flight_stream(
            self.config.partitions[partition].clone(),
            self.schema(),
            self.metadata_map.clone(),
        );
        let stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn fetch(&self) -> Option<usize> {
        self.config.limit
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let mut new_plan = self.clone();
        new_plan.plan_properties.partitioning =
            Partitioning::UnknownPartitioning(self.config.partitions.len());
        Ok(Some(Arc::new(new_plan)))
    }
}
