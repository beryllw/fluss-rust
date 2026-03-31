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

use crate::error::client_error;
use crate::fcore;
use napi_derive::napi;
use std::collections::HashMap;

#[napi]
pub struct Config {
    pub(crate) inner: fcore::config::Config,
}

#[napi]
impl Config {
    #[napi(constructor)]
    pub fn new(options: Option<serde_json::Value>) -> napi::Result<Self> {
        let mut config = fcore::config::Config::default();

        if let Some(serde_json::Value::Object(map)) = options {
            for (key, value) in map {
                let val_str = match &value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => return Err(client_error(format!("Invalid value type for '{key}'"))),
                };
                Self::apply_property(&mut config, &key, &val_str)?;
            }
        }

        Ok(Self { inner: config })
    }

    /// Create a Config from a properties map (string key-value pairs).
    #[napi(factory)]
    pub fn from_properties(props: HashMap<String, String>) -> napi::Result<Self> {
        let mut config = fcore::config::Config::default();
        for (key, value) in &props {
            Self::apply_property(&mut config, key, value)?;
        }
        Ok(Self { inner: config })
    }

    // --- Getters ---
    #[napi(getter)]
    pub fn bootstrap_servers(&self) -> String {
        self.inner.bootstrap_servers.clone()
    }

    #[napi(setter)]
    pub fn set_bootstrap_servers(&mut self, value: String) {
        self.inner.bootstrap_servers = value;
    }

    #[napi(getter)]
    pub fn writer_acks(&self) -> String {
        self.inner.writer_acks.clone()
    }

    #[napi(setter)]
    pub fn set_writer_acks(&mut self, value: String) {
        self.inner.writer_acks = value;
    }

    #[napi(getter)]
    pub fn writer_retries(&self) -> i32 {
        self.inner.writer_retries
    }

    #[napi(setter)]
    pub fn set_writer_retries(&mut self, value: i32) {
        self.inner.writer_retries = value;
    }

    #[napi(getter)]
    pub fn writer_batch_size(&self) -> i32 {
        self.inner.writer_batch_size
    }

    #[napi(setter)]
    pub fn set_writer_batch_size(&mut self, value: i32) {
        self.inner.writer_batch_size = value;
    }

    #[napi(getter)]
    pub fn writer_enable_idempotence(&self) -> bool {
        self.inner.writer_enable_idempotence
    }

    #[napi(setter)]
    pub fn set_writer_enable_idempotence(&mut self, value: bool) {
        self.inner.writer_enable_idempotence = value;
    }

    #[napi(getter)]
    pub fn security_protocol(&self) -> String {
        self.inner.security_protocol.clone()
    }

    #[napi(setter)]
    pub fn set_security_protocol(&mut self, value: String) {
        self.inner.security_protocol = value;
    }

    #[napi(getter)]
    pub fn connect_timeout_ms(&self) -> f64 {
        self.inner.connect_timeout_ms as f64
    }

    #[napi(setter)]
    pub fn set_connect_timeout_ms(&mut self, value: f64) {
        self.inner.connect_timeout_ms = value as u64;
    }
}

impl Config {
    fn apply_property(config: &mut fcore::config::Config, key: &str, value: &str) -> napi::Result<()> {
        match key {
            "bootstrapServers" | "bootstrap.servers" => {
                config.bootstrap_servers = value.to_string();
            }
            "writerRequestMaxSize" | "writer.request-max-size" => {
                config.writer_request_max_size = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "writerAcks" | "writer.acks" => {
                config.writer_acks = value.to_string();
            }
            "writerRetries" | "writer.retries" => {
                config.writer_retries = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "writerBatchSize" | "writer.batch-size" => {
                config.writer_batch_size = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "writerBatchTimeoutMs" | "writer.batch-timeout-ms" => {
                config.writer_batch_timeout_ms = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "scannerRemoteLogPrefetchNum" | "scanner.remote-log.prefetch-num" => {
                config.scanner_remote_log_prefetch_num = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "remoteFileDownloadThreadNum" | "remote-file.download-thread-num" => {
                config.remote_file_download_thread_num = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "scannerRemoteLogReadConcurrency" | "scanner.remote-log.read-concurrency" => {
                config.scanner_remote_log_read_concurrency = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "scannerLogMaxPollRecords" | "scanner.log.max-poll-records" => {
                config.scanner_log_max_poll_records = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "scannerLogFetchMaxBytes" | "scanner.log.fetch.max-bytes" => {
                config.scanner_log_fetch_max_bytes = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "scannerLogFetchMinBytes" | "scanner.log.fetch.min-bytes" => {
                config.scanner_log_fetch_min_bytes = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "scannerLogFetchWaitMaxTimeMs" | "scanner.log.fetch.wait-max-time-ms" => {
                config.scanner_log_fetch_wait_max_time_ms = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "scannerLogFetchMaxBytesForBucket" | "scanner.log.fetch.max-bytes-for-bucket" => {
                config.scanner_log_fetch_max_bytes_for_bucket = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "writerEnableIdempotence" | "writer.enable-idempotence" => {
                config.writer_enable_idempotence = match value {
                    "true" => true,
                    "false" => false,
                    _ => return Err(client_error(format!("Invalid value '{value}' for '{key}', expected 'true' or 'false'"))),
                };
            }
            "writerMaxInflightRequestsPerBucket" | "writer.max-inflight-requests-per-bucket" => {
                config.writer_max_inflight_requests_per_bucket = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "writerBufferMemorySize" | "writer.buffer.memory-size" => {
                config.writer_buffer_memory_size = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "writerBufferWaitTimeoutMs" | "writer.buffer.wait-timeout-ms" => {
                config.writer_buffer_wait_timeout_ms = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "writerBucketNoKeyAssigner" | "writer.bucket.no-key-assigner" => {
                config.writer_bucket_no_key_assigner = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "connectTimeoutMs" | "connect-timeout" => {
                config.connect_timeout_ms = value.parse().map_err(|e| client_error(format!("Invalid value '{value}' for '{key}': {e}")))?;
            }
            "securityProtocol" | "security.protocol" => {
                config.security_protocol = value.to_string();
            }
            "securitySaslMechanism" | "security.sasl.mechanism" => {
                config.security_sasl_mechanism = value.to_string();
            }
            "securitySaslUsername" | "security.sasl.username" => {
                config.security_sasl_username = value.to_string();
            }
            "securitySaslPassword" | "security.sasl.password" => {
                config.security_sasl_password = value.to_string();
            }
            _ => {
                return Err(client_error(format!("Unknown property: {key}")));
            }
        }
        Ok(())
    }

    pub fn get_core_config(&self) -> fcore::config::Config {
        self.inner.clone()
    }
}
