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

use crate::error::{client_error, from_core_error};
use crate::fcore;
use crate::TOKIO_RUNTIME;
use napi_derive::napi;
use std::sync::Mutex;

/// Handle for a pending write operation.
/// Can be safely ignored for fire-and-forget semantics,
/// or awaited via `wait()` for per-record acknowledgment.
#[napi]
pub struct WriteResultHandle {
    inner: Mutex<Option<fcore::client::WriteResultFuture>>,
}

impl WriteResultHandle {
    pub fn new(future: fcore::client::WriteResultFuture) -> Self {
        Self {
            inner: Mutex::new(Some(future)),
        }
    }
}

#[napi]
impl WriteResultHandle {
    /// Wait for server acknowledgment of this specific write.
    #[napi]
    pub async fn wait(&self) -> napi::Result<()> {
        let future = self
            .inner
            .lock()
            .map_err(|e| client_error(format!("Lock poisoned: {e}")))?
            .take()
            .ok_or_else(|| client_error("WriteResultHandle already consumed"))?;

        TOKIO_RUNTIME
            .spawn(async move { future.await })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }
}
