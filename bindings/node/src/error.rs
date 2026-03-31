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

use crate::fcore;

const CLIENT_ERROR_CODE: i32 = -2;

/// Convert a core fluss Error to a napi::Error.
pub fn from_core_error(error: &fcore::error::Error) -> napi::Error {
    use fcore::error::Error;
    let (msg, code) = match error {
        Error::FlussAPIError { api_error } => (api_error.message.clone(), api_error.code),
        _ => (error.to_string(), CLIENT_ERROR_CODE),
    };
    napi::Error::new(
        napi::Status::GenericFailure,
        format!("[FlussError code={code}] {msg}"),
    )
}

/// Create a napi::Error from a message string (client-side error).
pub fn client_error(message: impl std::fmt::Display) -> napi::Error {
    napi::Error::new(
        napi::Status::GenericFailure,
        format!("[FlussError code={CLIENT_ERROR_CODE}] {message}"),
    )
}
