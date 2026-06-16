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

#[cfg(test)]
mod connection_io_handle_test {
    use crate::integration::utils::get_shared_cluster;
    use fluss::client::FlussConnection;
    use fluss::config::Config;
    use std::sync::Arc;
    use std::time::Duration;

    async fn list_database_count(connection: Arc<FlussConnection>, runtime_label: &str) -> usize {
        let admin = connection.get_admin().expect("should get admin");
        tokio::time::timeout(Duration::from_secs(10), admin.list_databases())
            .await
            .unwrap_or_else(|_| panic!("{runtime_label} request timed out"))
            .expect("should list databases over injected-I/O connection")
            .len()
    }

    fn list_database_count_on_fresh_runtime(
        connection: Arc<FlussConnection>,
        runtime_label: &str,
    ) -> usize {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build caller runtime");

        rt.block_on(list_database_count(connection, runtime_label))
    }

    /// Proves the injected-I/O connection survives the original failure mode:
    /// the same cached connection is first created/used from one caller runtime,
    /// then reused from a second caller runtime after the first runtime is
    /// dropped. Before the runtime-agnostic actor fix, splitting one cached
    /// connection across runtimes deadlocked deterministically; now the socket
    /// lives on the injected long-lived `host` runtime, so both caller runtimes
    /// can use it safely.
    #[test]
    fn connection_uses_injected_io_handle_across_caller_runtimes() {
        let cluster = get_shared_cluster();

        // A long-lived "host" runtime that owns the connection's socket I/O,
        // mimicking how a gateway would unify its I/O onto a single runtime.
        let host = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("host-injected-io")
            .build()
            .expect("build host I/O runtime");

        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: cluster.plaintext_bootstrap_servers().to_string(),
            ..Default::default()
        };

        // Caller runtime A: create the connection and execute the first RPC on
        // the SAME caller runtime that created it.
        let caller_a = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build first caller runtime");
        let host_handle = host.handle().clone();
        let (connection, first_count) = caller_a.block_on(async move {
            let connection = Arc::new(
                tokio::time::timeout(
                    Duration::from_secs(10),
                    FlussConnection::new_with_io_handle(config, host_handle),
                )
                .await
                .expect("connection construction on caller runtime A timed out")
                .expect("should connect with injected I/O handle"),
            );
            let first_count = list_database_count(Arc::clone(&connection), "caller runtime A").await;
            (connection, first_count)
        });
        drop(caller_a);

        // Caller runtime B: reuse the SAME connection from a different runtime.
        let connection_for_b = Arc::clone(&connection);
        let second_count = std::thread::spawn(move || {
            list_database_count_on_fresh_runtime(connection_for_b, "caller runtime B")
        })
        .join()
        .expect("caller runtime B thread should not panic");

        assert_eq!(
            first_count, second_count,
            "reusing the same connection across caller runtimes should succeed consistently"
        );

        drop(connection);
        host.shutdown_background();
    }
}
