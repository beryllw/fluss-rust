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

use std::env;
use std::error::Error;
use std::sync::Arc;

use fluss::client::FlussConnection;
use fluss::config::Config;

#[path = "support/demo_runner.rs"]
mod demo_runner;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let bootstrap_servers = parse_bootstrap_servers()?;

    let mut config = Config::default();
    config.bootstrap_servers = bootstrap_servers;
    let connection = Arc::new(FlussConnection::new(config).await?);

    let summary = demo_runner::run_demo(connection).await?;

    println!(
        "Connected to Fluss and registered catalog `{}`.",
        demo_runner::CATALOG
    );
    println!(
        "Seeded demo tables `{}` and `{}` in database `{}`.",
        demo_runner::KV_TABLE,
        demo_runner::LOG_TABLE,
        demo_runner::DATABASE
    );
    println!();

    println!(
        "KV lookup: SELECT id, name, age FROM {}.{}.{} WHERE id = 2",
        demo_runner::CATALOG,
        demo_runner::DATABASE,
        demo_runner::KV_TABLE
    );
    println!("Result: name={}, age={}", summary.kv_name, summary.kv_age);
    println!();

    println!(
        "Log bounded scan: SELECT id, action FROM {}.{}.{} LIMIT 3",
        demo_runner::CATALOG,
        demo_runner::DATABASE,
        demo_runner::LOG_TABLE
    );
    println!(
        "Result ids={:?}, actions={:?}",
        summary.log_ids, summary.log_actions
    );
    println!();

    println!(
        "EXPLAIN SELECT * FROM {}.{}.{} WHERE id = 2",
        demo_runner::CATALOG,
        demo_runner::DATABASE,
        demo_runner::KV_TABLE
    );
    println!("{}", summary.kv_explain.trim_end());
    println!();

    println!(
        "EXPLAIN SELECT * FROM {}.{}.{} LIMIT 3",
        demo_runner::CATALOG,
        demo_runner::DATABASE,
        demo_runner::LOG_TABLE
    );
    println!("{}", summary.log_explain.trim_end());

    Ok(())
}

fn parse_bootstrap_servers() -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut args = env::args().skip(1);
    let mut bootstrap_servers = "127.0.0.1:9123".to_string();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bootstrap-servers" => {
                bootstrap_servers = args.next().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "missing value for --bootstrap-servers",
                    )
                })?;
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}"),
                )
                .into());
            }
        }
    }

    Ok(bootstrap_servers)
}

fn print_help() {
    println!("Run a minimal fluss-datafusion demo against an external Fluss cluster.");
    println!();
    println!("Usage:");
    println!("  cargo run -p fluss-datafusion --example datafusion_demo -- [--bootstrap-servers HOST:PORT]");
    println!();
    println!("Options:");
    println!("  --bootstrap-servers HOST:PORT   Fluss bootstrap servers (default: 127.0.0.1:9123)");
}
