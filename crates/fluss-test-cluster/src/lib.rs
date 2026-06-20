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

use fluss::client::FlussConnection;
use fluss::config::Config;
use std::collections::HashMap;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::bollard::Docker;
use testcontainers::bollard::query_parameters::{
    ListContainersOptionsBuilder, RemoveContainerOptionsBuilder,
};
use testcontainers::core::client::docker_client_instance;
use testcontainers::core::{ContainerPort, ExecCommand, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Returns the exact Docker client `testcontainers` uses to start containers.
///
/// `docker_client_instance` hands back testcontainers' own lazily-created, shared
/// `bollard::Docker`, configured by testcontainers' full daemon resolution
/// (`DOCKER_HOST`, the docker context, `~/.testcontainers.properties`, ...). Routing
/// teardown/existence checks through it — rather than constructing a parallel client
/// or shelling out to a `docker`/`podman` CLI — guarantees `start` and `stop` act on
/// one daemon. `None` (with a warning) when the client cannot be reached.
async fn docker_client() -> Option<Docker> {
    match docker_client_instance().await {
        Ok(docker) => Some(docker),
        Err(e) => {
            eprintln!("warning: cannot reach the testcontainers Docker client: {e}");
            None
        }
    }
}

/// Force-removes one container by name, ignoring a "no such container" miss.
///
/// Teardown is best-effort: an absent container is success, so the 404 from the
/// daemon is swallowed. Other errors are surfaced as warnings rather than panics.
async fn force_remove_container(docker: &Docker, name: &str) {
    let options = RemoveContainerOptionsBuilder::default().force(true).build();
    match docker.remove_container(name, Some(options)).await {
        Ok(())
        | Err(testcontainers::bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }) => {}
        Err(e) => eprintln!("warning: failed to remove container '{name}': {e}"),
    }
}

/// Drives an async teardown future to completion from a synchronous caller.
///
/// `stop()` is called both from inside a tokio runtime (async integration tests)
/// and from a plain atexit handler with no runtime, so blocking the current thread
/// is unsafe. Running on a dedicated thread with its own current-thread runtime
/// works in both cases.
fn run_blocking<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for container teardown")
            .block_on(future)
    })
    .join()
    .expect("container teardown thread panicked")
}

pub const FLUSS_IMAGE: &str = env!("FLUSS_IMAGE");
pub const FLUSS_VERSION: &str = env!("FLUSS_VERSION");
pub const ZOOKEEPER_IMAGE: &str = env!("ZOOKEEPER_IMAGE");
pub const ZOOKEEPER_VERSION: &str = env!("ZOOKEEPER_VERSION");
pub const RUSTFS_IMAGE: &str = env!("RUSTFS_IMAGE");
pub const RUSTFS_VERSION: &str = env!("RUSTFS_VERSION");
pub const MC_IMAGE: &str = env!("MC_IMAGE");
pub const MC_VERSION: &str = env!("MC_VERSION");
pub const FLUSS_FLINK_IMAGE: &str = env!("FLUSS_FLINK_IMAGE");
pub const FLUSS_FLINK_VERSION: &str = env!("FLUSS_FLINK_VERSION");
pub const PAIMON_S3_VERSION: &str = env!("PAIMON_S3_VERSION");

/// In-network S3 port RustFS binds (same inside the container network for every
/// cluster; the host-facing port is mapped dynamically).
const RUSTFS_S3_PORT: u16 = 9000;

/// Dedicated in-network client listener port on the Fluss servers, used by the
/// Flink tiering job. The default `CLIENT` listener advertises `localhost` for
/// the host test process, which an in-network client cannot follow; this second
/// listener advertises the container hostname instead. Defaults to PLAINTEXT
/// since it is absent from `security.protocol.map`.
const NETWORK_LISTENER_PORT: u16 = 9900;

/// Cluster-level lakehouse configuration: a RustFS (S3-compatible) store shared
/// by the Fluss servers, the Flink tiering job, and the in-process Paimon
/// reader. Mirrors the official Fluss lakehouse quickstart.
#[derive(Clone, Debug)]
pub struct PaimonLakeConfig {
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub warehouse: String,
    /// Host path to the `paimon-s3-<ver>.jar` mounted into the Fluss servers'
    /// `/opt/fluss/plugins/paimon/` so they can read/write the S3 warehouse.
    pub paimon_s3_plugin_jar: std::path::PathBuf,
}

impl PaimonLakeConfig {
    /// Quickstart defaults (`rustfsadmin` credentials, bucket `fluss`, warehouse
    /// `s3://fluss/paimon`), with the `paimon-s3` plugin jar provisioned
    /// automatically by [`ensure_paimon_s3_jar`]. This is the harness's stable
    /// entry point for tiering tests — callers no longer manage the jar.
    pub fn new() -> Self {
        Self::with_plugin_jar(ensure_paimon_s3_jar())
    }

    /// Like [`Self::new`] but with an explicit `paimon-s3-<ver>.jar` path
    /// (see [`PAIMON_S3_VERSION`]), for callers that vendor the jar themselves.
    pub fn with_plugin_jar(paimon_s3_plugin_jar: impl Into<std::path::PathBuf>) -> Self {
        Self {
            access_key: "rustfsadmin".to_string(),
            secret_key: "rustfsadmin".to_string(),
            bucket: "fluss".to_string(),
            warehouse: "s3://fluss/paimon".to_string(),
            paimon_s3_plugin_jar: paimon_s3_plugin_jar.into(),
        }
    }
}

impl Default for PaimonLakeConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Ensures the `paimon-s3-<ver>.jar` (the plugin the Fluss servers mount to read
/// and write the S3 lake warehouse) is available locally, returning its path.
///
/// The jar is cached under the OS temp dir and downloaded once via `curl` from
/// Maven Central. Set `PAIMON_S3_JAR` to an absolute path to use a vendored jar
/// instead (e.g. air-gapped CI). Owned by the harness so tiering tests don't each
/// reimplement download/caching.
pub fn ensure_paimon_s3_jar() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("PAIMON_S3_JAR") {
        let path = std::path::PathBuf::from(path);
        assert!(
            path.exists(),
            "PAIMON_S3_JAR points at a missing file: {}",
            path.display()
        );
        return path;
    }

    let dir = std::env::temp_dir().join("fluss-test-cluster");
    std::fs::create_dir_all(&dir).expect("failed to create paimon-s3 jar cache dir");
    let jar = dir.join(format!("paimon-s3-{PAIMON_S3_VERSION}.jar"));
    if !jar.exists() {
        let url = format!(
            "https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/{PAIMON_S3_VERSION}/paimon-s3-{PAIMON_S3_VERSION}.jar"
        );
        // Download to a per-process temp path and atomically rename on success:
        // an interrupted run cannot leave a truncated jar that a later run reuses,
        // and two concurrent downloads (separate test binaries on a cold cache)
        // never write the same temp file.
        let part = dir.join(format!(
            "paimon-s3-{PAIMON_S3_VERSION}.jar.{}.part",
            std::process::id()
        ));
        let status = std::process::Command::new("curl")
            .args(["-fL", "-o", part.to_str().unwrap(), &url])
            .status()
            .expect("failed to run curl to download paimon-s3 jar");
        assert!(
            status.success(),
            "failed to download paimon-s3 jar from {url}"
        );
        std::fs::rename(&part, &jar).expect("failed to finalize paimon-s3 jar download");
    }
    jar
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ClusterInfo {
    pub bootstrap_servers: String,
    pub sasl_bootstrap_servers: Option<String>,
}

pub struct FlussTestingClusterBuilder {
    number_of_tablet_servers: u16,
    network: &'static str,
    cluster_conf: HashMap<String, String>,
    testing_name: String,
    remote_data_dir: Option<std::path::PathBuf>,
    sasl_enabled: bool,
    sasl_users: Vec<(String, String)>,
    coordinator_host_port: u16,
    plain_client_port: Option<u16>,
    image: String,
    image_tag: String,
    paimon_lake: Option<PaimonLakeConfig>,
    tiering_service: bool,
}

impl FlussTestingClusterBuilder {
    pub fn new(testing_name: impl Into<String>) -> Self {
        Self::new_with_cluster_conf(testing_name.into(), &HashMap::default())
    }

    pub fn with_remote_data_dir(mut self, dir: std::path::PathBuf) -> Self {
        std::fs::create_dir_all(&dir).expect("Failed to create remote data directory");
        self.remote_data_dir = Some(dir);
        self
    }

    pub fn with_sasl(mut self, users: Vec<(String, String)>) -> Self {
        self.sasl_enabled = true;
        self.sasl_users = users;
        self.plain_client_port = Some(self.coordinator_host_port + 100);
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.coordinator_host_port = port;
        // Re-derive SASL port if SASL was already enabled.
        if self.sasl_enabled {
            self.plain_client_port = Some(port + 100);
        }
        self
    }

    pub fn new_with_cluster_conf(
        testing_name: impl Into<String>,
        conf: &HashMap<String, String>,
    ) -> Self {
        let mut cluster_conf = conf.clone();
        cluster_conf.insert(
            "netty.server.num-network-threads".to_string(),
            "1".to_string(),
        );
        cluster_conf.insert(
            "netty.server.num-worker-threads".to_string(),
            "3".to_string(),
        );

        FlussTestingClusterBuilder {
            number_of_tablet_servers: 1,
            cluster_conf,
            network: "fluss-cluster-network",
            testing_name: testing_name.into(),
            remote_data_dir: None,
            sasl_enabled: false,
            sasl_users: Vec::new(),
            coordinator_host_port: 9123,
            plain_client_port: None,
            image: FLUSS_IMAGE.to_string(),
            image_tag: FLUSS_VERSION.to_string(),
            paimon_lake: None,
            tiering_service: false,
        }
    }

    /// Enables Paimon lakehouse storage: starts a RustFS (S3) store, creates the
    /// bucket, injects cluster-level `datalake.*` / `s3.*` config into the Fluss
    /// servers, and mounts the `paimon-s3` plugin. The lake warehouse lives on
    /// `s3://<bucket>/paimon`, derived by the server into each lake table's
    /// properties.
    pub fn with_paimon_lake(mut self, cfg: PaimonLakeConfig) -> Self {
        self.paimon_lake = Some(cfg);
        self
    }

    /// Enables the tiering service: starts a Flink job/task manager on the
    /// cluster network so [`FlussTestingCluster::start_paimon_tiering`] can
    /// submit the real tiering job. Requires [`Self::with_paimon_lake`].
    pub fn with_tiering_service(mut self) -> Self {
        self.tiering_service = true;
        self
    }

    fn tablet_server_container_name(&self, server_id: u16) -> String {
        format!("tablet-server-{}-{}", self.testing_name, server_id)
    }

    fn coordinator_server_container_name(&self) -> String {
        format!("coordinator-server-{}", self.testing_name)
    }

    fn zookeeper_container_name(&self) -> String {
        format!("zookeeper-{}", self.testing_name)
    }

    fn rustfs_container_name(&self) -> String {
        format!("rustfs-{}", self.testing_name)
    }

    fn mc_init_container_name(&self) -> String {
        format!("mc-init-{}", self.testing_name)
    }

    fn jobmanager_container_name(&self) -> String {
        format!("jobmanager-{}", self.testing_name)
    }

    fn taskmanager_container_name(&self) -> String {
        format!("taskmanager-{}", self.testing_name)
    }

    fn container_names(&self) -> Vec<String> {
        let mut names: Vec<String> = std::iter::once(self.zookeeper_container_name())
            .chain(std::iter::once(self.coordinator_server_container_name()))
            .chain(
                (0..self.number_of_tablet_servers).map(|id| self.tablet_server_container_name(id)),
            )
            .collect();
        if self.paimon_lake.is_some() {
            names.push(self.rustfs_container_name());
            names.push(self.mc_init_container_name());
        }
        if self.tiering_service {
            names.push(self.jobmanager_container_name());
            names.push(self.taskmanager_container_name());
        }
        names
    }

    /// Injects cluster-level lakehouse config (Fluss remote storage on S3 +
    /// Paimon datalake catalog) into `cluster_conf` so both the coordinator and
    /// every tablet server receive it. The S3 endpoint is the in-network RustFS
    /// hostname; the host-facing endpoint is exposed separately on the cluster.
    fn inject_datalake_conf(&mut self) {
        let Some(lake) = self.paimon_lake.clone() else {
            return;
        };
        let endpoint = format!("http://{}:{}", self.rustfs_container_name(), RUSTFS_S3_PORT);
        let entries = [
            (
                "remote.data.dir",
                format!("s3://{}/remote-data", lake.bucket),
            ),
            ("s3.endpoint", endpoint.clone()),
            ("s3.access-key", lake.access_key.clone()),
            ("s3.secret-key", lake.secret_key.clone()),
            ("s3.region", "us-east-1".to_string()),
            ("s3.path-style-access", "true".to_string()),
            ("datalake.enabled", "true".to_string()),
            ("datalake.format", "paimon".to_string()),
            ("datalake.paimon.metastore", "filesystem".to_string()),
            ("datalake.paimon.warehouse", lake.warehouse.clone()),
            ("datalake.paimon.s3.endpoint", endpoint),
            ("datalake.paimon.s3.access-key", lake.access_key.clone()),
            ("datalake.paimon.s3.secret-key", lake.secret_key.clone()),
            ("datalake.paimon.s3.path.style.access", "true".to_string()),
        ];
        for (k, v) in entries {
            self.cluster_conf.entry(k.to_string()).or_insert(v);
        }
    }

    fn inject_sasl_conf(&mut self) {
        if self.sasl_enabled
            && !self.sasl_users.is_empty()
            && !self.cluster_conf.contains_key("security.protocol.map")
        {
            self.cluster_conf.insert(
                "security.protocol.map".to_string(),
                "CLIENT:sasl".to_string(),
            );
            self.cluster_conf.insert(
                "security.sasl.enabled.mechanisms".to_string(),
                "plain".to_string(),
            );
            let user_entries: Vec<String> = self
                .sasl_users
                .iter()
                .map(|(u, p)| format!("user_{}=\"{}\"", u, p))
                .collect();
            let jaas_config = format!(
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required {};",
                user_entries.join(" ")
            );
            self.cluster_conf
                .insert("security.sasl.plain.jaas.config".to_string(), jaas_config);
        }
    }

    fn bootstrap_addresses(&self) -> (String, Option<String>) {
        if let Some(plain_port) = self.plain_client_port {
            (
                format!("127.0.0.1:{}", plain_port),
                Some(format!("127.0.0.1:{}", self.coordinator_host_port)),
            )
        } else {
            (format!("127.0.0.1:{}", self.coordinator_host_port), None)
        }
    }

    async fn all_containers_exist(&self) -> bool {
        let Some(docker) = docker_client().await else {
            return false;
        };
        for name in self.container_names() {
            // Anchored name filter for an exact match; `all(false)` => running only,
            // matching the original `docker ps -q` (no `-a`).
            let mut filters = HashMap::new();
            filters.insert("name".to_string(), vec![format!("^{name}$")]);
            let options = ListContainersOptionsBuilder::default()
                .all(false)
                .filters(&filters)
                .build();
            match docker.list_containers(Some(options)).await {
                Ok(list) if !list.is_empty() => continue,
                _ => return false,
            }
        }
        true
    }

    async fn start_all_containers(&mut self) -> StartedCluster {
        if let Some(docker) = docker_client().await {
            for name in self.container_names() {
                force_remove_container(&docker, &name).await;
            }
        }
        self.inject_sasl_conf();
        self.inject_datalake_conf();

        let mut all: Vec<Arc<ContainerAsync<GenericImage>>> = Vec::new();
        all.push(Arc::new(self.start_zookeeper().await));

        // Lake store must be up (and its bucket created) before the Fluss servers
        // start, since they validate the S3 remote.data.dir on boot.
        let mut s3_host_endpoint = None;
        if self.paimon_lake.is_some() {
            let rustfs = self.start_rustfs().await;
            let host_port = rustfs
                .get_host_port_ipv4(ContainerPort::Tcp(RUSTFS_S3_PORT))
                .await
                .expect("failed to resolve RustFS host port");
            s3_host_endpoint = Some(format!("http://127.0.0.1:{host_port}"));
            all.push(Arc::new(rustfs));
            all.push(Arc::new(self.start_mc_init().await));
        }

        all.push(Arc::new(self.start_coordinator_server().await));
        for server_id in 0..self.number_of_tablet_servers {
            all.push(Arc::new(self.start_tablet_server(server_id).await));
        }

        let mut jobmanager = None;
        if self.tiering_service {
            let jm = Arc::new(self.start_jobmanager().await);
            all.push(jm.clone());
            all.push(Arc::new(self.start_taskmanager().await));
            jobmanager = Some(jm);
        }

        StartedCluster {
            all,
            jobmanager,
            s3_host_endpoint,
        }
    }

    /// Containers stop when the returned struct is dropped.
    pub async fn build(&mut self) -> FlussTestingCluster {
        let container_names = self.container_names();
        let started = self.start_all_containers().await;
        let (bootstrap_servers, sasl_bootstrap_servers) = self.bootstrap_addresses();

        FlussTestingCluster {
            containers: started.all,
            jobmanager: started.jobmanager,
            s3_host_endpoint: started.s3_host_endpoint,
            paimon_lake: self.paimon_lake.clone(),
            testing_name: self.testing_name.clone(),
            bootstrap_servers,
            sasl_bootstrap_servers,
            remote_data_dir: self.remote_data_dir.clone(),
            sasl_users: self.sasl_users.clone(),
            container_names,
        }
    }

    /// Containers outlive the process. Clean up via `stop_cluster()`.
    /// Idempotent: if the cluster is already running, returns its info.
    pub async fn build_detached(&mut self) -> ClusterInfo {
        if !self.all_containers_exist().await {
            let started = self.start_all_containers().await;
            let _ = ManuallyDrop::new(started);
        }

        let (bootstrap_servers, sasl_bootstrap_servers) = self.bootstrap_addresses();
        ClusterInfo {
            bootstrap_servers,
            sasl_bootstrap_servers,
        }
    }

    async fn start_zookeeper(&self) -> ContainerAsync<GenericImage> {
        GenericImage::new(ZOOKEEPER_IMAGE, ZOOKEEPER_VERSION)
            .with_network(self.network)
            .with_container_name(self.zookeeper_container_name())
            .start()
            .await
            .unwrap()
    }

    async fn start_coordinator_server(&mut self) -> ContainerAsync<GenericImage> {
        let port = self.coordinator_host_port;
        let container_name = self.coordinator_server_container_name();
        let mut coordinator_confs = HashMap::new();
        coordinator_confs.insert(
            "zookeeper.address",
            format!("{}:2181", self.zookeeper_container_name()),
        );

        if let Some(plain_port) = self.plain_client_port {
            coordinator_confs.insert(
                "bind.listeners",
                format!(
                    "INTERNAL://{}:0, CLIENT://{}:{}, PLAIN_CLIENT://{}:{}",
                    container_name, container_name, port, container_name, plain_port
                ),
            );
            coordinator_confs.insert(
                "advertised.listeners",
                format!(
                    "CLIENT://localhost:{}, PLAIN_CLIENT://localhost:{}",
                    port, plain_port
                ),
            );
        } else {
            coordinator_confs.insert(
                "bind.listeners",
                format!(
                    "INTERNAL://{}:0, CLIENT://{}:{}",
                    container_name, container_name, port
                ),
            );
            coordinator_confs.insert(
                "advertised.listeners",
                format!("CLIENT://localhost:{}", port),
            );
        }

        coordinator_confs.insert("internal.listener.name", "INTERNAL".to_string());
        self.append_network_listener(&mut coordinator_confs, &container_name);

        let mut image = GenericImage::new(&self.image, &self.image_tag)
            .with_container_name(self.coordinator_server_container_name())
            .with_mapped_port(port, ContainerPort::Tcp(port))
            .with_network(self.network)
            .with_cmd(vec!["coordinatorServer"])
            .with_env_var(
                "FLUSS_PROPERTIES",
                self.to_fluss_properties_with(coordinator_confs),
            );

        if let Some(plain_port) = self.plain_client_port {
            image = image.with_mapped_port(plain_port, ContainerPort::Tcp(plain_port));
        }
        if let Some(mount) = self.paimon_s3_plugin_mount() {
            image = image.with_mount(mount);
        }

        image.start().await.unwrap()
    }

    async fn start_tablet_server(&self, server_id: u16) -> ContainerAsync<GenericImage> {
        let port = self.coordinator_host_port;
        let container_name = self.tablet_server_container_name(server_id);
        let mut tablet_server_confs = HashMap::new();
        let expose_host_port = port + 1 + server_id;
        let tablet_server_id = format!("{}", server_id);

        if let Some(plain_port) = self.plain_client_port {
            let bind_listeners = format!(
                "INTERNAL://{}:0, CLIENT://{}:{}, PLAIN_CLIENT://{}:{}",
                container_name, container_name, port, container_name, plain_port,
            );
            let plain_expose_host_port = plain_port + 1 + server_id;
            let advertised_listeners = format!(
                "CLIENT://localhost:{}, PLAIN_CLIENT://localhost:{}",
                expose_host_port, plain_expose_host_port
            );
            tablet_server_confs.insert("bind.listeners", bind_listeners);
            tablet_server_confs.insert("advertised.listeners", advertised_listeners);
        } else {
            let bind_listeners = format!(
                "INTERNAL://{}:0, CLIENT://{}:{}",
                container_name, container_name, port,
            );
            let advertised_listeners = format!("CLIENT://localhost:{}", expose_host_port);
            tablet_server_confs.insert("bind.listeners", bind_listeners);
            tablet_server_confs.insert("advertised.listeners", advertised_listeners);
        }

        tablet_server_confs.insert(
            "zookeeper.address",
            format!("{}:2181", self.zookeeper_container_name()),
        );
        tablet_server_confs.insert("internal.listener.name", "INTERNAL".to_string());
        tablet_server_confs.insert("tablet-server.id", tablet_server_id);
        self.append_network_listener(&mut tablet_server_confs, &container_name);

        if let Some(remote_data_dir) = &self.remote_data_dir {
            tablet_server_confs.insert(
                "remote.data.dir",
                remote_data_dir.to_string_lossy().to_string(),
            );
        }
        let mut image = GenericImage::new(&self.image, &self.image_tag)
            .with_cmd(vec!["tabletServer"])
            .with_mapped_port(expose_host_port, ContainerPort::Tcp(port))
            .with_network(self.network)
            .with_container_name(self.tablet_server_container_name(server_id))
            .with_env_var(
                "FLUSS_PROPERTIES",
                self.to_fluss_properties_with(tablet_server_confs),
            );

        if let Some(plain_port) = self.plain_client_port {
            let plain_expose_host_port = plain_port + 1 + server_id;
            image = image.with_mapped_port(plain_expose_host_port, ContainerPort::Tcp(plain_port));
        }

        if let Some(ref remote_data_dir) = self.remote_data_dir {
            std::fs::create_dir_all(remote_data_dir)
                .expect("Failed to create remote data directory for mount");
            let host_path = remote_data_dir.to_string_lossy().to_string();
            let container_path = remote_data_dir.to_string_lossy().to_string();
            image = image.with_mount(Mount::bind_mount(host_path, container_path));
        }
        if let Some(mount) = self.paimon_s3_plugin_mount() {
            image = image.with_mount(mount);
        }

        image.start().await.unwrap()
    }

    /// Appends the in-network `NETWORK` client listener (advertised by container
    /// hostname) to a server's bind/advertised listener lists when the tiering
    /// service is enabled. The Flink tiering job connects through it because the
    /// default `CLIENT` listener advertises `localhost` for the host process.
    fn append_network_listener(&self, confs: &mut HashMap<&str, String>, container_name: &str) {
        if !self.tiering_service {
            return;
        }
        let net = format!("NETWORK://{}:{}", container_name, NETWORK_LISTENER_PORT);
        if let Some(bind) = confs.get_mut("bind.listeners") {
            bind.push_str(&format!(", {net}"));
        }
        if let Some(adv) = confs.get_mut("advertised.listeners") {
            adv.push_str(&format!(", {net}"));
        }
    }

    /// Bind-mount for the `paimon-s3` plugin jar into the Fluss servers'
    /// `/opt/fluss/plugins/paimon/` (lake mode only), so they can read/write the
    /// S3 warehouse.
    fn paimon_s3_plugin_mount(&self) -> Option<Mount> {
        let lake = self.paimon_lake.as_ref()?;
        let jar = &lake.paimon_s3_plugin_jar;
        let file_name = jar
            .file_name()
            .expect("paimon_s3_plugin_jar must be a file path")
            .to_string_lossy()
            .to_string();
        let host_path = jar.to_string_lossy().to_string();
        let container_path = format!("/opt/fluss/plugins/paimon/{file_name}");
        Some(Mount::bind_mount(host_path, container_path))
    }

    async fn start_rustfs(&self) -> ContainerAsync<GenericImage> {
        let lake = self
            .paimon_lake
            .as_ref()
            .expect("start_rustfs called without paimon_lake");
        GenericImage::new(RUSTFS_IMAGE, RUSTFS_VERSION)
            .with_exposed_port(ContainerPort::Tcp(RUSTFS_S3_PORT))
            .with_network(self.network)
            .with_container_name(self.rustfs_container_name())
            .with_env_var("RUSTFS_ACCESS_KEY", &lake.access_key)
            .with_env_var("RUSTFS_SECRET_KEY", &lake.secret_key)
            .with_env_var("RUSTFS_CONSOLE_ENABLE", "false")
            .with_cmd(vec!["/data"])
            .start()
            .await
            .unwrap()
    }

    async fn start_mc_init(&self) -> ContainerAsync<GenericImage> {
        let lake = self
            .paimon_lake
            .as_ref()
            .expect("start_mc_init called without paimon_lake");
        // Wait for RustFS, create the bucket, then idle so testcontainers sees a
        // running container with a clear readiness marker on stdout.
        let script = format!(
            "until mc alias set rustfs http://{rustfs}:{port} {ak} {sk}; do echo 'waiting for rustfs'; sleep 1; done; \
             mc mb --ignore-existing rustfs/{bucket}; echo BUCKET_READY; tail -f /dev/null",
            rustfs = self.rustfs_container_name(),
            port = RUSTFS_S3_PORT,
            ak = lake.access_key,
            sk = lake.secret_key,
            bucket = lake.bucket,
        );
        GenericImage::new(MC_IMAGE, MC_VERSION)
            .with_entrypoint("/bin/sh")
            .with_network(self.network)
            .with_container_name(self.mc_init_container_name())
            .with_cmd(vec!["-c".to_string(), script])
            .with_ready_conditions(vec![WaitFor::message_on_stdout("BUCKET_READY")])
            .start()
            .await
            .unwrap()
    }

    async fn start_jobmanager(&self) -> ContainerAsync<GenericImage> {
        let jm = self.jobmanager_container_name();
        let flink_properties =
            format!("jobmanager.rpc.address: {jm}\nrest.address: {jm}\nrest.bind-address: 0.0.0.0");
        GenericImage::new(FLUSS_FLINK_IMAGE, FLUSS_FLINK_VERSION)
            .with_entrypoint("/opt/flink/init_paimon.sh")
            .with_network(self.network)
            .with_container_name(jm)
            .with_cmd(vec!["jobmanager"])
            .with_env_var("FLINK_PROPERTIES", flink_properties)
            .with_ready_conditions(vec![WaitFor::message_on_stdout("Rest endpoint listening")])
            .start()
            .await
            .unwrap()
    }

    async fn start_taskmanager(&self) -> ContainerAsync<GenericImage> {
        let flink_properties = format!(
            "jobmanager.rpc.address: {}\ntaskmanager.numberOfTaskSlots: 4\ntaskmanager.memory.process.size: 2048m\ntaskmanager.memory.task.off-heap.size: 128m",
            self.jobmanager_container_name()
        );
        GenericImage::new(FLUSS_FLINK_IMAGE, FLUSS_FLINK_VERSION)
            .with_entrypoint("/opt/flink/init_paimon.sh")
            .with_network(self.network)
            .with_container_name(self.taskmanager_container_name())
            .with_cmd(vec!["taskmanager"])
            .with_env_var("FLINK_PROPERTIES", flink_properties)
            .with_ready_conditions(vec![WaitFor::message_on_stdout(
                "Successful registration at resource manager",
            )])
            .start()
            .await
            .unwrap()
    }

    fn to_fluss_properties_with(&self, extra_properties: HashMap<&str, String>) -> String {
        let mut fluss_properties = Vec::new();
        for (k, v) in self.cluster_conf.iter() {
            fluss_properties.push(format!("{}: {}", k, v));
        }
        for (k, v) in extra_properties.iter() {
            fluss_properties.push(format!("{}: {}", k, v));
        }
        fluss_properties.join("\n")
    }
}

/// Internal result of starting every container, handed to `build`.
struct StartedCluster {
    /// All started containers, kept alive for RAII / teardown.
    all: Vec<Arc<ContainerAsync<GenericImage>>>,
    /// The Flink job manager, present when the tiering service is enabled, used
    /// to submit/cancel the tiering job via `exec`.
    jobmanager: Option<Arc<ContainerAsync<GenericImage>>>,
    /// Host-mapped S3 endpoint (`http://127.0.0.1:<port>`), present in lake mode.
    s3_host_endpoint: Option<String>,
}

#[derive(Clone)]
pub struct FlussTestingCluster {
    /// All containers, held for RAII so they stop when the cluster is dropped.
    #[allow(dead_code)]
    containers: Vec<Arc<ContainerAsync<GenericImage>>>,
    jobmanager: Option<Arc<ContainerAsync<GenericImage>>>,
    s3_host_endpoint: Option<String>,
    paimon_lake: Option<PaimonLakeConfig>,
    testing_name: String,
    bootstrap_servers: String,
    sasl_bootstrap_servers: Option<String>,
    remote_data_dir: Option<std::path::PathBuf>,
    sasl_users: Vec<(String, String)>,
    container_names: Vec<String>,
}

impl FlussTestingCluster {
    pub fn stop(&self) {
        let names = self.container_names.clone();
        run_blocking(async move {
            if let Some(docker) = docker_client().await {
                for name in &names {
                    force_remove_container(&docker, name).await;
                }
            }
        });
        if let Some(ref dir) = self.remote_data_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    pub fn sasl_users(&self) -> &[(String, String)] {
        &self.sasl_users
    }

    pub fn plaintext_bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Host-visible S3 endpoint for the lake store (`http://127.0.0.1:<port>`).
    /// `None` unless [`FlussTestingClusterBuilder::with_paimon_lake`] was used.
    /// Feed this to `fluss_lake::set_test_lake_s3_endpoint_override` so the
    /// in-process Paimon reader reaches the same store the cluster derives into
    /// table properties (which carries the container-internal endpoint).
    pub fn s3_endpoint_host(&self) -> Option<&str> {
        self.s3_host_endpoint.as_deref()
    }

    pub fn s3_access_key(&self) -> Option<&str> {
        self.paimon_lake.as_ref().map(|l| l.access_key.as_str())
    }

    pub fn s3_secret_key(&self) -> Option<&str> {
        self.paimon_lake.as_ref().map(|l| l.secret_key.as_str())
    }

    pub fn paimon_warehouse(&self) -> Option<&str> {
        self.paimon_lake.as_ref().map(|l| l.warehouse.as_str())
    }

    /// Submits the real Flink tiering job for `paimon` and returns a handle that
    /// can stop it. Requires both `with_paimon_lake` and `with_tiering_service`.
    ///
    /// The job runs detached inside the cluster network, connecting to the Fluss
    /// coordinator through the in-network `NETWORK` listener and writing to the
    /// S3 lake warehouse. Stopping it (via [`TieringJob::stop`]) freezes the lake
    /// seam so subsequent writes stay in the Fluss log for union-read tests.
    pub async fn start_paimon_tiering(&self) -> TieringJob {
        let jobmanager = self
            .jobmanager
            .clone()
            .expect("start_paimon_tiering requires with_tiering_service()");
        let lake = self
            .paimon_lake
            .as_ref()
            .expect("start_paimon_tiering requires with_paimon_lake()");
        let bootstrap = format!(
            "coordinator-server-{}:{}",
            self.testing_name, NETWORK_LISTENER_PORT
        );
        let endpoint = format!("http://rustfs-{}:{}", self.testing_name, RUSTFS_S3_PORT);
        let tiering_jar = format!("/opt/flink/opt/fluss-flink-tiering-{FLUSS_VERSION}.jar");
        let cmd: Vec<String> = vec![
            "/opt/flink/bin/flink".to_string(),
            "run".to_string(),
            "-d".to_string(),
            tiering_jar,
            "--fluss.bootstrap.servers".to_string(),
            bootstrap,
            "--datalake.format".to_string(),
            "paimon".to_string(),
            "--datalake.paimon.metastore".to_string(),
            "filesystem".to_string(),
            "--datalake.paimon.warehouse".to_string(),
            lake.warehouse.clone(),
            "--datalake.paimon.s3.endpoint".to_string(),
            endpoint,
            "--datalake.paimon.s3.access.key".to_string(),
            lake.access_key.clone(),
            "--datalake.paimon.s3.secret.key".to_string(),
            lake.secret_key.clone(),
            "--datalake.paimon.s3.path.style.access".to_string(),
            "true".to_string(),
        ];
        let mut result = jobmanager
            .exec(ExecCommand::new(cmd))
            .await
            .expect("failed to exec `flink run` for tiering job");
        let stdout = String::from_utf8_lossy(
            &result
                .stdout_to_vec()
                .await
                .expect("failed to read `flink run` stdout"),
        )
        .to_string();
        let stderr =
            String::from_utf8_lossy(&result.stderr_to_vec().await.unwrap_or_default()).to_string();
        let job_id = parse_flink_job_id(&stdout).unwrap_or_else(|| {
            panic!(
                "could not parse Flink JobID from tiering submit output:\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
            )
        });
        TieringJob { jobmanager, job_id }
    }

    pub async fn get_fluss_connection(&self) -> FlussConnection {
        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: self.bootstrap_servers.clone(),
            ..Default::default()
        };

        self.connect_with_retry(config).await
    }

    pub async fn get_fluss_connection_with_sasl(
        &self,
        username: &str,
        password: &str,
    ) -> FlussConnection {
        let bootstrap = self
            .sasl_bootstrap_servers
            .clone()
            .unwrap_or_else(|| self.bootstrap_servers.clone());
        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: bootstrap,
            security_protocol: "sasl".to_string(),
            security_sasl_mechanism: "PLAIN".to_string(),
            security_sasl_username: username.to_string(),
            security_sasl_password: password.to_string(),
            ..Default::default()
        };

        self.connect_with_retry(config).await
    }

    pub async fn try_fluss_connection_with_sasl(
        &self,
        username: &str,
        password: &str,
    ) -> fluss::error::Result<FlussConnection> {
        let bootstrap = self
            .sasl_bootstrap_servers
            .clone()
            .unwrap_or_else(|| self.bootstrap_servers.clone());
        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: bootstrap,
            security_protocol: "sasl".to_string(),
            security_sasl_mechanism: "PLAIN".to_string(),
            security_sasl_username: username.to_string(),
            security_sasl_password: password.to_string(),
            ..Default::default()
        };

        FlussConnection::new(config).await
    }

    async fn connect_with_retry(&self, config: Config) -> FlussConnection {
        let max_retries = 60;
        let retry_interval = Duration::from_secs(1);

        for attempt in 1..=max_retries {
            match FlussConnection::new(config.clone()).await {
                Ok(connection) => {
                    return connection;
                }
                Err(e) => {
                    if attempt == max_retries {
                        panic!(
                            "Failed to connect to Fluss cluster after {} attempts: {}",
                            max_retries, e
                        );
                    }
                    tokio::time::sleep(retry_interval).await;
                }
            }
        }
        unreachable!()
    }
}

/// A submitted Flink tiering job that can be cancelled to freeze the lake seam.
pub struct TieringJob {
    jobmanager: Arc<ContainerAsync<GenericImage>>,
    job_id: String,
}

impl TieringJob {
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Cancels the tiering job (`flink cancel <job_id>`). Best-effort: failures
    /// are logged, not panicked, so teardown still proceeds.
    pub async fn stop(self) {
        let cmd: Vec<String> = vec![
            "/opt/flink/bin/flink".to_string(),
            "cancel".to_string(),
            self.job_id.clone(),
        ];
        match self.jobmanager.exec(ExecCommand::new(cmd)).await {
            Ok(mut result) => {
                let _ = result.stdout_to_vec().await;
            }
            Err(e) => eprintln!("warning: failed to cancel tiering job {}: {e}", self.job_id),
        }
    }
}

/// Extracts the Flink job id from `flink run -d` output, which prints
/// "Job has been submitted with JobID <hex>".
fn parse_flink_job_id(output: &str) -> Option<String> {
    let idx = output.find("JobID")?;
    output[idx + "JobID".len()..]
        .split_whitespace()
        .next()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn stop_cluster(name: &str) {
    let name = name.to_string();
    run_blocking(async move { stop_cluster_async(&name).await });
}

/// Lists every container belonging to cluster `name` (by name prefix) on the
/// testcontainers daemon and force-removes each. Same daemon `start` uses, so a
/// cluster started via `build_detached` is always the one torn down here.
async fn stop_cluster_async(name: &str) {
    let Some(docker) = docker_client().await else {
        return;
    };

    // Multiple values for the `name` filter are OR'd by the daemon; these prefixes
    // cover zookeeper, coordinator, and any number of tablet servers.
    let mut filters = HashMap::new();
    filters.insert(
        "name".to_string(),
        vec![
            format!("zookeeper-{name}"),
            format!("coordinator-server-{name}"),
            format!("tablet-server-{name}-"),
            format!("rustfs-{name}"),
            format!("mc-init-{name}"),
            format!("jobmanager-{name}"),
            format!("taskmanager-{name}"),
        ],
    );
    let options = ListContainersOptionsBuilder::default()
        .all(true)
        .filters(&filters)
        .build();

    let containers = match docker.list_containers(Some(options)).await {
        Ok(containers) => containers,
        Err(e) => {
            eprintln!("warning: failed to list cluster containers: {e}");
            return;
        }
    };

    for container in containers {
        // Prefer the container name (daemon prefixes it with '/'); fall back to id.
        if let Some(cname) = container.names.and_then(|n| n.into_iter().next()) {
            force_remove_container(&docker, cname.trim_start_matches('/')).await;
        } else if let Some(id) = container.id {
            force_remove_container(&docker, &id).await;
        }
    }
}
