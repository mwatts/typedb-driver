/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use std::fmt;
#[cfg(any(feature = "grpc", feature = "embedded"))]
use std::sync::Arc;
#[cfg(feature = "grpc")]
use std::collections::HashSet;

use tracing::debug;
#[cfg(feature = "grpc")]
use tracing::error;

use crate::common::{
    error::Error,
    Result,
};
#[cfg(feature = "grpc")]
use crate::connection::{
    runtime::BackgroundRuntime,
    server::{
        AvailableServer, Server, server_connection::ServerConnection, server_manager::ServerManager,
        server_routing::ServerRouting, server_version::ServerVersion,
    },
};
#[cfg(feature = "grpc")]
use crate::{Addresses, Credentials, DatabaseManager, DriverOptions, UserManager};
use crate::{Transaction, TransactionOptions, TransactionType};

// ---------------------------------------------------------------------------
// TypeDBDriver struct -- two layouts depending on feature flags
// ---------------------------------------------------------------------------

#[cfg(feature = "grpc")]
/// A connection to a TypeDB server which serves as the starting point for all interaction.
pub struct TypeDBDriver {
    server_manager: Arc<ServerManager>,
    database_manager: DatabaseManager,
    user_manager: UserManager,
    background_runtime: Arc<BackgroundRuntime>,
    #[cfg(feature = "embedded")]
    embedded_state: Option<crate::embedded::embedded_backend::EmbeddedState>,
}

#[cfg(not(feature = "grpc"))]
/// An embedded TypeDB driver (no gRPC server connection).
pub struct TypeDBDriver {
    #[cfg(feature = "embedded")]
    embedded_state: Option<crate::embedded::embedded_backend::EmbeddedState>,
}

// ---------------------------------------------------------------------------
// Constants (shared)
// ---------------------------------------------------------------------------

#[cfg(feature = "grpc")]
impl TypeDBDriver {
    const DRIVER_LANG: &'static str = "rust";
    const VERSION: &'static str = match option_env!("CARGO_PKG_VERSION") {
        None => "0.0.0",
        Some(version) => version,
    };

    pub const DEFAULT_ADDRESS: &'static str = "127.0.0.1:1729";
}

// ---------------------------------------------------------------------------
// gRPC constructors & server-oriented methods
// ---------------------------------------------------------------------------

#[cfg(feature = "grpc")]
impl TypeDBDriver {
    /// Creates a new TypeDB Server connection.
    ///
    /// # Arguments
    ///
    /// * `addresses` — The address(es) of the TypeDB Server(s), provided in a unified format
    /// * `credentials` — The Credentials to connect with
    /// * `driver_options` — The DriverOptions to connect with
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(
        feature = "sync",
        doc = "TypeDBDriver::new(Addresses::try_from_address_str(\"127.0.0.1:1729\").unwrap(), Credentials::new(\"username\", \"password\"), DriverOptions::new(true, None))"
    )]
    #[cfg_attr(
        not(feature = "sync"),
        doc = "TypeDBDriver::new(Addresses::try_from_address_str(\"127.0.0.1:1729\").unwrap(), Credentials::new(\"username\", \"password\"), DriverOptions::new(true, None)).await"
    )]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn new(addresses: Addresses, credentials: Credentials, driver_options: DriverOptions) -> Result<Self> {
        debug!("Creating new TypeDB driver connection to {:?}", addresses);
        Self::new_with_description(addresses, credentials, driver_options, Self::DRIVER_LANG).await
    }

    /// Creates a new TypeDB Server connection with a description.
    /// This method is generally used by TypeDB drivers built on top of the Rust driver.
    /// In other cases, use [`Self::new`] instead.
    ///
    /// # Arguments
    ///
    /// * `addresses` — The address(es) of the TypeDB Server(s), provided in a unified format
    /// * `credentials` — The Credentials to connect with
    /// * `driver_options` — The DriverOptions to connect with
    /// * `driver_lang` — The language of the driver connecting to the server
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(
        feature = "sync",
        doc = "TypeDBDriver::new_with_description(Addresses::try_from_address_str(\"127.0.0.1:1729\").unwrap(), Credentials::new(\"username\", \"password\"), DriverOptions::new(true, None), \"rust\")"
    )]
    #[cfg_attr(
        not(feature = "sync"),
        doc = "TypeDBDriver::new_with_description(Addresses::try_from_address_str(\"127.0.0.1:1729\").unwrap(), Credentials::new(\"username\", \"password\"), DriverOptions::new(true, None), \"rust\").await"
    )]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn new_with_description(
        addresses: Addresses,
        credentials: Credentials,
        driver_options: DriverOptions,
        driver_lang: impl AsRef<str>,
    ) -> Result<Self> {
        debug!("Initializing TypeDB driver with description: {}", driver_lang.as_ref());
        let background_runtime = Arc::new(BackgroundRuntime::new()?);

        debug!("Establishing server connection to {:?}", addresses);
        let server_manager = Arc::new(
            ServerManager::new(
                background_runtime.clone(),
                addresses,
                credentials,
                driver_options,
                driver_lang.as_ref(),
                Self::VERSION,
            )
            .await?,
        );
        debug!("Successfully connected to servers");

        let database_manager = DatabaseManager::new(server_manager.clone())?;
        let user_manager = UserManager::new(server_manager.clone());
        debug!("Created database manager and user manager");

        debug!("TypeDB driver initialization completed successfully");
        Ok(Self {
            server_manager,
            database_manager,
            user_manager,
            background_runtime,
            #[cfg(feature = "embedded")]
            embedded_state: None,
        })
    }

    /// Checks if this connection is opened.
    ///
    /// # Examples
    ///
    /// ```rust
    /// driver.is_open()
    /// ```
    pub fn is_open(&self) -> bool {
        self.background_runtime.is_open()
    }

    /// The ``DatabaseManager`` for this connection, providing access to database management methods.
    ///
    /// # Examples
    ///
    /// ```rust
    /// driver.databases()
    /// ```
    pub fn databases(&self) -> &DatabaseManager {
        &self.database_manager
    }

    /// The ``UserManager`` for this connection, providing access to user management methods.
    ///
    /// # Examples
    ///
    /// ```rust
    /// driver.databases()
    /// ```
    pub fn users(&self) -> &UserManager {
        &self.user_manager
    }

    /// Retrieves the server's version, using default automatic server routing.
    ///
    /// See [`Self::server_version_with_routing`] for more details and options.
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(feature = "sync", doc = "driver.server_version()")]
    #[cfg_attr(not(feature = "sync"), doc = "driver.server_version().await")]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn server_version(&self) -> Result<ServerVersion> {
        self.server_version_with_routing(ServerRouting::Auto).await
    }

    /// Retrieves the server's version.
    ///
    /// # Arguments
    ///
    /// * `server_routing` — The server routing directive to use for the operation
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(feature = "sync", doc = "driver.server_version_with_routing(ServerRouting::Auto);")]
    #[cfg_attr(not(feature = "sync"), doc = "driver.server_version_with_routing(ServerRouting::Auto).await;")]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn server_version_with_routing(&self, server_routing: ServerRouting) -> Result<ServerVersion> {
        self.server_manager
            .execute(server_routing, |server_connection| async move { server_connection.version().await })
            .await
    }

    /// Retrieves the servers, using default automatic server routing.
    ///
    /// See [`Self::servers_with_routing`] for more details and options.
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(feature = "sync", doc = "driver.servers();")]
    #[cfg_attr(not(feature = "sync"), doc = "driver.servers().await;")]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn servers(&self) -> Result<HashSet<Server>> {
        self.servers_with_routing(ServerRouting::Auto).await
    }

    /// Retrieves the servers.
    ///
    /// # Arguments
    ///
    /// * `server_routing` — The server routing directive to use for the operation
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(feature = "sync", doc = "driver.servers_with_routing(ServerRouting::Auto);")]
    #[cfg_attr(not(feature = "sync"), doc = "driver.servers_with_routing(ServerRouting::Auto).await;")]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn servers_with_routing(&self, server_routing: ServerRouting) -> Result<HashSet<Server>> {
        self.server_manager.fetch_servers(server_routing).await
    }

    // TODO: Add servers_get call for a specific server. How to design it?

    /// Retrieves the primary server, if exists, using default automatic server routing.
    ///
    /// See [`Self::primary_server_with_routing`] for more details and options.
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(feature = "sync", doc = "driver.primary_server();")]
    #[cfg_attr(not(feature = "sync"), doc = "driver.primary_server().await;")]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn primary_server(&self) -> Result<Option<AvailableServer>> {
        self.primary_server_with_routing(ServerRouting::Auto).await
    }

    /// Retrieves the primary server, if exists.
    ///
    /// # Arguments
    ///
    /// * `server_routing` — The server routing directive to use for the operation
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(feature = "sync", doc = "driver.primary_server_with_routing(ServerRouting::Auto);")]
    #[cfg_attr(not(feature = "sync"), doc = "driver.primary_server_with_routing(ServerRouting::Auto).await;")]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn primary_server_with_routing(&self, server_routing: ServerRouting) -> Result<Option<AvailableServer>> {
        self.server_manager.fetch_primary_server(server_routing).await
    }

    /// The ``DriverOptions`` for this connection.
    ///
    /// # Examples
    ///
    /// ```rust
    /// driver.options()
    /// ```
    pub fn options(&self) -> &DriverOptions {
        self.server_manager.driver_options()
    }

    /// The ``Addresses`` this connection is configured to.
    ///
    /// # Examples
    ///
    /// ```rust
    /// driver.configured_addresses()
    /// ```
    pub fn configured_addresses(&self) -> &Addresses {
        self.server_manager.configured_addresses()
    }

    /// Closes this connection if it is open.
    pub fn force_close(&self) -> Result {
        if !self.is_open() {
            return Ok(());
        }

        debug!("Closing TypeDB driver connection");
        let close_result = self.server_manager.force_close().and(self.background_runtime.force_close());
        match &close_result {
            Ok(_) => debug!("Successfully closed TypeDB driver connection"),
            Err(e) => error!("Failed to close TypeDB driver connection: {}", e),
        }
        close_result
    }
}

// ---------------------------------------------------------------------------
// Non-gRPC stubs (embedded-only build)
// ---------------------------------------------------------------------------

#[cfg(not(feature = "grpc"))]
impl TypeDBDriver {
    /// Checks if this connection is opened. Always returns true for embedded-only drivers.
    pub fn is_open(&self) -> bool {
        true
    }

    /// No-op close for embedded-only drivers.
    pub fn force_close(&self) -> Result {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Transaction methods -- gRPC path
// ---------------------------------------------------------------------------

#[cfg(feature = "grpc")]
impl TypeDBDriver {
    /// Opens a transaction with default options.
    ///
    /// See [`TypeDBDriver::transaction_with_options`] for more details.
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(feature = "sync", doc = "driver.transaction(database_name, TransactionType::Read);")]
    #[cfg_attr(not(feature = "sync"), doc = "driver.transaction(database_name, TransactionType::Read).await;")]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn transaction(
        &self,
        database_name: impl AsRef<str>,
        transaction_type: TransactionType,
    ) -> Result<Transaction> {
        self.transaction_with_options(database_name, transaction_type, TransactionOptions::new()).await
    }

    /// Opens a new transaction with custom transaction options.
    ///
    /// # Arguments
    ///
    /// * `database_name` — The name of the database to connect to
    /// * `transaction_type` — The TransactionType to open the transaction with
    /// * `options` — The TransactionOptions to open the transaction with
    ///
    /// # Examples
    ///
    /// ```rust
    #[cfg_attr(
        feature = "sync",
        doc = "transaction.transaction_with_options(database_name, transaction_type, options)"
    )]
    #[cfg_attr(
        not(feature = "sync"),
        doc = "transaction.transaction_with_options(database_name, transaction_type, options).await"
    )]
    /// ```
    #[cfg_attr(feature = "sync", maybe_async::must_be_sync)]
    pub async fn transaction_with_options(
        &self,
        database_name: impl AsRef<str>,
        transaction_type: TransactionType,
        options: TransactionOptions,
    ) -> Result<Transaction> {
        let database_name = database_name.as_ref();
        let open_fn = |server_connection: ServerConnection| {
            let options = options.clone();
            async move { server_connection.open_transaction(database_name, transaction_type, options).await }
        };

        #[cfg(feature = "embedded")]
        if let Some(ref embedded_state) = self.embedded_state {
            let database = embedded_state
                .database_manager
                .database(database_name)
                .ok_or_else(|| {
                    Error::Other(format!("Database '{}' not found", database_name))
                })?;
            let embedded_tx =
                crate::embedded::embedded_backend::EmbeddedTransaction::open(database, transaction_type)?;
            debug!("Successfully opened embedded transaction for database: {}", database_name);
            return Ok(Transaction::new_embedded(embedded_tx));
        }

        debug!("Opening transaction for database: {} with type: {:?}", database_name, transaction_type);
        let transaction_stream = self.server_manager.execute(ServerRouting::Auto, open_fn).await?;

        debug!("Successfully opened transaction for database: {}", database_name);
        Ok(Transaction::new(transaction_stream))
    }
}

// ---------------------------------------------------------------------------
// Transaction methods -- embedded-only (no gRPC)
// ---------------------------------------------------------------------------

#[cfg(all(feature = "embedded", not(feature = "grpc")))]
impl TypeDBDriver {
    /// Opens a transaction with default options (embedded-only).
    pub async fn transaction(
        &self,
        database_name: impl AsRef<str>,
        transaction_type: TransactionType,
    ) -> Result<Transaction> {
        self.transaction_with_options(database_name, transaction_type, TransactionOptions::new()).await
    }

    /// Opens a transaction with the given options (embedded-only).
    pub async fn transaction_with_options(
        &self,
        database_name: impl AsRef<str>,
        transaction_type: TransactionType,
        _options: TransactionOptions,
    ) -> Result<Transaction> {
        let database_name = database_name.as_ref();
        debug!("Opening embedded transaction for database: {} with type: {:?}", database_name, transaction_type);

        let embedded_state = self.embedded_state.as_ref()
            .ok_or_else(|| Error::Other("No embedded state available".into()))?;
        let database = embedded_state
            .database_manager
            .database(database_name)
            .ok_or_else(|| {
                Error::Other(format!("Database '{}' not found", database_name))
            })?;
        let embedded_tx =
            crate::embedded::embedded_backend::EmbeddedTransaction::open(database, transaction_type)?;
        debug!("Successfully opened embedded transaction for database: {}", database_name);
        Ok(Transaction::new_embedded(embedded_tx))
    }
}

// ---------------------------------------------------------------------------
// Embedded constructor -- when gRPC IS available (needs placeholder fields)
// ---------------------------------------------------------------------------

#[cfg(all(feature = "embedded", feature = "grpc"))]
impl TypeDBDriver {
    /// Creates a new embedded (in-process) TypeDB driver.
    ///
    /// # Arguments
    ///
    /// * `path` -- The data directory path for the embedded database
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// TypeDBDriver::new_embedded("/tmp/typedb-data")
    /// ```
    pub fn new_embedded(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::new_embedded_with_options(path, Default::default())
    }

    pub fn new_embedded_with_options(
        path: impl AsRef<std::path::Path>,
        options: database::database_manager::DatabaseManagerOptions,
    ) -> Result<Self> {
        Self::new_embedded_with_backend_and_options(path, kv::KVBackend::Redb, options)
    }

    /// Creates an in-memory embedded TypeDB driver.
    /// Data is not persisted — suitable for tests.
    pub fn new_embedded_in_memory(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::new_embedded_in_memory_with_options(path, Default::default())
    }

    pub fn new_embedded_in_memory_with_options(
        path: impl AsRef<std::path::Path>,
        options: database::database_manager::DatabaseManagerOptions,
    ) -> Result<Self> {
        Self::new_embedded_with_backend_and_options(path, kv::KVBackend::InMemory, options)
    }

    fn new_embedded_with_backend_and_options(
        path: impl AsRef<std::path::Path>,
        backend: kv::KVBackend,
        options: database::database_manager::DatabaseManagerOptions,
    ) -> Result<Self> {
        use crate::embedded::embedded_backend::EmbeddedState;

        let data_directory = path.as_ref().to_owned();
        if !data_directory.exists() {
            std::fs::create_dir_all(&data_directory).map_err(|e| {
                Error::Other(format!(
                    "Failed to create data directory '{}': {}",
                    data_directory.display(),
                    e
                ))
            })?;
        }

        let database_manager = database::database_manager::DatabaseManager::new_with_backend_and_options(
                &data_directory,
                backend,
                options,
            )
            .map_err(|e| Error::Other(format!("Failed to create embedded DatabaseManager: {e:?}")))?;

        let embedded_state = EmbeddedState {
            database_manager,
            data_directory,
            vector_indices: std::sync::Mutex::new(std::collections::HashMap::new()),
            fulltext_indices: std::sync::Mutex::new(std::collections::HashMap::new()),
        };

        // gRPC fields unused in embedded mode, but struct requires valid BackgroundRuntime.
        let background_runtime = Arc::new(crate::connection::runtime::BackgroundRuntime::new()?);
        let server_manager =
            Arc::new(crate::connection::server::server_manager::ServerManager::new_embedded_placeholder(
                background_runtime.clone(),
            ));

        Ok(Self {
            server_manager: server_manager.clone(),
            database_manager: DatabaseManager::new(server_manager.clone())
                .map_err(|e| Error::Other(format!("Failed to create placeholder DatabaseManager: {e}")))?,
            user_manager: UserManager::new(server_manager),
            background_runtime,
            embedded_state: Some(embedded_state),
        })
    }
}

// ---------------------------------------------------------------------------
// Embedded constructor -- when gRPC is NOT available (simple struct)
// ---------------------------------------------------------------------------

#[cfg(all(feature = "embedded", not(feature = "grpc")))]
impl TypeDBDriver {
    /// Creates a new embedded (in-process) TypeDB driver.
    pub fn new_embedded(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::new_embedded_with_options(path, Default::default())
    }

    /// Creates a new embedded (in-process) TypeDB driver with custom options
    /// for configuring background task intervals (checkpoint, statistics).
    pub fn new_embedded_with_options(
        path: impl AsRef<std::path::Path>,
        options: database::database_manager::DatabaseManagerOptions,
    ) -> Result<Self> {
        Self::new_embedded_with_backend_and_options(path, kv::KVBackend::Redb, options)
    }

    /// Creates an in-memory embedded TypeDB driver.
    /// Data is not persisted — suitable for tests.
    pub fn new_embedded_in_memory(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::new_embedded_in_memory_with_options(path, Default::default())
    }

    pub fn new_embedded_in_memory_with_options(
        path: impl AsRef<std::path::Path>,
        options: database::database_manager::DatabaseManagerOptions,
    ) -> Result<Self> {
        Self::new_embedded_with_backend_and_options(path, kv::KVBackend::InMemory, options)
    }

    fn new_embedded_with_backend_and_options(
        path: impl AsRef<std::path::Path>,
        backend: kv::KVBackend,
        options: database::database_manager::DatabaseManagerOptions,
    ) -> Result<Self> {
        use crate::embedded::embedded_backend::EmbeddedState;

        let data_directory = path.as_ref().to_owned();
        if !data_directory.exists() {
            std::fs::create_dir_all(&data_directory).map_err(|e| {
                Error::Other(format!(
                    "Failed to create data directory '{}': {}",
                    data_directory.display(),
                    e
                ))
            })?;
        }

        let database_manager = database::database_manager::DatabaseManager::new_with_backend_and_options(
                &data_directory,
                backend,
                options,
            )
            .map_err(|e| Error::Other(format!("Failed to create embedded DatabaseManager: {e:?}")))?;

        let embedded_state = EmbeddedState {
            database_manager,
            data_directory,
            vector_indices: std::sync::Mutex::new(std::collections::HashMap::new()),
            fulltext_indices: std::sync::Mutex::new(std::collections::HashMap::new()),
        };

        Ok(Self {
            embedded_state: Some(embedded_state),
        })
    }
}

// ---------------------------------------------------------------------------
// Embedded helper methods (shared regardless of grpc)
// ---------------------------------------------------------------------------

#[cfg(feature = "embedded")]
impl TypeDBDriver {
    /// Returns the embedded database manager, if this is an embedded driver.
    pub fn embedded_databases(
        &self,
    ) -> Option<&Arc<::database::database_manager::DatabaseManager>> {
        self.embedded_state.as_ref().map(|s| &s.database_manager)
    }

    /// Create a consistent backup of the named database at `dest_path`.
    ///
    /// The destination directory will be created if it doesn't exist.
    /// Safe to call while the database is serving reads and writes.
    pub fn backup_database(
        &self,
        db_name: &str,
        dest_path: impl AsRef<std::path::Path>,
    ) -> Result<std::path::PathBuf> {
        let state = self
            .embedded_state
            .as_ref()
            .ok_or_else(|| Error::Other("backup_database requires embedded mode".into()))?;
        state
            .database_manager
            .backup_database(db_name, dest_path.as_ref())
            .map_err(|e| Error::Other(format!("{e:?}")))
    }

    /// Insert a vector for an entity into a named vector index.
    ///
    /// Creates the index on first use with the given `dimension`.
    pub fn vector_insert(
        &self,
        db_name: &str,
        index_name: &str,
        entity_id: &[u8],
        vector: &[f32],
        dimension: usize,
    ) -> Result<()> {
        let state = self
            .embedded_state
            .as_ref()
            .ok_or_else(|| Error::Other("vector_insert requires embedded mode".into()))?;
        let idx = state.vector_index(db_name, index_name, dimension)?;
        let mut guard = idx
            .lock()
            .map_err(|e| Error::Other(format!("Failed to lock vector index: {e}")))?;
        guard
            .insert(entity_id, vector)
            .map_err(|e| Error::Other(format!("Vector insert error: {e}")))
    }

    /// Search for the `k` nearest neighbors in a named vector index.
    ///
    /// Creates the index on first use with the given `dimension`.
    pub fn vector_search(
        &self,
        db_name: &str,
        index_name: &str,
        query: &[f32],
        k: usize,
        dimension: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        let state = self
            .embedded_state
            .as_ref()
            .ok_or_else(|| Error::Other("vector_search requires embedded mode".into()))?;
        let idx = state.vector_index(db_name, index_name, dimension)?;
        let guard = idx
            .lock()
            .map_err(|e| Error::Other(format!("Failed to lock vector index: {e}")))?;
        let results = guard.search(query, k);
        Ok(results
            .into_iter()
            .map(|r| VectorSearchResult {
                entity_id: r.entity_id,
                distance: r.distance,
            })
            .collect())
    }

    /// Index a document for full-text search in a named FTS index.
    ///
    /// Creates the index on first use. If a document with the same `entity_id`
    /// already exists, it is replaced.
    pub fn fts_index(
        &self,
        db_name: &str,
        index_name: &str,
        entity_id: &str,
        text: &str,
    ) -> Result<()> {
        let state = self
            .embedded_state
            .as_ref()
            .ok_or_else(|| Error::Other("fts_index requires embedded mode".into()))?;
        let idx = state.fulltext_index(db_name, index_name)?;
        let mut guard = idx
            .lock()
            .map_err(|e| Error::Other(format!("Failed to lock fulltext index: {e}")))?;
        guard
            .index_document(entity_id, text)
            .map_err(|e| Error::Other(format!("Full-text index error: {e}")))
    }

    /// Search a named FTS index, returning up to `limit` results ranked by BM25 score.
    ///
    /// Creates the index on first use.
    pub fn fts_search(
        &self,
        db_name: &str,
        index_name: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<FtsSearchResult>> {
        let state = self
            .embedded_state
            .as_ref()
            .ok_or_else(|| Error::Other("fts_search requires embedded mode".into()))?;
        let idx = state.fulltext_index(db_name, index_name)?;
        let guard = idx
            .lock()
            .map_err(|e| Error::Other(format!("Failed to lock fulltext index: {e}")))?;
        let results = guard
            .search(query, limit)
            .map_err(|e| Error::Other(format!("Full-text search error: {e}")))?;
        Ok(results
            .into_iter()
            .map(|r| FtsSearchResult {
                entity_id: r.entity_id,
                score: r.score,
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Embedded result types
// ---------------------------------------------------------------------------

/// Result of a vector nearest-neighbor search.
#[cfg(feature = "embedded")]
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    /// The entity identifier that was associated with the vector.
    pub entity_id: Vec<u8>,
    /// The distance from the query vector (lower is closer).
    pub distance: f32,
}

/// Result of a full-text search query.
#[cfg(feature = "embedded")]
#[derive(Debug, Clone)]
pub struct FtsSearchResult {
    /// The entity identifier associated with the matching document.
    pub entity_id: String,
    /// The BM25 relevance score (higher is more relevant).
    pub score: f32,
}

// ---------------------------------------------------------------------------
// Debug impl
// ---------------------------------------------------------------------------

#[cfg(feature = "grpc")]
impl fmt::Debug for TypeDBDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypeDBDriver").field("server_manager", &self.server_manager).finish()
    }
}

#[cfg(not(feature = "grpc"))]
impl fmt::Debug for TypeDBDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypeDBDriver").finish()
    }
}
