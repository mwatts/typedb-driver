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
#[cfg(feature = "grpc")]
use std::pin::Pin;

use tracing::debug;

use crate::{
    answer::QueryAnswer,
    common::{BoxPromise, Result, TransactionType},
    Error, QueryOptions, TransactionOptions,
};
#[cfg(feature = "grpc")]
use crate::common::Promise;
#[cfg(feature = "grpc")]
use crate::{
    analyze::AnalyzedQuery,
    connection::TransactionStream,
};
#[cfg(all(feature = "embedded", not(feature = "grpc")))]
use crate::analyze::AnalyzedQuery;

// ---------------------------------------------------------------------------
// TransactionInner enum
// ---------------------------------------------------------------------------

enum TransactionInner {
    #[cfg(feature = "grpc")]
    Grpc(Pin<Box<TransactionStream>>),
    #[cfg(feature = "embedded")]
    Embedded(std::sync::Mutex<crate::embedded::embedded_backend::EmbeddedTransaction>),
}

/// A transaction with a TypeDB database.
pub struct Transaction {
    /// The transaction's type (READ or WRITE)
    type_: TransactionType,
    /// The options for the transaction
    options: TransactionOptions,
    inner: TransactionInner,
}

// ---------------------------------------------------------------------------
// gRPC constructor
// ---------------------------------------------------------------------------

#[cfg(feature = "grpc")]
impl Transaction {
    pub(super) fn new(transaction_stream: TransactionStream) -> Self {
        let transaction_stream = Box::pin(transaction_stream);
        Transaction {
            type_: transaction_stream.type_(),
            options: transaction_stream.options().clone(),
            inner: TransactionInner::Grpc(transaction_stream),
        }
    }

    fn grpc_stream(&self) -> &TransactionStream {
        match &self.inner {
            TransactionInner::Grpc(stream) => stream,
            #[cfg(feature = "embedded")]
            TransactionInner::Embedded(_) => {
                panic!("Expected gRPC transaction but found embedded transaction")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Embedded constructor
// ---------------------------------------------------------------------------

#[cfg(feature = "embedded")]
impl Transaction {
    pub(super) fn new_embedded(
        embedded_tx: crate::embedded::embedded_backend::EmbeddedTransaction,
    ) -> Self {
        let type_ = embedded_tx.type_();
        Transaction {
            type_,
            options: TransactionOptions::new(),
            inner: TransactionInner::Embedded(std::sync::Mutex::new(embedded_tx)),
        }
    }
}

// ---------------------------------------------------------------------------
// Methods when gRPC is available (may also handle embedded variants)
// ---------------------------------------------------------------------------

#[cfg(feature = "grpc")]
impl Transaction {
    /// Checks if the transaction is open.
    ///
    /// # Examples
    ///
    /// ```rust
    /// transaction.is_open()
    /// ```
    pub fn is_open(&self) -> bool {
        match &self.inner {
            TransactionInner::Grpc(stream) => stream.is_open(),
            #[cfg(feature = "embedded")]
            TransactionInner::Embedded(tx) => tx.lock().unwrap().is_open(),
        }
    }

    /// Performs a TypeQL query with default options.
    /// See [`Transaction::query_with_options`]
    #[cfg(not(feature = "embedded"))]
    pub fn query(&self, query: impl AsRef<str>) -> impl Promise<'static, Result<QueryAnswer>> {
        self.query_with_options(query, QueryOptions::new())
    }

    /// Performs a TypeQL query with default options.
    /// See [`Transaction::query_with_options`]
    #[cfg(feature = "embedded")]
    pub fn query(&self, query: impl AsRef<str>) -> BoxPromise<'static, Result<QueryAnswer>> {
        self.query_with_options(query, QueryOptions::new())
    }

    /// Performs a TypeQL query in this transaction.
    #[cfg(not(feature = "embedded"))]
    pub fn query_with_options(
        &self,
        query: impl AsRef<str>,
        options: QueryOptions,
    ) -> impl Promise<'static, Result<QueryAnswer>> {
        let query = query.as_ref();
        debug!("Transaction submitting query: {}", query);
        self.grpc_stream().query(query, options)
    }

    /// Performs a TypeQL query in this transaction.
    #[cfg(feature = "embedded")]
    pub fn query_with_options(
        &self,
        query: impl AsRef<str>,
        options: QueryOptions,
    ) -> BoxPromise<'static, Result<QueryAnswer>> {
        let query = query.as_ref();
        debug!("Transaction submitting query: {}", query);
        match &self.inner {
            TransactionInner::Grpc(stream) => {
                crate::common::box_promise(stream.query(query, options))
            }
            TransactionInner::Embedded(tx) => {
                let result = tx.lock().unwrap().query(query, options);
                crate::common::box_promise(crate::promisify! { result })
            }
        }
    }

    /// Analyzes a TypeQL query in this transaction.
    #[cfg(not(feature = "embedded"))]
    pub fn analyze(&self, query: impl AsRef<str>) -> impl Promise<'static, Result<AnalyzedQuery>> {
        self.grpc_stream().analyze(query.as_ref())
    }

    /// Analyzes a TypeQL query in this transaction.
    #[cfg(feature = "embedded")]
    pub fn analyze(&self, query: impl AsRef<str>) -> BoxPromise<'static, Result<AnalyzedQuery>> {
        match &self.inner {
            TransactionInner::Grpc(stream) => {
                crate::common::box_promise(stream.analyze(query.as_ref()))
            }
            TransactionInner::Embedded(_) => {
                crate::common::box_promise(crate::promisify! {
                    Err(Error::Other("Analyze is not supported for embedded transactions".to_string()))
                })
            }
        }
    }

    /// Registers a callback function which will be executed when this transaction is closed.
    #[cfg(not(feature = "embedded"))]
    pub fn on_close(
        &self,
        callback: impl FnOnce(Option<Error>) + Send + Sync + 'static,
    ) -> impl Promise<'_, Result<()>> {
        self.grpc_stream().on_close(callback)
    }

    /// Registers a callback function which will be executed when this transaction is closed.
    #[cfg(feature = "embedded")]
    pub fn on_close(
        &self,
        callback: impl FnOnce(Option<Error>) + Send + Sync + 'static,
    ) -> BoxPromise<'_, Result<()>> {
        match &self.inner {
            TransactionInner::Grpc(stream) => {
                crate::common::box_promise(stream.on_close(callback))
            }
            TransactionInner::Embedded(_) => {
                callback(None);
                crate::common::box_promise(crate::promisify! { Ok(()) })
            }
        }
    }

    /// Closes the transaction.
    #[cfg(not(feature = "embedded"))]
    pub fn close(&self) -> impl Promise<'_, Result<()>> {
        self.grpc_stream().close()
    }

    /// Closes the transaction.
    #[cfg(feature = "embedded")]
    pub fn close(&self) -> BoxPromise<'_, Result<()>> {
        match &self.inner {
            TransactionInner::Grpc(stream) => {
                crate::common::box_promise(stream.close())
            }
            TransactionInner::Embedded(_) => {
                // Embedded transactions are closed when dropped
                crate::common::box_promise(crate::promisify! { Ok(()) })
            }
        }
    }

    /// Commits the changes made via this transaction to the TypeDB database.
    #[cfg(not(feature = "embedded"))]
    pub fn commit(self) -> impl Promise<'static, Result> {
        match self.inner {
            TransactionInner::Grpc(stream) => stream.commit(),
        }
    }

    /// Commits the changes made via this transaction.
    #[cfg(feature = "embedded")]
    pub fn commit(self) -> BoxPromise<'static, Result> {
        match self.inner {
            TransactionInner::Grpc(stream) => {
                crate::common::box_promise(stream.commit())
            }
            TransactionInner::Embedded(tx) => {
                let embedded_tx = tx.into_inner().unwrap();
                let result = embedded_tx.commit();
                crate::common::box_promise(crate::promisify! { result })
            }
        }
    }

    /// Rolls back the uncommitted changes made via this transaction.
    #[cfg(not(feature = "embedded"))]
    pub fn rollback(&self) -> impl Promise<'_, Result> {
        self.grpc_stream().rollback()
    }

    /// Rolls back the uncommitted changes made via this transaction.
    #[cfg(feature = "embedded")]
    pub fn rollback(&self) -> BoxPromise<'_, Result> {
        match &self.inner {
            TransactionInner::Grpc(stream) => {
                crate::common::box_promise(stream.rollback())
            }
            TransactionInner::Embedded(tx) => {
                let result = tx.lock().unwrap().rollback();
                crate::common::box_promise(crate::promisify! { result })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Methods when gRPC is NOT available (embedded-only)
// ---------------------------------------------------------------------------

#[cfg(all(feature = "embedded", not(feature = "grpc")))]
impl Transaction {
    /// Check if the transaction is open.
    pub fn is_open(&self) -> bool {
        match &self.inner {
            TransactionInner::Embedded(tx) => tx.lock().unwrap().is_open(),
        }
    }

    /// Performs a TypeQL query with default options.
    pub fn query(&self, query: impl AsRef<str>) -> BoxPromise<'static, Result<QueryAnswer>> {
        self.query_with_options(query, QueryOptions::new())
    }

    /// Performs a TypeQL query in this transaction.
    pub fn query_with_options(
        &self,
        query: impl AsRef<str>,
        options: QueryOptions,
    ) -> BoxPromise<'static, Result<QueryAnswer>> {
        let query = query.as_ref();
        debug!("Transaction submitting query: {}", query);
        match &self.inner {
            TransactionInner::Embedded(tx) => {
                let result = tx.lock().unwrap().query(query, options);
                crate::common::box_promise(crate::promisify! { result })
            }
        }
    }

    /// Analyzes a TypeQL query in this transaction.
    pub fn analyze(&self, query: impl AsRef<str>) -> BoxPromise<'static, Result<AnalyzedQuery>> {
        let _ = query;
        crate::common::box_promise(crate::promisify! {
            Err(Error::Other("Analyze is not supported for embedded transactions".to_string()))
        })
    }

    /// Registers a callback function which will be executed when this transaction is closed.
    pub fn on_close(
        &self,
        callback: impl FnOnce(Option<Error>) + Send + Sync + 'static,
    ) -> BoxPromise<'_, Result<()>> {
        callback(None);
        crate::common::box_promise(crate::promisify! { Ok(()) })
    }

    /// Closes the transaction.
    pub fn close(&self) -> BoxPromise<'_, Result<()>> {
        // Embedded transactions are closed when dropped
        crate::common::box_promise(crate::promisify! { Ok(()) })
    }

    /// Commits the changes made via this transaction.
    pub fn commit(self) -> BoxPromise<'static, Result> {
        match self.inner {
            TransactionInner::Embedded(tx) => {
                let embedded_tx = tx.into_inner().unwrap();
                let result = embedded_tx.commit();
                crate::common::box_promise(crate::promisify! { result })
            }
        }
    }

    /// Rolls back the uncommitted changes made via this transaction.
    pub fn rollback(&self) -> BoxPromise<'_, Result> {
        match &self.inner {
            TransactionInner::Embedded(tx) => {
                let result = tx.lock().unwrap().rollback();
                crate::common::box_promise(crate::promisify! { result })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shared methods (available with any feature combination)
// ---------------------------------------------------------------------------

impl Transaction {
    /// Retrieves the transaction's type (READ or WRITE).
    pub fn type_(&self) -> TransactionType {
        self.type_
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction").field("type_", &self.type_).field("options", &self.options).finish()
    }
}
