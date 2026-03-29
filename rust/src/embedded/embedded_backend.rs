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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use database::{
    database_manager::DatabaseManager as EngineDatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{TransactionRead, TransactionSchema, TransactionWrite},
    Database,
};
use executor::ExecutionInterrupt;
use lending_iterator::LendingIterator;
use storage::durability_client::WALClient;
use typeql::query::QueryStructure;

use crate::{
    answer::{
        concept_document::ConceptDocumentHeader,
        concept_row::{ConceptRow, ConceptRowHeader},
        QueryAnswer, QueryType,
    },
    common::{Error, Result},
    embedded::convert::{convert_document, convert_variable_value},
    QueryOptions, TransactionType,
};

/// Holds the engine state for an embedded TypeDB driver.
pub(crate) struct EmbeddedState {
    pub database_manager: Arc<EngineDatabaseManager>,
    pub data_directory: std::path::PathBuf,
    /// Vector indices keyed by (db_name, index_name).
    pub vector_indices: Mutex<HashMap<(String, String), Arc<Mutex<vector::VectorIndex>>>>,
    /// Full-text indices keyed by (db_name, index_name).
    pub fulltext_indices: Mutex<HashMap<(String, String), Arc<Mutex<fulltext::FullTextIndex>>>>,
}

impl EmbeddedState {
    /// Get or create a vector index for a database + index name pair.
    pub fn vector_index(
        &self,
        db_name: &str,
        index_name: &str,
        dimension: usize,
    ) -> crate::common::Result<Arc<Mutex<vector::VectorIndex>>> {
        let key = (db_name.to_string(), index_name.to_string());
        let mut indices = self.vector_indices.lock().map_err(|e| {
            crate::common::Error::Other(format!("Failed to acquire vector index lock: {e}"))
        })?;

        if let Some(idx) = indices.get(&key) {
            return Ok(idx.clone());
        }

        // Build path: <data_dir>/<db_name>/vectors/<index_name>.vdb
        let vector_dir = self.data_directory.join(db_name).join("vectors");
        std::fs::create_dir_all(&vector_dir).map_err(|e| {
            crate::common::Error::Other(format!(
                "Failed to create vector directory '{}': {e}",
                vector_dir.display()
            ))
        })?;
        let index_path = vector_dir.join(format!("{index_name}.vdb"));

        let index = if index_path.exists() {
            vector::VectorIndex::open(&index_path)
        } else {
            vector::VectorIndex::create(&index_path, dimension)
        }
        .map_err(|e| crate::common::Error::Other(format!("Vector index error: {e}")))?;

        let arc = Arc::new(Mutex::new(index));
        indices.insert(key, arc.clone());
        Ok(arc)
    }

    /// Get or create a full-text index for a database + index name pair.
    pub fn fulltext_index(
        &self,
        db_name: &str,
        index_name: &str,
    ) -> crate::common::Result<Arc<Mutex<fulltext::FullTextIndex>>> {
        let key = (db_name.to_string(), index_name.to_string());
        let mut indices = self.fulltext_indices.lock().map_err(|e| {
            crate::common::Error::Other(format!("Failed to acquire fulltext index lock: {e}"))
        })?;

        if let Some(idx) = indices.get(&key) {
            return Ok(idx.clone());
        }

        // Build path: <data_dir>/<db_name>/fulltext/<index_name>
        let fts_dir = self.data_directory.join(db_name).join("fulltext");
        std::fs::create_dir_all(&fts_dir).map_err(|e| {
            crate::common::Error::Other(format!(
                "Failed to create fulltext directory '{}': {e}",
                fts_dir.display()
            ))
        })?;
        let index_path = fts_dir.join(index_name);

        let index = if index_path.exists() {
            fulltext::FullTextIndex::open(&index_path)
        } else {
            fulltext::FullTextIndex::create(&index_path)
        }
        .map_err(|e| crate::common::Error::Other(format!("Full-text index error: {e}")))?;

        let arc = Arc::new(Mutex::new(index));
        indices.insert(key, arc.clone());
        Ok(arc)
    }
}

/// Holds an in-progress engine transaction.
pub(crate) enum EmbeddedTransaction {
    Schema {
        tx: Option<TransactionSchema<WALClient>>,
        database: Arc<Database<WALClient>>,
    },
    Write {
        tx: Option<TransactionWrite<WALClient>>,
        database: Arc<Database<WALClient>>,
    },
    Read {
        tx: Option<TransactionRead<WALClient>>,
        database: Arc<Database<WALClient>>,
    },
}

impl EmbeddedTransaction {
    /// Open a new embedded transaction of the specified type.
    pub fn open(
        database: Arc<Database<WALClient>>,
        transaction_type: TransactionType,
    ) -> Result<Self> {
        let tx_options = engine_options::TransactionOptions::default();
        match transaction_type {
            TransactionType::Schema => {
                let tx = TransactionSchema::open(database.clone(), tx_options)
                    .map_err(|e| Error::Other(format!("Failed to open schema transaction: {e:?}")))?;
                Ok(EmbeddedTransaction::Schema {
                    tx: Some(tx),
                    database,
                })
            }
            TransactionType::Read => {
                let tx = TransactionRead::open(database.clone(), tx_options)
                    .map_err(|e| Error::Other(format!("Failed to open read transaction: {e:?}")))?;
                Ok(EmbeddedTransaction::Read {
                    tx: Some(tx),
                    database,
                })
            }
            TransactionType::Write => {
                let tx = TransactionWrite::open(database.clone(), tx_options)
                    .map_err(|e| Error::Other(format!("Failed to open write transaction: {e:?}")))?;
                Ok(EmbeddedTransaction::Write {
                    tx: Some(tx),
                    database,
                })
            }
        }
    }

    /// Execute a TypeQL query string and return a driver QueryAnswer.
    pub fn query(&mut self, query_str: &str, _options: QueryOptions) -> Result<QueryAnswer> {
        let parsed = typeql::parse_query(query_str)
            .map_err(|e| Error::Other(format!("Failed to parse query: {e}")))?;

        match parsed.structure {
            QueryStructure::Schema(schema_query) => {
                self.execute_schema(schema_query, query_str.to_string())
            }
            QueryStructure::Pipeline(pipeline) => {
                self.execute_pipeline(pipeline, query_str)
            }
        }
    }

    fn execute_schema(
        &mut self,
        schema_query: typeql::query::SchemaQuery,
        source: String,
    ) -> Result<QueryAnswer> {
        match self {
            EmbeddedTransaction::Schema { tx, .. } => {
                let transaction = tx.take()
                    .ok_or_else(|| Error::Other("Schema transaction already consumed".to_string()))?;
                let (transaction, result) = execute_schema_query(transaction, schema_query, source);
                *tx = Some(transaction);
                result.map_err(|e| Error::Other(format!("Schema query error: {e:?}")))?;
                Ok(QueryAnswer::Ok(QueryType::SchemaQuery))
            }
            _ => Err(Error::Other(
                "Schema queries can only be executed in a schema transaction".to_string(),
            )),
        }
    }

    fn execute_pipeline(
        &mut self,
        pipeline: typeql::query::Pipeline,
        source_query: &str,
    ) -> Result<QueryAnswer> {
        match self {
            EmbeddedTransaction::Write { tx, .. } => {
                let transaction = tx.take()
                    .ok_or_else(|| Error::Other("Write transaction already consumed".to_string()))?;
                let query_options = engine_options::QueryOptions::default_grpc();
                let (transaction, result) = execute_write_query_in_write(
                    transaction,
                    query_options,
                    pipeline,
                    source_query.to_string(),
                    ExecutionInterrupt::new_uninterruptible(),
                );
                *tx = Some(transaction);

                let answer = result.map_err(|e| Error::Other(format!("Write query error: {e:?}")))?;

                // Convert write query answer to driver QueryAnswer
                match answer.answer {
                    itertools::Either::Left((output_descriptor, batch, pipeline_structure)) => {
                        // Row-based answer
                        let column_names: Vec<String> = output_descriptor
                            .iter()
                            .map(|(name, _)| name.clone())
                            .collect();
                        let positions: Vec<compiler::VariablePosition> = output_descriptor
                            .iter()
                            .map(|(_, pos)| *pos)
                            .collect();

                        let header = Arc::new(ConceptRowHeader {
                            column_names,
                            query_type: QueryType::WriteQuery,
                            query_structure: None,
                        });

                        // Convert batch rows to driver ConceptRows
                        let tx_ref = tx.as_ref().unwrap();
                        let rows = convert_batch_to_rows(
                            &batch,
                            &positions,
                            header.clone(),
                            tx_ref.snapshot.as_ref(),
                            &tx_ref.type_manager,
                            &tx_ref.thing_manager,
                        );

                        let stream = futures::stream::iter(rows.into_iter().map(Ok));
                        Ok(QueryAnswer::ConceptRowStream(
                            header,
                            Box::pin(stream),
                        ))
                    }
                    itertools::Either::Right((parameters, documents)) => {
                        // Document-based answer (fetch queries)
                        let tx_ref = tx.as_ref().unwrap();
                        let header = Arc::new(ConceptDocumentHeader {
                            query_type: QueryType::WriteQuery,
                        });

                        let driver_docs: Vec<_> = documents
                            .into_iter()
                            .map(|doc| {
                                let mut driver_doc = convert_document(
                                    doc,
                                    tx_ref.snapshot.as_ref(),
                                    &tx_ref.type_manager,
                                    &tx_ref.thing_manager,
                                    &parameters,
                                );
                                // Override header with shared one
                                driver_doc = crate::answer::concept_document::ConceptDocument::new(
                                    header.clone(),
                                    driver_doc.root,
                                );
                                Ok(driver_doc)
                            })
                            .collect();

                        let stream = futures::stream::iter(driver_docs);
                        Ok(QueryAnswer::ConceptDocumentStream(header, Box::pin(stream)))
                    }
                }
            }
            EmbeddedTransaction::Read { tx, .. } => {
                let tx_ref = tx.as_ref()
                    .ok_or_else(|| Error::Other("Read transaction already consumed".to_string()))?;

                let read_pipeline = tx_ref
                    .query_manager
                    .prepare_read_pipeline(
                        tx_ref.snapshot.clone(),
                        &tx_ref.type_manager,
                        tx_ref.thing_manager.clone(),
                        &tx_ref.function_manager,
                        &pipeline,
                        source_query,
                    )
                    .map_err(|e| Error::Other(format!("Read query preparation error: {e:?}")))?;

                if read_pipeline.has_fetch() {
                    // Document-based result (fetch queries)
                    let (iterator, context) = read_pipeline
                        .into_documents_iterator(ExecutionInterrupt::new_uninterruptible())
                        .map_err(|(e, _)| Error::Other(format!("Fetch query execution error: {e:?}")))?;

                    let header = Arc::new(ConceptDocumentHeader {
                        query_type: QueryType::ReadQuery,
                    });

                    let snapshot = &*context.snapshot;
                    let type_manager = &tx_ref.type_manager;
                    let thing_manager_ref = &context.thing_manager;
                    let parameters = &context.parameters;

                    let mut docs = Vec::new();
                    for result in iterator {
                        match result {
                            Ok(doc) => {
                                let mut driver_doc = convert_document(
                                    doc,
                                    snapshot,
                                    type_manager,
                                    thing_manager_ref,
                                    parameters,
                                );
                                driver_doc = crate::answer::concept_document::ConceptDocument::new(
                                    header.clone(),
                                    driver_doc.root,
                                );
                                docs.push(Ok(driver_doc));
                            }
                            Err(e) => {
                                docs.push(Err(Error::Other(format!("Document iteration error: {e:?}"))));
                                break;
                            }
                        }
                    }

                    let stream = futures::stream::iter(docs);
                    return Ok(QueryAnswer::ConceptDocumentStream(header, Box::pin(stream)));
                }

                let named_outputs = read_pipeline.rows_positions().unwrap().clone();
                let column_names: Vec<String> = {
                    let mut pairs: Vec<_> = named_outputs.iter().collect();
                    pairs.sort_by_key(|(_, pos)| pos.as_usize());
                    pairs.into_iter().map(|(name, _)| name.clone()).collect()
                };
                let positions: Vec<compiler::VariablePosition> = {
                    let mut pairs: Vec<_> = named_outputs.iter().collect();
                    pairs.sort_by_key(|(_, pos)| pos.as_usize());
                    pairs.into_iter().map(|(_, pos)| *pos).collect()
                };

                let header = Arc::new(ConceptRowHeader {
                    column_names,
                    query_type: QueryType::ReadQuery,
                    query_structure: None,
                });

                // Get type_manager reference from the transaction before consuming the pipeline
                let type_manager = &tx_ref.type_manager;

                let (mut iterator, context) = read_pipeline
                    .into_rows_iterator(ExecutionInterrupt::new_uninterruptible())
                    .map_err(|(e, _)| Error::Other(format!("Read query execution error: {e:?}")))?;

                // Collect all rows eagerly (the iterator borrows from the snapshot)
                let snapshot = &*context.snapshot;
                let thing_manager_ref = &context.thing_manager;

                let mut rows = Vec::new();
                while let Some(result) = iterator.next() {
                    match result {
                        Ok(row) => {
                            let driver_row = convert_row_to_concept_row(
                                &row,
                                &positions,
                                header.clone(),
                                snapshot,
                                type_manager,
                                thing_manager_ref,
                            );
                            rows.push(Ok(driver_row));
                        }
                        Err(e) => {
                            rows.push(Err(Error::Other(format!("Row iteration error: {e:?}"))));
                            break;
                        }
                    }
                }

                let stream = futures::stream::iter(rows);
                Ok(QueryAnswer::ConceptRowStream(header, Box::pin(stream)))
            }
            EmbeddedTransaction::Schema { .. } => {
                Err(Error::Other(
                    "Pipeline queries cannot be executed in a schema transaction. Use a read or write transaction.".to_string(),
                ))
            }
        }
    }

    /// Commit the transaction.
    pub fn commit(mut self) -> Result<()> {
        match &mut self {
            EmbeddedTransaction::Schema { tx, .. } => {
                let transaction = tx.take()
                    .ok_or_else(|| Error::Other("Schema transaction already consumed".to_string()))?;
                let (_profile, result) = transaction.commit();
                result.map_err(|e| Error::Other(format!("Schema commit error: {e:?}")))
            }
            EmbeddedTransaction::Write { tx, .. } => {
                let transaction = tx.take()
                    .ok_or_else(|| Error::Other("Write transaction already consumed".to_string()))?;
                let (_profile, result) = transaction.commit();
                result.map_err(|e| Error::Other(format!("Write commit error: {e:?}")))
            }
            EmbeddedTransaction::Read { .. } => {
                Err(Error::Other("Cannot commit a read transaction".to_string()))
            }
        }
    }

    /// Rollback the transaction.
    pub fn rollback(&mut self) -> Result<()> {
        match self {
            EmbeddedTransaction::Schema { tx, .. } => {
                if let Some(ref mut transaction) = tx {
                    transaction.rollback();
                }
                Ok(())
            }
            EmbeddedTransaction::Write { tx, .. } => {
                if let Some(ref mut transaction) = tx {
                    transaction.rollback();
                }
                Ok(())
            }
            EmbeddedTransaction::Read { .. } => Ok(()),
        }
    }

    /// Check if the transaction is still open.
    pub fn is_open(&self) -> bool {
        match self {
            EmbeddedTransaction::Schema { tx, .. } => tx.is_some(),
            EmbeddedTransaction::Write { tx, .. } => tx.is_some(),
            EmbeddedTransaction::Read { tx, .. } => tx.is_some(),
        }
    }

    /// Close the transaction (drop it).
    pub fn close(mut self) {
        match &mut self {
            EmbeddedTransaction::Schema { tx, .. } => {
                if let Some(transaction) = tx.take() {
                    transaction.close();
                }
            }
            EmbeddedTransaction::Write { tx, .. } => {
                if let Some(transaction) = tx.take() {
                    transaction.close();
                }
            }
            EmbeddedTransaction::Read { tx, .. } => {
                if let Some(transaction) = tx.take() {
                    transaction.close();
                }
            }
        }
    }

    /// Get the transaction type.
    pub fn type_(&self) -> TransactionType {
        match self {
            EmbeddedTransaction::Schema { .. } => TransactionType::Schema,
            EmbeddedTransaction::Write { .. } => TransactionType::Write,
            EmbeddedTransaction::Read { .. } => TransactionType::Read,
        }
    }
}

/// Convert an engine Batch to a Vec of driver ConceptRows.
fn convert_batch_to_rows(
    batch: &executor::batch::Batch,
    positions: &[compiler::VariablePosition],
    header: Arc<ConceptRowHeader>,
    snapshot: &impl storage::snapshot::ReadableSnapshot,
    type_manager: &engine_concept::type_::type_manager::TypeManager,
    thing_manager: &engine_concept::thing::thing_manager::ThingManager,
) -> Vec<ConceptRow> {
    batch
        .iter()
        .map(|engine_row| {
            convert_row_to_concept_row(
                &engine_row,
                positions,
                header.clone(),
                snapshot,
                type_manager,
                thing_manager,
            )
        })
        .collect()
}

/// Convert a single engine row to a driver ConceptRow.
fn convert_row_to_concept_row<'a>(
    engine_row: &executor::row::MaybeOwnedRow<'a>,
    positions: &[compiler::VariablePosition],
    header: Arc<ConceptRowHeader>,
    snapshot: &impl storage::snapshot::ReadableSnapshot,
    type_manager: &engine_concept::type_::type_manager::TypeManager,
    thing_manager: &engine_concept::thing::thing_manager::ThingManager,
) -> ConceptRow {
    let concepts: Vec<Option<crate::concept::Concept>> = positions
        .iter()
        .map(|pos| {
            let var_value = engine_row.get(*pos);
            convert_variable_value(var_value, snapshot, type_manager, thing_manager)
        })
        .collect();

    ConceptRow::new(header, concepts, None)
}
