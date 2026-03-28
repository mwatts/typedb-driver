/*
 * Integration tests for the embedded TypeDB backend.
 *
 * These tests verify that the full TypeQL lifecycle works in-process
 * without any gRPC server: schema definition, data insertion,
 * querying with result verification, and transaction management.
 */

#![cfg(feature = "embedded")]

use std::path::PathBuf;

use futures::{StreamExt, TryStreamExt};
use typedb_driver::{
    answer::{ConceptRow, QueryAnswer},
    concept::Concept,
    TransactionType, TypeDBDriver,
};

/// Create a unique temp directory for each test.
fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("thyra_embedded_test_{}_{}", name, rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Clean up a test directory.
fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

// ─── Driver Creation ────────────────────────────────────────────────

#[test]
fn embedded_driver_creates_successfully() {
    let dir = test_dir("create");
    {
        let driver = TypeDBDriver::new_embedded(&dir).unwrap();
        assert!(driver.is_open());
    }
    cleanup(&dir);
}

#[test]
fn embedded_driver_creates_database() {
    let dir = test_dir("create_db");
    {
        let driver = TypeDBDriver::new_embedded(&dir).unwrap();
        let db_mgr = driver.embedded_databases().unwrap();
        db_mgr.put_database("test_db").unwrap();
        let db = db_mgr.database("test_db");
        assert!(db.is_some(), "Database should exist after creation");
    }
    cleanup(&dir);
}

// ─── Schema Transactions ────────────────────────────────────────────

#[test]
fn embedded_schema_define_and_commit() {
    async_std::task::block_on(async {
        let dir = test_dir("schema");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            assert!(tx.is_open());

            let answer = tx
                .query("define entity person, owns name, owns age; attribute name, value string; attribute age, value integer;")
                .await
                .unwrap();
            assert!(answer.is_ok(), "Schema define should return Ok");

            tx.commit().await.unwrap();
        }
        cleanup(&dir);
    });
}

#[test]
fn embedded_schema_with_relations() {
    async_std::task::block_on(async {
        let dir = test_dir("schema_rel");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    entity person, owns name, plays friendship:friend;
                    attribute name, value string;
                    relation friendship, relates friend;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }
        cleanup(&dir);
    });
}

// ─── Write Transactions ─────────────────────────────────────────────

#[test]
fn embedded_insert_data() {
    async_std::task::block_on(async {
        let dir = test_dir("insert");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Insert data
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            let answer = tx.query("insert $p isa person, has name \"Alice\";").await.unwrap();
            // Insert returns a row stream with the inserted data
            assert!(
                answer.is_ok() || answer.is_row_stream(),
                "Insert should return Ok or row stream"
            );
            tx.commit().await.unwrap();
        }
        cleanup(&dir);
    });
}

#[test]
fn embedded_insert_multiple_entities() {
    async_std::task::block_on(async {
        let dir = test_dir("insert_multi");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name, owns age; attribute name, value string; attribute age, value integer;")
                .await.unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\", has age 30;").await.unwrap();
            tx.query("insert $p isa person, has name \"Bob\", has age 25;").await.unwrap();
            tx.query("insert $p isa person, has name \"Charlie\", has age 35;").await.unwrap();
            tx.commit().await.unwrap();
        }
        cleanup(&dir);
    });
}

// ─── Read Transactions ──────────────────────────────────────────────

#[test]
fn embedded_read_query_returns_rows() {
    async_std::task::block_on(async {
        let dir = test_dir("read");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Schema
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await.unwrap();
            tx.commit().await.unwrap();

            // Insert
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\";").await.unwrap();
            tx.commit().await.unwrap();

            // Read
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person, has name $n;").await.unwrap();
            assert!(answer.is_row_stream(), "Match query should return a row stream");

            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find exactly 1 person");

            let row = &rows[0];
            let names = row.get_column_names();
            assert!(names.contains(&"p".to_string()), "Should have column 'p'");
            assert!(names.contains(&"n".to_string()), "Should have column 'n'");

            // Verify the person concept
            let p = row.get("p").unwrap();
            assert!(p.is_some(), "Person should not be empty");
            let person = p.unwrap();
            assert!(person.is_entity(), "Person should be an Entity");
            assert_eq!(person.get_label(), "person");

            // Verify the name attribute
            let n = row.get("n").unwrap();
            assert!(n.is_some(), "Name should not be empty");
            let name = n.unwrap();
            assert!(name.is_attribute(), "Name should be an Attribute");
            assert_eq!(name.try_get_string(), Some("Alice"));
        }
        cleanup(&dir);
    });
}

#[test]
fn embedded_read_multiple_rows() {
    async_std::task::block_on(async {
        let dir = test_dir("read_multi");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Schema
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await.unwrap();
            tx.commit().await.unwrap();

            // Insert 3 people
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\";").await.unwrap();
            tx.query("insert $p isa person, has name \"Bob\";").await.unwrap();
            tx.query("insert $p isa person, has name \"Charlie\";").await.unwrap();
            tx.commit().await.unwrap();

            // Read all
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person, has name $n;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 3, "Should find 3 people");

            let names: Vec<&str> = rows
                .iter()
                .filter_map(|row| row.get("n").ok()?.and_then(|c| c.try_get_string()))
                .collect();
            assert!(names.contains(&"Alice"), "Should contain Alice");
            assert!(names.contains(&"Bob"), "Should contain Bob");
            assert!(names.contains(&"Charlie"), "Should contain Charlie");
        }
        cleanup(&dir);
    });
}

#[test]
fn embedded_read_integer_values() {
    async_std::task::block_on(async {
        let dir = test_dir("read_int");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name, owns age; attribute name, value string; attribute age, value integer;")
                .await.unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\", has age 30;").await.unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person, has name $n, has age $a;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1);

            let row = &rows[0];
            let age = row.get("a").unwrap().unwrap();
            assert!(age.is_attribute(), "Age should be an Attribute");
            assert_eq!(age.try_get_integer(), Some(30));

            let name = row.get("n").unwrap().unwrap();
            assert_eq!(name.try_get_string(), Some("Alice"));
        }
        cleanup(&dir);
    });
}

// ─── Transaction Lifecycle ──────────────────────────────────────────

#[test]
fn embedded_rollback_discards_changes() {
    async_std::task::block_on(async {
        let dir = test_dir("rollback");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await.unwrap();
            tx.commit().await.unwrap();

            // Insert then rollback
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Ghost\";").await.unwrap();
            tx.rollback().await.unwrap();
            drop(tx);

            // Verify nothing was persisted
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 0, "Rollback should discard inserted data");
        }
        cleanup(&dir);
    });
}

#[test]
fn embedded_read_transaction_is_read_only() {
    async_std::task::block_on(async {
        let dir = test_dir("read_only");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person;").await.unwrap();
            tx.commit().await.unwrap();

            // Read transaction should not allow inserts
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let result = tx.query("insert $p isa person;").await;
            assert!(result.is_err(), "Read transaction should reject insert queries");
        }
        cleanup(&dir);
    });
}

// ─── Type Queries ───────────────────────────────────────────────────

#[test]
fn embedded_query_type_results() {
    async_std::task::block_on(async {
        let dir = test_dir("type_query");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person; entity animal;").await.unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match entity $t;").await.unwrap();
            assert!(answer.is_row_stream());

            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            // Should include at least 'person' and 'animal' (and potentially 'entity' root)
            assert!(rows.len() >= 2, "Should find at least 2 entity types, found {}", rows.len());

            let labels: Vec<&str> = rows
                .iter()
                .filter_map(|row| row.get("t").ok()?.map(|c| c.get_label()))
                .collect();
            assert!(labels.contains(&"person"), "Should contain person type");
            assert!(labels.contains(&"animal"), "Should contain animal type");
        }
        cleanup(&dir);
    });
}

// ─── Database Persistence ───────────────────────────────────────────

#[test]
fn embedded_data_persists_across_driver_instances() {
    async_std::task::block_on(async {
        let dir = test_dir("persist");

        // First driver: create schema and insert data
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await.unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Persisted\";").await.unwrap();
            tx.commit().await.unwrap();
        }
        // Driver dropped here

        // Second driver: verify data survived
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person, has name $n;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Data should persist across driver instances");

            let name = rows[0].get("n").unwrap().unwrap();
            assert_eq!(name.try_get_string(), Some("Persisted"));
        }

        cleanup(&dir);
    });
}

// ─── Vector Search ─────────────────────────────────────────────────

#[test]
fn embedded_vector_search() {
    async_std::task::block_on(async {
        let dir = test_dir("vector");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema with entity
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity document, owns title; attribute title, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Insert documents
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query(r#"insert $d isa document, has title "about cats";"#)
                .await
                .unwrap();
            tx.query(r#"insert $d isa document, has title "about dogs";"#)
                .await
                .unwrap();
            tx.query(r#"insert $d isa document, has title "about fish";"#)
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Insert vectors (using synthetic IDs for this test)
            driver
                .vector_insert("test", "embeddings", b"doc1", &[1.0, 0.0, 0.0], 3)
                .unwrap();
            driver
                .vector_insert("test", "embeddings", b"doc2", &[0.0, 1.0, 0.0], 3)
                .unwrap();
            driver
                .vector_insert("test", "embeddings", b"doc3", &[0.9, 0.1, 0.0], 3)
                .unwrap();

            // Search for nearest to [1, 0, 0]
            let results = driver
                .vector_search("test", "embeddings", &[1.0, 0.0, 0.0], 2, 3)
                .unwrap();
            assert_eq!(results.len(), 2, "Should return 2 nearest neighbors");
            assert_eq!(results[0].entity_id, b"doc1", "Closest should be doc1");
            assert_eq!(results[1].entity_id, b"doc3", "Second closest should be doc3");
        }
        cleanup(&dir);
    });
}

#[test]
fn embedded_vector_persistence() {
    async_std::task::block_on(async {
        let dir = test_dir("vector_persist");

        // First driver: create index and insert vectors
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            driver
                .vector_insert("test", "emb", b"a", &[1.0, 0.0, 0.0], 3)
                .unwrap();
            driver
                .vector_insert("test", "emb", b"b", &[0.0, 1.0, 0.0], 3)
                .unwrap();
        }

        // Second driver: verify vectors survive
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            let results = driver
                .vector_search("test", "emb", &[1.0, 0.0, 0.0], 2, 3)
                .unwrap();
            assert_eq!(results.len(), 2, "Vectors should persist across driver instances");
            assert_eq!(results[0].entity_id, b"a");
        }

        cleanup(&dir);
    });
}

// ─── Full-Text Search ─────────────────────────────────────────────

#[test]
fn embedded_fts_search() {
    async_std::task::block_on(async {
        let dir = test_dir("fts");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            driver.fts_index("test", "content", "doc1", "the quick brown fox").unwrap();
            driver.fts_index("test", "content", "doc2", "cats and dogs").unwrap();
            driver.fts_index("test", "content", "doc3", "the fox is clever").unwrap();

            let results = driver.fts_search("test", "content", "quick fox", 2).unwrap();
            assert_eq!(results.len(), 2);
            let ids: Vec<&str> = results.iter().map(|r| r.entity_id.as_str()).collect();
            assert!(ids.contains(&"doc1"));
            assert!(ids.contains(&"doc3"));
        }
        cleanup(&dir);
    });
}

#[test]
fn embedded_fts_persistence() {
    async_std::task::block_on(async {
        let dir = test_dir("fts_persist");

        // First driver: create index and add documents
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            driver.fts_index("test", "articles", "a1", "rust programming language").unwrap();
            driver.fts_index("test", "articles", "a2", "python programming language").unwrap();
        }

        // Second driver: verify documents survive
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            let results = driver
                .fts_search("test", "articles", "rust", 10)
                .unwrap();
            assert_eq!(results.len(), 1, "FTS documents should persist across driver instances");
            assert_eq!(results[0].entity_id, "a1");
        }

        cleanup(&dir);
    });
}
