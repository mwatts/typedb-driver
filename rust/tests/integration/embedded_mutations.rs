/*
 * Integration tests for mutations (delete, schema evolution, write lifecycle)
 * with the embedded TypeDB backend.
 *
 * These tests verify that the embedded driver correctly handles entity
 * deletion, attribute ownership removal, schema evolution via redefine,
 * multiple writes within a single transaction, and cross-transaction
 * schema visibility.
 */

#![cfg(feature = "embedded")]

use std::path::PathBuf;

use futures::TryStreamExt;
use typedb_driver::{answer::ConceptRow, TransactionType, TypeDBDriver};

/// Create a unique temp directory for each test.
fn test_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("thyra_mutation_test_{}_{}", name, rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Clean up a test directory.
fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

// ─── Delete Entity ─────────────────────────────────────────────────

#[test]
fn delete_entity() {
    async_std::task::block_on(async {
        let dir = test_dir("delete_entity");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Insert Alice
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\";").await.unwrap();
            tx.commit().await.unwrap();

            // Verify Alice exists
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Alice should exist before deletion");
            drop(tx);

            // Delete Alice
            // TypeQL 3.x delete syntax: match ... ; delete $var;
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            let answer = tx
                .query("match $p isa person, has name \"Alice\"; delete $p;")
                .await
                .unwrap();
            // Delete returns a row stream (the matched-then-deleted rows)
            assert!(
                answer.is_ok() || answer.is_row_stream(),
                "Delete should return Ok or row stream"
            );
            // Consume the stream if it is one, to drive execution
            if answer.is_row_stream() {
                let _rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            }
            tx.commit().await.unwrap();

            // Verify Alice is gone
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 0, "No persons should remain after deleting Alice");
        }
        cleanup(&dir);
    });
}

// ─── Delete One of Multiple ────────────────────────────────────────

#[test]
fn delete_one_of_multiple() {
    async_std::task::block_on(async {
        let dir = test_dir("delete_one_of_multi");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Insert Alice, Bob, Charlie
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\";").await.unwrap();
            tx.query("insert $p isa person, has name \"Bob\";").await.unwrap();
            tx.query("insert $p isa person, has name \"Charlie\";").await.unwrap();
            tx.commit().await.unwrap();

            // Delete only Bob
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            let answer = tx
                .query("match $p isa person, has name \"Bob\"; delete $p;")
                .await
                .unwrap();
            if answer.is_row_stream() {
                let _rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            }
            tx.commit().await.unwrap();

            // Verify Alice and Charlie remain, Bob is gone
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person, has name $n;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 2, "Should have 2 people remaining after deleting Bob");

            let names: Vec<&str> = rows
                .iter()
                .filter_map(|row| row.get("n").ok()?.and_then(|c| c.try_get_string()))
                .collect();
            assert!(names.contains(&"Alice"), "Alice should remain");
            assert!(names.contains(&"Charlie"), "Charlie should remain");
            assert!(!names.contains(&"Bob"), "Bob should be deleted");
        }
        cleanup(&dir);
    });
}

// ─── Delete Attribute Ownership ────────────────────────────────────

#[test]
fn delete_attribute_ownership() {
    async_std::task::block_on(async {
        let dir = test_dir("delete_attr_own");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema with name and age
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define entity person, owns name, owns age; attribute name, value string; attribute age, value integer;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert person with name and age
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\", has age 30;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Delete only the age ownership (not the entity)
            // TypeQL 3.x syntax: delete has $attr of $owner;
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            let answer = tx
                .query("match $p isa person, has name \"Alice\", has age $a; delete has $a of $p;")
                .await
                .unwrap();
            if answer.is_row_stream() {
                let _rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            }
            tx.commit().await.unwrap();

            // Verify: person still exists with name, but no age
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();

            // Person still has name
            let answer = tx.query("match $p isa person, has name $n;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Person should still exist with name");
            let name = rows[0].get("n").unwrap().unwrap();
            assert_eq!(name.try_get_string(), Some("Alice"), "Name should still be Alice");

            // Person should NOT have age anymore
            let answer = tx
                .query("match $p isa person, has name \"Alice\", has age $a;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 0, "Person should no longer have an age attribute");
        }
        cleanup(&dir);
    });
}

// ─── Schema Evolution: Add Attribute to Existing Type ──────────────

#[test]
fn schema_evolution_add_attribute() {
    async_std::task::block_on(async {
        let dir = test_dir("schema_redefine");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define initial schema: person with name only
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Insert initial data
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\";").await.unwrap();
            tx.commit().await.unwrap();

            // Evolve schema: add email attribute and make person own it
            // Both the new attribute type and the new owns capability use `define`
            // (redefine is only for replacing existing capabilities)
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define attribute email, value string;").await.unwrap();
            tx.query("define entity person, owns email;").await.unwrap();
            tx.commit().await.unwrap();

            // Insert new data using the evolved schema
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Bob\", has email \"bob@example.com\";")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Verify: old data (Alice with name) is still queryable
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person, has name \"Alice\";").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Alice should still be queryable after schema evolution");

            // Verify: new data (Bob with email) is accessible
            let answer = tx
                .query("match $p isa person, has email $e;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Bob should be queryable with email");
            let email = rows[0].get("e").unwrap().unwrap();
            assert_eq!(email.try_get_string(), Some("bob@example.com"));
        }
        cleanup(&dir);
    });
}

// ─── Multiple Writes in One Transaction ────────────────────────────

#[test]
fn multiple_writes_in_one_transaction() {
    async_std::task::block_on(async {
        let dir = test_dir("multi_write_tx");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // In a single write transaction: insert 3, delete 1
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\";").await.unwrap();
            tx.query("insert $p isa person, has name \"Bob\";").await.unwrap();
            tx.query("insert $p isa person, has name \"Charlie\";").await.unwrap();

            // Delete Bob within the same transaction
            let answer = tx
                .query("match $p isa person, has name \"Bob\"; delete $p;")
                .await
                .unwrap();
            if answer.is_row_stream() {
                let _rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            }

            tx.commit().await.unwrap();

            // Verify: only Alice and Charlie remain
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person, has name $n;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(
                rows.len(),
                2,
                "Should have 2 people after inserting 3 and deleting 1 in one transaction"
            );

            let names: Vec<&str> = rows
                .iter()
                .filter_map(|row| row.get("n").ok()?.and_then(|c| c.try_get_string()))
                .collect();
            assert!(names.contains(&"Alice"), "Alice should remain");
            assert!(names.contains(&"Charlie"), "Charlie should remain");
        }
        cleanup(&dir);
    });
}

// ─── Write After Schema Change ─────────────────────────────────────

#[test]
fn write_after_schema_change() {
    async_std::task::block_on(async {
        let dir = test_dir("write_after_schema");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Schema transaction: define person
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // New write transaction: schema should be visible
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\";").await.unwrap();
            tx.commit().await.unwrap();

            // Verify the data was written
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person, has name $n;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find the inserted person");
            let name = rows[0].get("n").unwrap().unwrap();
            assert_eq!(name.try_get_string(), Some("Alice"));
        }
        cleanup(&dir);
    });
}
