/*
 * Snapshot tests for the embedded TypeDB backend.
 *
 * These tests use the `insta` crate to capture and verify the textual
 * output of queries, errors, and type introspection. On first run they
 * create `.snap` files which should be reviewed and committed.
 *
 * Run: cargo test -p typedb-driver --test test_embedded_snapshots --no-default-features --features embedded
 * Review snapshots: cargo insta review
 */

#![cfg(feature = "embedded")]

use std::path::PathBuf;

use futures::TryStreamExt;
use typedb_driver::{
    answer::ConceptRow,
    TransactionType, TypeDBDriver,
};

/// Create a unique temp directory for each test.
fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "thyra_snapshot_test_{}_{}",
        name,
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Clean up a test directory.
fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Create a driver with a "test" database ready to go.
fn driver_with_db(dir: &PathBuf) -> TypeDBDriver {
    let driver = TypeDBDriver::new_embedded(dir).unwrap();
    driver.embedded_databases().unwrap().put_database("test").unwrap();
    driver
}

// ─── Snapshot: Person Query Debug Output ───────────────────────────

#[test]
fn snapshot_person_query_debug() {
    async_std::task::block_on(async {
        let dir = test_dir("person_debug");
        {
            let driver = driver_with_db(&dir);

            // Define schema
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define entity person, owns name, owns age; \
                 attribute name, value string; \
                 attribute age, value integer;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert Alice
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\", has age 30;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Query and snapshot the Debug output
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx
                .query("match $p isa person, has name $n, has age $a;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find exactly 1 person");

            // Extract fields in deterministic order, redacting non-deterministic IDs
            let row = &rows[0];
            let p = row.get("p").unwrap().unwrap();
            let n = row.get("n").unwrap().unwrap();
            let a = row.get("a").unwrap().unwrap();

            let id_re = regex::Regex::new(r"0x[0-9a-f]+").unwrap();
            let p_debug = id_re.replace_all(&format!("{:?}", p), "0x[ID]").to_string();
            let n_debug = format!("{:?}", n);
            let a_debug = format!("{:?}", a);

            let snapshot = format!(
                "person: {}\nname: {}\nage: {}",
                p_debug, n_debug, a_debug
            );
            insta::assert_snapshot!(snapshot);
        }
        cleanup(&dir);
    });
}

// ─── Snapshot: Entity Types After Schema ───────────────────────────

#[test]
fn snapshot_entity_types_after_schema() {
    async_std::task::block_on(async {
        let dir = test_dir("entity_types");
        {
            let driver = driver_with_db(&dir);

            // Define three entity types
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person; entity animal; entity document;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Query all entity types
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match entity $t;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();

            let mut labels: Vec<String> = rows
                .iter()
                .filter_map(|row| row.get("t").ok()?.map(|c| c.get_label().to_string()))
                .collect();
            labels.sort();

            insta::assert_yaml_snapshot!(labels);
        }
        cleanup(&dir);
    });
}

// ─── Snapshot: Error Message for Invalid TypeQL ────────────────────

#[test]
fn snapshot_error_message_invalid_typeql() {
    async_std::task::block_on(async {
        let dir = test_dir("err_invalid_typeql");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            let result = tx.query("this is not valid typeql at all!!!").await;
            assert!(result.is_err(), "Garbage TypeQL should produce an error");

            let error_string = format!("{}", result.unwrap_err());
            insta::assert_snapshot!(error_string);
        }
        cleanup(&dir);
    });
}

// ─── Snapshot: Error Message for Wrong Database ────────────────────

#[test]
fn snapshot_error_message_wrong_database() {
    async_std::task::block_on(async {
        let dir = test_dir("err_wrong_db");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            // Do NOT create any database — attempt to open a tx on a nonexistent one
            let result = driver
                .transaction("nonexistent_database", TransactionType::Read)
                .await;
            assert!(result.is_err(), "Opening tx on nonexistent db should error");

            let error_string = format!("{}", result.unwrap_err());
            insta::assert_snapshot!(error_string);
        }
        cleanup(&dir);
    });
}
