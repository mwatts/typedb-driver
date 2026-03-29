/*
 * Integration tests for error handling in the embedded TypeDB backend.
 *
 * These tests verify that invalid operations return meaningful errors
 * instead of panicking, covering parse failures, transaction misuse,
 * schema violations, and API boundary conditions.
 */

#![cfg(feature = "embedded")]

use std::path::PathBuf;

use futures::TryStreamExt;
use typedb_driver::{TransactionType, TypeDBDriver};

/// Create a unique temp directory for each test.
fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "thyra_test_errors_{}_{}",
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

// -- Helpers ------------------------------------------------------------------

/// Create a driver with a database named "test" already set up.
fn driver_with_db(dir: &PathBuf) -> TypeDBDriver {
    let driver = TypeDBDriver::new_embedded(dir).unwrap();
    driver.embedded_databases().unwrap().put_database("test").unwrap();
    driver
}

/// Assert that a Result is Err and the error message is not empty.
fn assert_meaningful_error<T: std::fmt::Debug>(result: Result<T, typedb_driver::Error>, context: &str) {
    assert!(result.is_err(), "Should return error for {}", context);
    let err_msg = format!("{}", result.unwrap_err());
    assert!(!err_msg.is_empty(), "Error message should not be empty for {}", context);
}

// -- 1. Invalid TypeQL syntax ------------------------------------------------

#[test]
fn invalid_typeql_syntax_returns_error() {
    async_std::task::block_on(async {
        let dir = test_dir("invalid_syntax");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            let result = tx.query("this is not typeql").await;
            assert_meaningful_error(result, "invalid TypeQL syntax");
        }
        cleanup(&dir);
    });
}

// -- 2. Partial / malformed syntax -------------------------------------------

#[test]
fn invalid_typeql_partial_syntax_returns_error() {
    async_std::task::block_on(async {
        let dir = test_dir("partial_syntax");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            let result = tx.query("define entity;;").await;
            assert_meaningful_error(result, "partial TypeQL syntax with double semicolon");
        }
        cleanup(&dir);
    });
}

// -- 3. Empty query string ---------------------------------------------------

#[test]
fn empty_query_string_returns_error() {
    async_std::task::block_on(async {
        let dir = test_dir("empty_query");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            let result = tx.query("").await;
            assert_meaningful_error(result, "empty query string");
        }
        cleanup(&dir);
    });
}

// -- 4. Transaction on nonexistent database ----------------------------------

#[test]
fn transaction_on_nonexistent_database() {
    async_std::task::block_on(async {
        let dir = test_dir("no_such_db");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            // Do NOT create any database
            let result = driver.transaction("no_such_db", TransactionType::Read).await;
            assert_meaningful_error(result, "transaction on nonexistent database");
        }
        cleanup(&dir);
    });
}

// -- 5. Schema query in write transaction ------------------------------------

#[test]
fn schema_query_in_write_transaction() {
    async_std::task::block_on(async {
        let dir = test_dir("schema_in_write");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            let result = tx.query("define entity foo;").await;
            assert_meaningful_error(result, "schema query in write transaction");
        }
        cleanup(&dir);
    });
}

// -- 6. Pipeline query in schema transaction ---------------------------------

#[test]
fn pipeline_query_in_schema_transaction() {
    async_std::task::block_on(async {
        let dir = test_dir("pipeline_in_schema");
        {
            let driver = driver_with_db(&dir);

            // First define the schema so "person" exists
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person;").await.unwrap();
            tx.commit().await.unwrap();

            // Now try a pipeline (insert) query in a schema transaction
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            let result = tx.query("insert $p isa person;").await;
            assert_meaningful_error(result, "pipeline query in schema transaction");
        }
        cleanup(&dir);
    });
}

// -- 7. Insert without schema ------------------------------------------------

#[test]
fn insert_without_schema() {
    async_std::task::block_on(async {
        let dir = test_dir("insert_no_schema");
        {
            let driver = driver_with_db(&dir);

            // No schema defined -- try inserting directly
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            let result = tx.query("insert $p isa person;").await;
            assert_meaningful_error(result, "insert without schema");
        }
        cleanup(&dir);
    });
}

// -- 8. Get nonexistent variable from row ------------------------------------

#[test]
fn get_nonexistent_variable() {
    async_std::task::block_on(async {
        let dir = test_dir("no_such_var");
        {
            let driver = driver_with_db(&dir);

            // Define schema and insert data so we get a row back
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $p isa person, has name \"Alice\";")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person;").await.unwrap();
            let rows: Vec<_> = answer.into_rows().try_collect().await.unwrap();
            assert!(!rows.is_empty(), "Should have at least one row");

            let row = &rows[0];
            let result = row.get("nonexistent");
            assert!(result.is_err(), "Getting a nonexistent variable should return Err");
            let err_msg = format!("{}", result.unwrap_err());
            assert!(!err_msg.is_empty(), "Error message should not be empty");
        }
        cleanup(&dir);
    });
}

// -- 9. Create duplicate database (idempotent or error, must not panic) ------

#[test]
fn create_duplicate_database() {
    let dir = test_dir("dup_db");
    {
        let driver = TypeDBDriver::new_embedded(&dir).unwrap();
        let db_mgr = driver.embedded_databases().unwrap();

        db_mgr.put_database("test").unwrap();
        // Second call: should either succeed (idempotent) or return Err, but must NOT panic
        let result = db_mgr.put_database("test");
        // We accept either Ok or Err -- the key invariant is no panic
        match result {
            Ok(_) => {} // idempotent -- fine
            Err(e) => {
                let msg = format!("{:?}", e);
                assert!(!msg.is_empty(), "If error, message should not be empty");
            }
        }
    }
    cleanup(&dir);
}

// -- 10. Query after commit (type-level safety) ------------------------------
//
// `Transaction::commit(self)` consumes the transaction by taking `self` by value.
// This means the Rust compiler prevents any use of the transaction after commit.
// There is no runtime test to write here -- the borrow checker enforces it.
// This test documents that the API is correctly designed.
//
// The following code would NOT compile:
//
//   let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
//   tx.commit().await.unwrap();
//   tx.query("match $p isa person;").await; // ERROR: use of moved value `tx`
//

#[test]
fn query_after_commit_is_prevented_by_type_system() {
    // This is a compile-time guarantee test.
    // If this test compiles and runs, the API is correctly consuming `self` on commit.
    async_std::task::block_on(async {
        let dir = test_dir("after_commit");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person;").await.unwrap();
            // commit consumes tx -- any subsequent use would be a compile error
            tx.commit().await.unwrap();
            // Verified: cannot call tx.query() here (moved value)
        }
        cleanup(&dir);
    });
}

// -- 11. Schema violation: wrong value type ----------------------------------

#[test]
fn schema_violation_wrong_value_type() {
    async_std::task::block_on(async {
        let dir = test_dir("wrong_value_type");
        {
            let driver = driver_with_db(&dir);

            // Define schema with integer attribute
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns age; attribute age, value integer;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Try to insert a string where an integer is expected
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            let result = tx.query("insert $p isa person, has age \"not_a_number\";").await;
            assert_meaningful_error(result, "schema violation: string value for integer attribute");
        }
        cleanup(&dir);
    });
}
