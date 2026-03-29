/*
 * Integration tests for multiple database lifecycle management in the
 * embedded TypeDB backend.
 *
 * These tests verify that an embedded driver can manage several independent
 * databases: creation, listing, deletion, schema isolation, and persistence
 * across driver reopens.
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
        "thyra_test_multidb_{}_{}",
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

// ---- 1. Create multiple databases -------------------------------------------

#[test]
fn create_multiple_databases() {
    let dir = test_dir("create_multi");
    {
        let driver = TypeDBDriver::new_embedded(&dir).unwrap();
        let db_mgr = driver.embedded_databases().unwrap();

        db_mgr.put_database("alpha").unwrap();
        db_mgr.put_database("beta").unwrap();
        db_mgr.put_database("gamma").unwrap();

        let names = db_mgr.database_names();
        assert!(names.contains(&"alpha".to_string()), "Should contain alpha");
        assert!(names.contains(&"beta".to_string()), "Should contain beta");
        assert!(names.contains(&"gamma".to_string()), "Should contain gamma");
    }
    cleanup(&dir);
}

// ---- 2. Independent schemas per database ------------------------------------

#[test]
fn independent_schemas_per_database() {
    async_std::task::block_on(async {
        let dir = test_dir("independent_schemas");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            let db_mgr = driver.embedded_databases().unwrap();
            db_mgr.put_database("people_db").unwrap();
            db_mgr.put_database("products_db").unwrap();

            // Define different schemas in each database
            let tx = driver
                .transaction("people_db", TransactionType::Schema)
                .await
                .unwrap();
            tx.query("define entity person, owns name; attribute name, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            let tx = driver
                .transaction("products_db", TransactionType::Schema)
                .await
                .unwrap();
            tx.query("define entity product, owns sku; attribute sku, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Insert data into each
            let tx = driver
                .transaction("people_db", TransactionType::Write)
                .await
                .unwrap();
            tx.query(r#"insert $p isa person, has name "Alice";"#)
                .await
                .unwrap();
            tx.commit().await.unwrap();

            let tx = driver
                .transaction("products_db", TransactionType::Write)
                .await
                .unwrap();
            tx.query(r#"insert $p isa product, has sku "SKU-001";"#)
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Query people_db: should find person, not product
            let tx = driver
                .transaction("people_db", TransactionType::Read)
                .await
                .unwrap();
            let answer = tx
                .query("match $p isa person, has name $n;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "people_db should have 1 person");
            let name = rows[0].get("n").unwrap().unwrap();
            assert_eq!(name.try_get_string(), Some("Alice"));

            // people_db should NOT have product type
            let result = tx.query("match $p isa product;").await;
            assert!(
                result.is_err(),
                "people_db should not know about 'product' type"
            );

            // Query products_db: should find product, not person
            let tx = driver
                .transaction("products_db", TransactionType::Read)
                .await
                .unwrap();
            let answer = tx
                .query("match $p isa product, has sku $s;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "products_db should have 1 product");
            let sku = rows[0].get("s").unwrap().unwrap();
            assert_eq!(sku.try_get_string(), Some("SKU-001"));

            // products_db should NOT have person type
            let result = tx.query("match $p isa person;").await;
            assert!(
                result.is_err(),
                "products_db should not know about 'person' type"
            );
        }
        cleanup(&dir);
    });
}

// ---- 3. Database listing ----------------------------------------------------

#[test]
fn database_listing() {
    let dir = test_dir("listing");
    {
        let driver = TypeDBDriver::new_embedded(&dir).unwrap();
        let db_mgr = driver.embedded_databases().unwrap();

        db_mgr.put_database("first").unwrap();
        db_mgr.put_database("second").unwrap();
        db_mgr.put_database("third").unwrap();

        let names = db_mgr.database_names();
        // Filter to only user databases (exclude internal ones)
        let user_names: Vec<&String> = names
            .iter()
            .filter(|n| {
                *n == "first" || *n == "second" || *n == "third"
            })
            .collect();
        assert_eq!(
            user_names.len(),
            3,
            "Should list all 3 created databases, got: {:?}",
            names
        );
    }
    cleanup(&dir);
}

// ---- 4. Database deletion ---------------------------------------------------

#[test]
fn database_deletion() {
    async_std::task::block_on(async {
        let dir = test_dir("deletion");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            let db_mgr = driver.embedded_databases().unwrap();

            db_mgr.put_database("keep1").unwrap();
            db_mgr.put_database("remove_me").unwrap();
            db_mgr.put_database("keep2").unwrap();

            // Put some data in each so they are non-trivial
            for db in &["keep1", "remove_me", "keep2"] {
                let tx = driver
                    .transaction(db, TransactionType::Schema)
                    .await
                    .unwrap();
                tx.query("define entity item, owns label; attribute label, value string;")
                    .await
                    .unwrap();
                tx.commit().await.unwrap();

                let tx = driver
                    .transaction(db, TransactionType::Write)
                    .await
                    .unwrap();
                tx.query(&format!(r#"insert $i isa item, has label "{}";"#, db))
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }

            // Delete one database
            db_mgr.delete_database("remove_me").unwrap();

            // Verify it is gone
            let names = db_mgr.database_names();
            assert!(
                !names.contains(&"remove_me".to_string()),
                "Deleted database should not appear in listing"
            );
            assert!(
                db_mgr.database("remove_me").is_none(),
                "Deleted database should not be retrievable"
            );

            // Verify remaining databases still work
            for db in &["keep1", "keep2"] {
                let tx = driver
                    .transaction(db, TransactionType::Read)
                    .await
                    .unwrap();
                let answer = tx
                    .query("match $i isa item, has label $l;")
                    .await
                    .unwrap();
                let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
                assert_eq!(
                    rows.len(),
                    1,
                    "Database '{}' should still have its data after sibling deletion",
                    db
                );
            }
        }
        cleanup(&dir);
    });
}

// ---- 5. Create duplicate database is idempotent -----------------------------

#[test]
fn create_duplicate_database_is_idempotent() {
    async_std::task::block_on(async {
        let dir = test_dir("dup_idempotent");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            let db_mgr = driver.embedded_databases().unwrap();

            db_mgr.put_database("test").unwrap();

            // Second put_database with the same name: should not panic.
            // It may succeed (idempotent) or return an error, but the database
            // must remain usable either way.
            let _ = db_mgr.put_database("test");

            // Verify database is still functional
            let tx = driver
                .transaction("test", TransactionType::Schema)
                .await
                .unwrap();
            tx.query("define entity widget;").await.unwrap();
            tx.commit().await.unwrap();

            let tx = driver
                .transaction("test", TransactionType::Write)
                .await
                .unwrap();
            tx.query("insert $w isa widget;").await.unwrap();
            tx.commit().await.unwrap();

            let tx = driver
                .transaction("test", TransactionType::Read)
                .await
                .unwrap();
            let answer = tx.query("match $w isa widget;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(
                rows.len(),
                1,
                "Database should be fully functional after duplicate put_database"
            );
        }
        cleanup(&dir);
    });
}

// ---- 6. Databases persist across reopens ------------------------------------

#[test]
fn databases_persist_across_reopens() {
    async_std::task::block_on(async {
        let dir = test_dir("persist_reopen");

        // First driver session: create 2 databases with data
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            let db_mgr = driver.embedded_databases().unwrap();

            db_mgr.put_database("db_one").unwrap();
            db_mgr.put_database("db_two").unwrap();

            // Schema + data in db_one
            let tx = driver
                .transaction("db_one", TransactionType::Schema)
                .await
                .unwrap();
            tx.query("define entity user, owns email; attribute email, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            let tx = driver
                .transaction("db_one", TransactionType::Write)
                .await
                .unwrap();
            tx.query(r#"insert $u isa user, has email "alice@example.com";"#)
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Schema + data in db_two
            let tx = driver
                .transaction("db_two", TransactionType::Schema)
                .await
                .unwrap();
            tx.query("define entity event, owns name; attribute name, value string;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            let tx = driver
                .transaction("db_two", TransactionType::Write)
                .await
                .unwrap();
            tx.query(r#"insert $e isa event, has name "launch";"#)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        // Driver dropped -- all state must be persisted

        // Second driver session: verify both databases and their data survived
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            let db_mgr = driver.embedded_databases().unwrap();

            let names = db_mgr.database_names();
            assert!(
                names.contains(&"db_one".to_string()),
                "db_one should persist across reopen"
            );
            assert!(
                names.contains(&"db_two".to_string()),
                "db_two should persist across reopen"
            );

            // Verify db_one data
            let tx = driver
                .transaction("db_one", TransactionType::Read)
                .await
                .unwrap();
            let answer = tx
                .query("match $u isa user, has email $e;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "db_one should have 1 user after reopen");
            let email = rows[0].get("e").unwrap().unwrap();
            assert_eq!(email.try_get_string(), Some("alice@example.com"));

            // Verify db_two data
            let tx = driver
                .transaction("db_two", TransactionType::Read)
                .await
                .unwrap();
            let answer = tx
                .query("match $e isa event, has name $n;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "db_two should have 1 event after reopen");
            let name = rows[0].get("n").unwrap().unwrap();
            assert_eq!(name.try_get_string(), Some("launch"));
        }

        cleanup(&dir);
    });
}
