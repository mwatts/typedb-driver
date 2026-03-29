/*
 * Integration tests for value type roundtrips via the embedded TypeDB backend.
 *
 * Each test defines a minimal schema, inserts a value of a specific type,
 * reads it back, and verifies the driver's accessor method returns the
 * expected value.
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
    let dir = std::env::temp_dir().join(format!("thyra_values_test_{}_{}", name, rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Clean up a test directory.
fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Helper: create driver + database, define schema, return driver.
fn setup(dir: &PathBuf, schema: &str) -> TypeDBDriver {
    async_std::task::block_on(async {
        let driver = TypeDBDriver::new_embedded(dir).unwrap();
        driver.embedded_databases().unwrap().put_database("test").unwrap();

        let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
        tx.query(schema).await.unwrap();
        tx.commit().await.unwrap();

        driver
    })
}

/// Helper: insert a statement, commit.
async fn insert(driver: &TypeDBDriver, query: &str) {
    let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
    tx.query(query).await.unwrap();
    tx.commit().await.unwrap();
}

/// Helper: run a match query and collect all rows.
async fn query_rows(driver: &TypeDBDriver, query: &str) -> Vec<ConceptRow> {
    let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
    let answer = tx.query(query).await.unwrap();
    answer.into_rows().try_collect().await.unwrap()
}

// ---- String value roundtrips ------------------------------------------------

#[test]
fn string_value_roundtrip() {
    async_std::task::block_on(async {
        let dir = test_dir("string");
        {
            let driver = setup(&dir, "define entity person, owns name; attribute name, value string;");
            insert(&driver, r#"insert $p isa person, has name "Hello World";"#).await;

            let rows = query_rows(&driver, "match $p isa person, has name $n;").await;
            assert_eq!(rows.len(), 1);

            let name = rows[0].get("n").unwrap().unwrap();
            assert!(name.is_string(), "Expected string attribute");
            assert_eq!(name.try_get_string(), Some("Hello World"));
        }
        cleanup(&dir);
    });
}

#[test]
fn string_empty_roundtrip() {
    async_std::task::block_on(async {
        let dir = test_dir("string_empty");
        {
            let driver = setup(&dir, "define entity person, owns name; attribute name, value string;");
            insert(&driver, r#"insert $p isa person, has name "";"#).await;

            let rows = query_rows(&driver, "match $p isa person, has name $n;").await;
            assert_eq!(rows.len(), 1);

            let name = rows[0].get("n").unwrap().unwrap();
            assert_eq!(name.try_get_string(), Some(""));
        }
        cleanup(&dir);
    });
}

#[test]
fn string_unicode_roundtrip() {
    async_std::task::block_on(async {
        let dir = test_dir("string_unicode");
        {
            let driver = setup(
                &dir,
                "define entity message, owns text; attribute text, value string;",
            );

            // Hebrew text
            insert(
                &driver,
                "insert $m isa message, has text \"\u{05E9}\u{05DC}\u{05D5}\u{05DD} \u{05E2}\u{05D5}\u{05DC}\u{05DD}\";",
            )
            .await;

            // Emoji text (separate entity)
            insert(
                &driver,
                "insert $m isa message, has text \"\u{1F980}\u{1F525}\";",
            )
            .await;

            let rows = query_rows(&driver, "match $m isa message, has text $t;").await;
            assert_eq!(rows.len(), 2, "Should find both unicode messages");

            let texts: Vec<&str> = rows
                .iter()
                .filter_map(|row| row.get("t").ok()?.and_then(|c| c.try_get_string()))
                .collect();

            assert!(
                texts.contains(&"\u{05E9}\u{05DC}\u{05D5}\u{05DD} \u{05E2}\u{05D5}\u{05DC}\u{05DD}"),
                "Should contain Hebrew text, got: {:?}",
                texts,
            );
            assert!(
                texts.contains(&"\u{1F980}\u{1F525}"),
                "Should contain emoji text, got: {:?}",
                texts,
            );
        }
        cleanup(&dir);
    });
}

// ---- Integer value roundtrips -----------------------------------------------

#[test]
fn integer_value_roundtrip() {
    async_std::task::block_on(async {
        let dir = test_dir("integer");
        {
            let driver = setup(
                &dir,
                "define entity person, owns age; attribute age, value integer;",
            );
            insert(&driver, "insert $p isa person, has age 42;").await;

            let rows = query_rows(&driver, "match $p isa person, has age $a;").await;
            assert_eq!(rows.len(), 1);

            let age = rows[0].get("a").unwrap().unwrap();
            assert!(age.is_integer(), "Expected integer attribute");
            assert_eq!(age.try_get_integer(), Some(42));
        }
        cleanup(&dir);
    });
}

#[test]
fn integer_zero_and_negative() {
    async_std::task::block_on(async {
        let dir = test_dir("integer_zero_neg");
        {
            let driver = setup(
                &dir,
                "define entity counter, owns count; attribute count, value integer;",
            );
            insert(&driver, "insert $c isa counter, has count 0;").await;
            insert(&driver, "insert $c isa counter, has count -100;").await;

            let rows = query_rows(&driver, "match $c isa counter, has count $v;").await;
            assert_eq!(rows.len(), 2, "Should find both zero and negative");

            let values: Vec<i64> = rows
                .iter()
                .filter_map(|row| row.get("v").ok()?.and_then(|c| c.try_get_integer()))
                .collect();

            assert!(values.contains(&0), "Should contain 0, got: {:?}", values);
            assert!(values.contains(&-100), "Should contain -100, got: {:?}", values);
        }
        cleanup(&dir);
    });
}

// ---- Double value roundtrips ------------------------------------------------

#[test]
fn double_value_roundtrip() {
    async_std::task::block_on(async {
        let dir = test_dir("double");
        {
            let driver = setup(
                &dir,
                "define entity measurement, owns score; attribute score, value double;",
            );
            insert(&driver, "insert $m isa measurement, has score 3.14159;").await;

            let rows = query_rows(&driver, "match $m isa measurement, has score $s;").await;
            assert_eq!(rows.len(), 1);

            let score = rows[0].get("s").unwrap().unwrap();
            assert!(score.is_double(), "Expected double attribute");
            let val = score.try_get_double().expect("Should have double value");
            assert!(
                (val - 3.14159).abs() < 1e-10,
                "Expected ~3.14159, got {}",
                val,
            );
        }
        cleanup(&dir);
    });
}

// ---- Boolean value roundtrips -----------------------------------------------

#[test]
fn boolean_true_and_false() {
    async_std::task::block_on(async {
        let dir = test_dir("boolean");
        {
            let driver = setup(
                &dir,
                "define entity flag, owns active, owns name; attribute active, value boolean; attribute name, value string;",
            );
            // Use a distinguishing attribute so we can tell which entity has which boolean
            insert(
                &driver,
                r#"insert $f isa flag, has name "on", has active true;"#,
            )
            .await;
            insert(
                &driver,
                r#"insert $f isa flag, has name "off", has active false;"#,
            )
            .await;

            let rows = query_rows(
                &driver,
                "match $f isa flag, has name $n, has active $a;",
            )
            .await;
            assert_eq!(rows.len(), 2);

            for row in &rows {
                let name = row.get("n").unwrap().unwrap();
                let active = row.get("a").unwrap().unwrap();
                assert!(active.is_boolean(), "Expected boolean attribute");

                match name.try_get_string() {
                    Some("on") => assert_eq!(active.try_get_boolean(), Some(true)),
                    Some("off") => assert_eq!(active.try_get_boolean(), Some(false)),
                    other => panic!("Unexpected name: {:?}", other),
                }
            }
        }
        cleanup(&dir);
    });
}

// ---- Date value roundtrips --------------------------------------------------

#[test]
fn date_value_roundtrip() {
    // TypeQL date literal syntax: 2024-03-28
    // If this syntax is not supported by the embedded backend, the test
    // will fail at the insert step rather than silently producing wrong data.
    async_std::task::block_on(async {
        let dir = test_dir("date");
        {
            let driver = setup(
                &dir,
                "define entity event, owns birthday; attribute birthday, value date;",
            );
            insert(&driver, "insert $e isa event, has birthday 2024-03-28;").await;

            let rows = query_rows(&driver, "match $e isa event, has birthday $d;").await;
            assert_eq!(rows.len(), 1);

            let date_concept = rows[0].get("d").unwrap().unwrap();
            assert!(date_concept.is_date(), "Expected date attribute");
            let date = date_concept.try_get_date().expect("Should have date value");
            assert_eq!(date.to_string(), "2024-03-28");
        }
        cleanup(&dir);
    });
}

// ---- Datetime value roundtrips ----------------------------------------------

#[test]
fn datetime_value_roundtrip() {
    // TypeQL datetime literal syntax: 2024-03-28T10:30:00
    // If this syntax is not supported, the test will fail at insert.
    async_std::task::block_on(async {
        let dir = test_dir("datetime");
        {
            let driver = setup(
                &dir,
                "define entity event, owns created_at; attribute created_at, value datetime;",
            );
            insert(&driver, "insert $e isa event, has created_at 2024-03-28T10:30:00;").await;

            let rows = query_rows(&driver, "match $e isa event, has created_at $dt;").await;
            assert_eq!(rows.len(), 1);

            let dt_concept = rows[0].get("dt").unwrap().unwrap();
            assert!(dt_concept.is_datetime(), "Expected datetime attribute");
            let dt = dt_concept.try_get_datetime().expect("Should have datetime value");
            assert_eq!(dt.format("%Y-%m-%dT%H:%M:%S").to_string(), "2024-03-28T10:30:00");
        }
        cleanup(&dir);
    });
}

// ---- Multiple value types in one query --------------------------------------

#[test]
fn multiple_value_types_in_one_query() {
    async_std::task::block_on(async {
        let dir = test_dir("multi_types");
        {
            let driver = setup(
                &dir,
                "define entity person, owns name, owns age, owns score, owns active;
                 attribute name, value string;
                 attribute age, value integer;
                 attribute score, value double;
                 attribute active, value boolean;",
            );
            insert(
                &driver,
                r#"insert $p isa person, has name "Alice", has age 30, has score 9.5, has active true;"#,
            )
            .await;

            let rows = query_rows(
                &driver,
                "match $p isa person, has name $n, has age $a, has score $s, has active $act;",
            )
            .await;
            assert_eq!(rows.len(), 1, "Should find exactly one person with all attributes");

            let row = &rows[0];

            let name = row.get("n").unwrap().unwrap();
            assert_eq!(name.try_get_string(), Some("Alice"));

            let age = row.get("a").unwrap().unwrap();
            assert_eq!(age.try_get_integer(), Some(30));

            let score = row.get("s").unwrap().unwrap();
            let score_val = score.try_get_double().expect("Should have double");
            assert!((score_val - 9.5).abs() < 1e-10, "Expected 9.5, got {}", score_val);

            let active = row.get("act").unwrap().unwrap();
            assert_eq!(active.try_get_boolean(), Some(true));
        }
        cleanup(&dir);
    });
}
