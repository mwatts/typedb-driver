/*
 * Property-based tests for the embedded TypeDB backend using proptest.
 *
 * These tests verify invariants that should hold for *any* valid input,
 * not just hand-picked examples: round-trip fidelity for strings and
 * integers, robustness against arbitrary query strings, and vector
 * search contract compliance (result count, ordering).
 */

#![cfg(feature = "embedded")]

use std::path::PathBuf;

use futures::TryStreamExt;
use proptest::prelude::*;
use typedb_driver::{answer::ConceptRow, TransactionType, TypeDBDriver};

/// Create a driver with a person schema (name: string, age: integer).
fn setup_driver(test_name: &str) -> (PathBuf, TypeDBDriver) {
    let dir = std::env::temp_dir().join(format!(
        "thyra_prop_{}_{}",
        test_name,
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let driver = TypeDBDriver::new_embedded(&dir).unwrap();
    driver
        .embedded_databases()
        .unwrap()
        .put_database("test")
        .unwrap();
    async_std::task::block_on(async {
        let tx = driver
            .transaction("test", TransactionType::Schema)
            .await
            .unwrap();
        tx.query(
            "define entity person, owns name, owns age; \
             attribute name, value string; \
             attribute age, value integer;",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    });
    (dir, driver)
}

// ---- 1. Arbitrary query strings never panic ---------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]
    #[test]
    fn arbitrary_string_never_panics(query in "\\PC{0,200}") {
        let (dir, driver) = setup_driver("fuzz");
        async_std::task::block_on(async {
            let tx = driver
                .transaction("test", TransactionType::Schema)
                .await
                .unwrap();
            let _ = tx.query(&query).await; // Ok or Err, never panic
        });
        drop(driver);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ---- 2. String attribute round-trip -----------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]
    #[test]
    fn string_attribute_roundtrip(name in "[a-zA-Z][a-zA-Z0-9 ]{0,50}") {
        let (dir, driver) = setup_driver("str_rt");
        let result: Result<(), TestCaseError> = async_std::task::block_on(async {
            // Insert
            let tx = driver
                .transaction("test", TransactionType::Write)
                .await
                .unwrap();
            let query = format!(r#"insert $p isa person, has name "{}";"#, name);
            tx.query(&query).await.unwrap();
            tx.commit().await.unwrap();

            // Read back
            let tx = driver
                .transaction("test", TransactionType::Read)
                .await
                .unwrap();
            let answer = tx
                .query("match $p isa person, has name $n;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            prop_assert!(rows.len() >= 1);
            let found_name = rows[0]
                .get("n")
                .unwrap()
                .unwrap()
                .try_get_string()
                .unwrap();
            prop_assert_eq!(found_name, name.as_str());
            Ok(())
        });
        drop(driver);
        let _ = std::fs::remove_dir_all(&dir);
        result?;
    }
}

// ---- 3. Integer attribute round-trip ----------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]
    #[test]
    fn integer_attribute_roundtrip(age in 0i64..1_000_000) {
        let (dir, driver) = setup_driver("int_rt");
        let result: Result<(), TestCaseError> = async_std::task::block_on(async {
            // Insert
            let tx = driver
                .transaction("test", TransactionType::Write)
                .await
                .unwrap();
            let query = format!("insert $p isa person, has age {};", age);
            tx.query(&query).await.unwrap();
            tx.commit().await.unwrap();

            // Read back
            let tx = driver
                .transaction("test", TransactionType::Read)
                .await
                .unwrap();
            let answer = tx
                .query("match $p isa person, has age $a;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            prop_assert!(rows.len() >= 1);
            let found_age = rows[0]
                .get("a")
                .unwrap()
                .unwrap()
                .try_get_integer()
                .unwrap();
            prop_assert_eq!(found_age, age);
            Ok(())
        });
        drop(driver);
        let _ = std::fs::remove_dir_all(&dir);
        result?;
    }
}

// ---- 4. Vector search respects k limit --------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]
    #[test]
    fn vector_search_respects_k(k in 1usize..20, n in 1usize..50) {
        let (dir, driver) = setup_driver("vec_k");

        // Insert n random 3-D vectors with deterministic seed from n.
        use rand::rngs::SmallRng;
        use rand::{Rng, SeedableRng};
        let mut rng = SmallRng::seed_from_u64(n as u64);
        for i in 0..n {
            let v: [f32; 3] = [rng.gen(), rng.gen(), rng.gen()];
            let id = format!("v{}", i);
            driver
                .vector_insert("test", "emb", id.as_bytes(), &v, 3)
                .unwrap();
        }

        let results = driver
            .vector_search("test", "emb", &[0.5, 0.5, 0.5], k, 3)
            .unwrap();

        let expected_max = std::cmp::min(k, n);
        let actual_len = results.len();
        drop(driver);
        let _ = std::fs::remove_dir_all(&dir);
        prop_assert!(
            actual_len <= expected_max,
            "Expected at most {} results, got {}",
            expected_max,
            actual_len
        );
    }
}

// ---- 5. Vector results sorted by distance -----------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    #[test]
    fn vector_results_sorted_by_distance(seed in 0u64..10000) {
        let (dir, driver) = setup_driver("vec_sort");

        use rand::rngs::SmallRng;
        use rand::{Rng, SeedableRng};
        let mut rng = SmallRng::seed_from_u64(seed);

        // Insert 20 vectors with 3 dimensions.
        for i in 0..20u32 {
            let v: [f32; 3] = [rng.gen(), rng.gen(), rng.gen()];
            let id = i.to_be_bytes();
            driver
                .vector_insert("test", "emb", &id, &v, 3)
                .unwrap();
        }

        let query_vec: [f32; 3] = [rng.gen(), rng.gen(), rng.gen()];
        let results = driver
            .vector_search("test", "emb", &query_vec, 10, 3)
            .unwrap();

        let distances: Vec<f32> = results.iter().map(|r| r.distance).collect();
        drop(driver);
        let _ = std::fs::remove_dir_all(&dir);

        // Verify distances are non-decreasing.
        for window in distances.windows(2) {
            prop_assert!(
                window[0] <= window[1],
                "Results not sorted: distance {} > {}",
                window[0],
                window[1]
            );
        }
    }
}
