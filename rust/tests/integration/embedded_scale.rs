/*
 * Scale tests for the embedded TypeDB backend.
 *
 * These tests exercise the driver with larger data volumes to verify
 * correctness at scale. They are ignored by default and intended to
 * be run explicitly with `cargo test -- --ignored`.
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
        "thyra_scale_test_{}_{}",
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

/// Create a driver with a database named "test" and person schema.
fn driver_with_person_schema(dir: &PathBuf) -> TypeDBDriver {
    let driver = TypeDBDriver::new_embedded(dir).unwrap();
    driver.embedded_databases().unwrap().put_database("test").unwrap();

    async_std::task::block_on(async {
        let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
        tx.query(
            "define entity person, owns name, owns age; \
             attribute name, value string; \
             attribute age, value integer;",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    });

    driver
}

// ─── Scale Insert & Query ──────────────────────────────────────────

#[test]
#[ignore = "scale test — run with --ignored"]
fn large_insert_and_query_1000() {
    async_std::task::block_on(async {
        let dir = test_dir("insert_1000");
        {
            let driver = driver_with_person_schema(&dir);

            // Insert 1000 persons in a single write transaction
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            for i in 0..1000 {
                tx.query(&format!(
                    "insert $p isa person, has name \"person_{}\", has age {};",
                    i, i % 100
                ))
                .await
                .unwrap();
            }
            tx.commit().await.unwrap();

            // Query all persons and verify count
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(
                rows.len(),
                1000,
                "Should find exactly 1000 persons, found {}",
                rows.len()
            );
        }
        cleanup(&dir);
    });
}

// ─── Batch Insert with Selective Query ─────────────────────────────

#[test]
#[ignore = "scale test — run with --ignored"]
fn large_insert_batch_performance() {
    async_std::task::block_on(async {
        let dir = test_dir("batch_500");
        {
            let driver = driver_with_person_schema(&dir);

            // Insert 500 entities
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            for i in 0..500 {
                tx.query(&format!(
                    "insert $p isa person, has name \"batch_person_{}\", has age {};",
                    i, i
                ))
                .await
                .unwrap();
            }
            tx.commit().await.unwrap();

            // Query for a specific name match — should return exactly 1
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx
                .query("match $p isa person, has name \"batch_person_250\";")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(
                rows.len(),
                1,
                "Selective query should return exactly 1 result, found {}",
                rows.len()
            );
        }
        cleanup(&dir);
    });
}

// ─── Vector Search at Scale ────────────────────────────────────────

#[test]
#[ignore = "scale test — run with --ignored"]
fn vector_search_500() {
    use rand::Rng;

    async_std::task::block_on(async {
        let dir = test_dir("vector_500");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            let dims: usize = 8;
            let mut rng = rand::thread_rng();

            // Insert 500 random 8-dimensional vectors
            for i in 0..500 {
                let vec: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect();
                let id = format!("vec_{}", i);
                driver
                    .vector_insert("test", "scale_embeddings", id.as_bytes(), &vec, dims)
                    .unwrap();
            }

            // Search for nearest 10 to a known query vector
            let query_vec: Vec<f32> = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
            let results = driver
                .vector_search("test", "scale_embeddings", &query_vec, 10, dims)
                .unwrap();

            assert_eq!(
                results.len(),
                10,
                "Should return exactly 10 nearest neighbors, got {}",
                results.len()
            );

            // Verify distances are sorted ascending (each <= next)
            for window in results.windows(2) {
                assert!(
                    window[0].distance <= window[1].distance,
                    "Distances should be sorted ascending: {} <= {}",
                    window[0].distance,
                    window[1].distance
                );
            }
        }
        cleanup(&dir);
    });
}

// ─── Full-Text Search at Scale ─────────────────────────────────────

#[test]
#[ignore = "scale test — run with --ignored"]
fn fts_search_200_documents() {
    async_std::task::block_on(async {
        let dir = test_dir("fts_200");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Generate 200 documents with varying content
            let topics = [
                "rust programming language systems",
                "python machine learning data science",
                "javascript web development frontend",
                "database query optimization indexing",
                "network protocol distributed systems",
                "algorithm complexity sorting searching",
                "operating system kernel memory management",
                "compiler parser lexer abstract syntax tree",
                "cryptography encryption security hashing",
                "cloud computing infrastructure deployment",
            ];

            for i in 0..200 {
                let topic = topics[i % topics.len()];
                // Create varying-length documents by repeating and mixing content
                let doc_text = format!(
                    "Document {} about {}. This is a test document number {} covering topics \
                     related to {} with additional context for full text search testing. \
                     The quick brown fox jumps over the lazy dog for document {}.",
                    i, topic, i, topic, i
                );
                let doc_id = format!("doc_{}", i);
                driver
                    .fts_index("test", "documents", &doc_id, &doc_text)
                    .unwrap();
            }

            // Search for a specific term that appears in a known subset
            let results = driver.fts_search("test", "documents", "cryptography encryption", 50).unwrap();
            assert!(
                !results.is_empty(),
                "Search for 'cryptography encryption' should return results"
            );

            // Every 10th document starting at index 8 should match the cryptography topic
            // (indices 8, 18, 28, ..., 198 = 20 documents)
            assert!(
                results.len() >= 1 && results.len() <= 50,
                "Should return a reasonable number of results (1..50), got {}",
                results.len()
            );

            // Search for a term present in all documents
            let universal_results = driver.fts_search("test", "documents", "document", 200).unwrap();
            assert!(
                universal_results.len() > 100,
                "Search for 'document' should match most documents, found {}",
                universal_results.len()
            );
        }
        cleanup(&dir);
    });
}
