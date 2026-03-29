/*
 * Integration tests for hybrid workflows combining vector search, full-text
 * search, and TypeQL queries in the embedded TypeDB backend.
 *
 * These tests verify that vector/FTS results can be used as entity keys to
 * drive follow-up TypeQL queries, enabling semantic-search-augmented graph
 * traversals entirely in-process.
 */

#![cfg(feature = "embedded")]

use std::path::PathBuf;

use futures::TryStreamExt;
use typedb_driver::{
    answer::ConceptRow,
    TransactionType, TypeDBDriver, VectorSearchResult, FtsSearchResult,
};

/// Create a unique temp directory for each test.
fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "thyra_test_hybrid_{}_{}",
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

// ---- Helpers ----------------------------------------------------------------

/// Create a driver with a database and a document schema (entity document, owns title).
fn driver_with_document_schema(dir: &PathBuf, db_name: &str) -> TypeDBDriver {
    let driver = TypeDBDriver::new_embedded(dir).unwrap();
    driver.embedded_databases().unwrap().put_database(db_name).unwrap();

    async_std::task::block_on(async {
        let tx = driver.transaction(db_name, TransactionType::Schema).await.unwrap();
        tx.query("define entity document, owns title; attribute title, value string;")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    });

    driver
}

/// Create a driver with an article schema (entity article, owns title, owns body).
fn driver_with_article_schema(dir: &PathBuf, db_name: &str) -> TypeDBDriver {
    let driver = TypeDBDriver::new_embedded(dir).unwrap();
    driver.embedded_databases().unwrap().put_database(db_name).unwrap();

    async_std::task::block_on(async {
        let tx = driver.transaction(db_name, TransactionType::Schema).await.unwrap();
        tx.query(
            "define entity article, owns title, owns body; \
             attribute title, value string; \
             attribute body, value string;",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    });

    driver
}

// ---- 1. Vector search then TypeQL query -------------------------------------

#[test]
fn vector_then_typeql_query() {
    async_std::task::block_on(async {
        let dir = test_dir("vec_then_tql");
        {
            let db = "hybrid";
            let driver = driver_with_document_schema(&dir, db);

            // Insert 3 documents via TypeQL
            let tx = driver.transaction(db, TransactionType::Write).await.unwrap();
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

            // Index vectors keyed by document titles (as byte keys)
            let dim = 3;
            driver
                .vector_insert(db, "embeddings", b"about cats", &[1.0, 0.0, 0.0], dim)
                .unwrap();
            driver
                .vector_insert(db, "embeddings", b"about dogs", &[0.0, 1.0, 0.0], dim)
                .unwrap();
            driver
                .vector_insert(db, "embeddings", b"about fish", &[0.9, 0.1, 0.0], dim)
                .unwrap();

            // Vector search: nearest to "about cats" direction
            let results: Vec<VectorSearchResult> = driver
                .vector_search(db, "embeddings", &[1.0, 0.0, 0.0], 1, dim)
                .unwrap();
            assert_eq!(results.len(), 1, "Should return 1 nearest neighbor");

            // Use the returned entity_id (title bytes) to drive a TypeQL query
            let title = std::str::from_utf8(&results[0].entity_id)
                .expect("entity_id should be valid UTF-8");
            assert_eq!(title, "about cats");

            let query = format!(r#"match $d isa document, has title "{}";"#, title);
            let tx = driver.transaction(db, TransactionType::Read).await.unwrap();
            let answer = tx.query(&query).await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "TypeQL should find exactly 1 matching document");

            let doc = rows[0].get("d").unwrap().unwrap();
            assert!(doc.is_entity(), "Result should be an entity");
            assert_eq!(doc.get_label(), "document");
        }
        cleanup(&dir);
    });
}

// ---- 2. FTS search then TypeQL query ----------------------------------------

#[test]
fn fts_then_typeql_query() {
    async_std::task::block_on(async {
        let dir = test_dir("fts_then_tql");
        {
            let db = "hybrid_fts";
            let driver = driver_with_article_schema(&dir, db);

            // Insert articles via TypeQL
            let tx = driver.transaction(db, TransactionType::Write).await.unwrap();
            tx.query(
                r#"insert $a isa article, has title "rust-intro", has body "Rust is a systems programming language focused on safety";"#,
            )
            .await
            .unwrap();
            tx.query(
                r#"insert $a isa article, has title "python-intro", has body "Python is a dynamic scripting language for rapid development";"#,
            )
            .await
            .unwrap();
            tx.query(
                r#"insert $a isa article, has title "rust-async", has body "Async programming in Rust uses futures and the tokio runtime";"#,
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Index article text keyed by title
            driver
                .fts_index(db, "articles", "rust-intro", "Rust is a systems programming language focused on safety")
                .unwrap();
            driver
                .fts_index(db, "articles", "python-intro", "Python is a dynamic scripting language for rapid development")
                .unwrap();
            driver
                .fts_index(db, "articles", "rust-async", "Async programming in Rust uses futures and the tokio runtime")
                .unwrap();

            // FTS search for "Rust systems safety"
            let results: Vec<FtsSearchResult> = driver
                .fts_search(db, "articles", "Rust systems safety", 1)
                .unwrap();
            assert!(!results.is_empty(), "FTS should return at least 1 result");

            let entity_id = &results[0].entity_id;
            assert_eq!(entity_id, "rust-intro", "Best match should be rust-intro");

            // Use the entity_id as a title to query TypeQL
            let query = format!(r#"match $a isa article, has title "{}";"#, entity_id);
            let tx = driver.transaction(db, TransactionType::Read).await.unwrap();
            let answer = tx.query(&query).await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "TypeQL should find exactly 1 matching article");

            let article = rows[0].get("a").unwrap().unwrap();
            assert!(article.is_entity(), "Result should be an entity");
            assert_eq!(article.get_label(), "article");
        }
        cleanup(&dir);
    });
}

// ---- 3. Vector AND FTS combined ---------------------------------------------

#[test]
fn vector_and_fts_combined() {
    async_std::task::block_on(async {
        let dir = test_dir("vec_fts_combined");
        {
            let db = "hybrid_both";
            let driver = driver_with_article_schema(&dir, db);

            // Insert articles via TypeQL
            let titles = ["ai-safety", "ai-models", "web-security", "rust-perf", "ai-ethics"];
            let bodies = [
                "Artificial intelligence safety research and alignment",
                "Large language models and neural network architectures",
                "Web application security best practices and OWASP",
                "High performance computing with Rust and SIMD",
                "Ethics of artificial intelligence and bias in AI systems",
            ];
            // Vectors: AI-related docs cluster near [1,0,0], others elsewhere
            let vectors: Vec<[f32; 3]> = vec![
                [1.0, 0.0, 0.1],  // ai-safety
                [0.9, 0.1, 0.0],  // ai-models
                [0.0, 1.0, 0.0],  // web-security
                [0.0, 0.0, 1.0],  // rust-perf
                [0.8, 0.0, 0.2],  // ai-ethics
            ];

            let tx = driver.transaction(db, TransactionType::Write).await.unwrap();
            for i in 0..titles.len() {
                let q = format!(
                    r#"insert $a isa article, has title "{}", has body "{}";"#,
                    titles[i], bodies[i]
                );
                tx.query(&q).await.unwrap();
            }
            tx.commit().await.unwrap();

            // Index into both vector and FTS
            let dim = 3;
            for i in 0..titles.len() {
                driver
                    .vector_insert(db, "emb", titles[i].as_bytes(), &vectors[i], dim)
                    .unwrap();
                driver
                    .fts_index(db, "content", titles[i], bodies[i])
                    .unwrap();
            }

            // Vector search: nearest to [1, 0, 0] (AI cluster) -> top 3
            let vec_results: Vec<VectorSearchResult> = driver
                .vector_search(db, "emb", &[1.0, 0.0, 0.0], 3, dim)
                .unwrap();
            let vec_ids: Vec<String> = vec_results
                .iter()
                .map(|r| String::from_utf8(r.entity_id.clone()).unwrap())
                .collect();
            assert_eq!(vec_ids.len(), 3, "Vector search should return 3 results");

            // FTS search: "artificial intelligence" -> top 3
            let fts_results: Vec<FtsSearchResult> = driver
                .fts_search(db, "content", "artificial intelligence", 3)
                .unwrap();
            let fts_ids: Vec<&str> = fts_results
                .iter()
                .map(|r| r.entity_id.as_str())
                .collect();

            // Compute intersection: entities that appear in BOTH result sets
            let intersection: Vec<&str> = vec_ids
                .iter()
                .filter(|id| fts_ids.contains(&id.as_str()))
                .map(|id| id.as_str())
                .collect();
            assert!(
                !intersection.is_empty(),
                "Intersection of vector and FTS results should not be empty"
            );

            // Query each intersected entity via TypeQL
            let tx = driver.transaction(db, TransactionType::Read).await.unwrap();
            for entity_id in &intersection {
                let query = format!(r#"match $a isa article, has title "{}";"#, entity_id);
                let answer = tx.query(&query).await.unwrap();
                let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
                assert_eq!(
                    rows.len(),
                    1,
                    "TypeQL should find exactly 1 article for title '{}'",
                    entity_id
                );
                let article = rows[0].get("a").unwrap().unwrap();
                assert!(article.is_entity());
                assert_eq!(article.get_label(), "article");
            }
        }
        cleanup(&dir);
    });
}
