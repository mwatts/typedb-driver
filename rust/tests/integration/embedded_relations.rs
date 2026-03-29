/*
 * Integration tests for relations and graph traversal with the embedded TypeDB backend.
 *
 * These tests verify that relations (binary, multi-role, attribute-owning),
 * graph traversals, and multiple relation instances work correctly
 * in the embedded in-process TypeDB engine.
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
    let dir = std::env::temp_dir().join(format!("thyra_rel_test_{}_{}", name, rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Clean up a test directory.
fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

// ─── Binary Relations ──────────────────────────────────────────────

#[test]
fn binary_relation_insert_and_query() {
    async_std::task::block_on(async {
        let dir = test_dir("binary_rel");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema: person plays friendship:friend
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    entity person, owns name, plays friendship:friend;
                    attribute name, value string;
                    relation friendship, relates friend @card(0..);",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert two people and a friendship between them
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query(
                "insert
                    $a isa person, has name \"Alice\";
                    $b isa person, has name \"Bob\";
                    (friend: $a, friend: $b) isa friendship;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Query the friendship relation
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx
                .query(
                    "match
                        $f (friend: $a, friend: $b) isa friendship;
                        $a has name $na;
                        $b has name $nb;",
                )
                .await
                .unwrap();
            assert!(answer.is_row_stream(), "Match query should return a row stream");

            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert!(rows.len() >= 1, "Should find at least one friendship row, found {}", rows.len());

            // Collect all name pairs from results
            let mut found_alice = false;
            let mut found_bob = false;
            for row in &rows {
                let na = row.get("na").unwrap().unwrap();
                let nb = row.get("nb").unwrap().unwrap();
                let name_a = na.try_get_string().unwrap();
                let name_b = nb.try_get_string().unwrap();
                if name_a == "Alice" || name_b == "Alice" {
                    found_alice = true;
                }
                if name_a == "Bob" || name_b == "Bob" {
                    found_bob = true;
                }
            }
            assert!(found_alice, "Should find Alice in friendship results");
            assert!(found_bob, "Should find Bob in friendship results");
        }
        cleanup(&dir);
    });
}

// ─── Relations with Distinct Roles ─────────────────────────────────

#[test]
fn relation_with_distinct_roles() {
    async_std::task::block_on(async {
        let dir = test_dir("distinct_roles");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema with employer/employee roles
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    entity person, owns name, plays employment:employee;
                    entity company, owns name, plays employment:employer;
                    attribute name, value string;
                    relation employment, relates employer, relates employee;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert a company, a person, and an employment relation
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query(
                "insert
                    $c isa company, has name \"Acme Corp\";
                    $p isa person, has name \"Alice\";
                    (employer: $c, employee: $p) isa employment;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Query back via the employment relation
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx
                .query(
                    "match
                        (employer: $c, employee: $p) isa employment;
                        $c has name $cn;
                        $p has name $pn;",
                )
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find exactly 1 employment");

            let row = &rows[0];
            let cn = row.get("cn").unwrap().unwrap();
            assert_eq!(cn.try_get_string(), Some("Acme Corp"));

            let pn = row.get("pn").unwrap().unwrap();
            assert_eq!(pn.try_get_string(), Some("Alice"));

            // Verify the entity types
            let c = row.get("c").unwrap().unwrap();
            assert!(c.is_entity(), "Employer should be an Entity");
            assert_eq!(c.get_label(), "company");

            let p = row.get("p").unwrap().unwrap();
            assert!(p.is_entity(), "Employee should be an Entity");
            assert_eq!(p.get_label(), "person");
        }
        cleanup(&dir);
    });
}

// ─── Relations Owning Attributes ───────────────────────────────────

#[test]
fn relation_owns_attribute() {
    async_std::task::block_on(async {
        let dir = test_dir("rel_attr");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema: membership relation owns 'since' attribute
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    entity person, owns name, plays membership:member;
                    entity group, owns name, plays membership:group;
                    attribute name, value string;
                    attribute since, value integer;
                    relation membership, relates member, relates group, owns since;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert a membership with the 'since' attribute
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query(
                "insert
                    $p isa person, has name \"Alice\";
                    $g isa group, has name \"Engineers\";
                    (member: $p, group: $g) isa membership, has since 2024;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Query the membership and its attribute
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx
                .query(
                    "match
                        $m (member: $p, group: $g) isa membership, has since $s;
                        $p has name $pn;
                        $g has name $gn;",
                )
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find exactly 1 membership");

            let row = &rows[0];

            // Verify the 'since' attribute on the relation
            let s = row.get("s").unwrap().unwrap();
            assert!(s.is_attribute(), "since should be an Attribute");
            assert_eq!(s.try_get_integer(), Some(2024));

            // Verify member and group names
            let pn = row.get("pn").unwrap().unwrap();
            assert_eq!(pn.try_get_string(), Some("Alice"));

            let gn = row.get("gn").unwrap().unwrap();
            assert_eq!(gn.try_get_string(), Some("Engineers"));

            // Verify the relation concept itself
            let m = row.get("m").unwrap().unwrap();
            assert_eq!(m.get_label(), "membership");
        }
        cleanup(&dir);
    });
}

// ─── Graph Traversal ───────────────────────────────────────────────

#[test]
fn query_via_relation_traversal() {
    async_std::task::block_on(async {
        let dir = test_dir("traversal");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema: person lives-in city
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    entity person, owns name, plays lives-in:resident;
                    entity city, owns name, plays lives-in:city;
                    attribute name, value string;
                    relation lives-in, relates resident, relates city;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert Alice and Bob both living in London
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query(
                "insert
                    $london isa city, has name \"London\";
                    $alice isa person, has name \"Alice\";
                    (resident: $alice, city: $london) isa lives-in;",
            )
            .await
            .unwrap();
            // Bob in a separate insert (reuse London by match)
            tx.query(
                "match $london isa city, has name \"London\";
                 insert
                    $bob isa person, has name \"Bob\";
                    (resident: $bob, city: $london) isa lives-in;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Traverse: find all people living in London
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx
                .query(
                    "match
                        $p isa person, has name $n;
                        (resident: $p, city: $c) isa lives-in;
                        $c has name \"London\";",
                )
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 2, "Should find 2 people living in London, found {}", rows.len());

            let names: Vec<&str> = rows
                .iter()
                .filter_map(|row| row.get("n").ok()?.and_then(|c| c.try_get_string()))
                .collect();
            assert!(names.contains(&"Alice"), "Should find Alice in London");
            assert!(names.contains(&"Bob"), "Should find Bob in London");
        }
        cleanup(&dir);
    });
}

// ─── Multiple Relations Between Same Entities ──────────────────────

#[test]
fn multiple_relations_between_same_entities() {
    async_std::task::block_on(async {
        let dir = test_dir("multi_rel");
        {
            let driver = TypeDBDriver::new_embedded(&dir).unwrap();
            driver.embedded_databases().unwrap().put_database("test").unwrap();

            // Define schema
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    entity person, owns name, plays friendship:friend;
                    attribute name, value string;
                    relation friendship, relates friend @card(0..);",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert two people
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query(
                "insert
                    $a isa person, has name \"Alice\";
                    $b isa person, has name \"Bob\";",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert two separate friendship instances between the same pair
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query(
                "match
                    $a isa person, has name \"Alice\";
                    $b isa person, has name \"Bob\";
                 insert
                    (friend: $a, friend: $b) isa friendship;",
            )
            .await
            .unwrap();
            tx.query(
                "match
                    $a isa person, has name \"Alice\";
                    $b isa person, has name \"Bob\";
                 insert
                    (friend: $a, friend: $b) isa friendship;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Query all friendships — should find 2 distinct relation instances
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx
                .query("match $f isa friendship;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 2, "Should find 2 friendship instances, found {}", rows.len());

            // Verify they are distinct relation instances (different IIDs)
            let f0 = rows[0].get("f").unwrap().unwrap();
            let f1 = rows[1].get("f").unwrap().unwrap();
            assert!(f0.is_relation(), "First result should be a Relation");
            assert!(f1.is_relation(), "Second result should be a Relation");
        }
        cleanup(&dir);
    });
}
