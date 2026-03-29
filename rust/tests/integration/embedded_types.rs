/*
 * Integration tests for the type system in the embedded TypeDB backend.
 *
 * These tests verify type inheritance (entity/attribute sub), abstract types,
 * polymorphic queries, type-level queries (match entity/attribute/relation),
 * and entities owning multiple attributes.
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
        "thyra_test_types_{}_{}",
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

/// Create a driver with a database named "test" already set up.
fn driver_with_db(dir: &PathBuf) -> TypeDBDriver {
    let driver = TypeDBDriver::new_embedded(dir).unwrap();
    driver.embedded_databases().unwrap().put_database("test").unwrap();
    driver
}

// -- 1. Entity type inheritance (sub) with polymorphic query ----------------

#[test]
fn type_inheritance_entity_sub() {
    async_std::task::block_on(async {
        let dir = test_dir("entity_sub");
        {
            let driver = driver_with_db(&dir);

            // Define person and employee sub person
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity person, owns name; attribute name, value string; entity employee sub person;")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Insert an employee
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $e isa employee, has name \"Alice\";")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Polymorphic query: match person should return the employee
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $p isa person, has name $n;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Polymorphic query on person should return the employee");

            let row = &rows[0];
            let p = row.get("p").unwrap().unwrap();
            assert!(p.is_entity(), "Result should be an Entity");
            assert_eq!(p.get_label(), "employee", "Entity label should be employee");

            let n = row.get("n").unwrap().unwrap();
            assert_eq!(n.try_get_string(), Some("Alice"));
        }
        cleanup(&dir);
    });
}

// -- 2. Attribute type inheritance (sub) with polymorphic query -------------

#[test]
fn type_inheritance_attribute_sub() {
    async_std::task::block_on(async {
        let dir = test_dir("attr_sub");
        {
            let driver = driver_with_db(&dir);

            // Define abstract attribute id, with email and phone as subtypes
            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    attribute id @abstract, value string;
                    attribute email sub id;
                    attribute phone sub id;
                    entity contact, owns email, owns phone;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert a contact with an email
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query("insert $c isa contact, has email \"a@b.com\";")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            // Polymorphic query: match via parent attribute type id
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match $c isa contact, has id $i;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Query via parent attribute type should return the email");

            let row = &rows[0];
            let i = row.get("i").unwrap().unwrap();
            assert!(i.is_attribute(), "Result should be an Attribute");
            assert_eq!(i.get_label(), "email", "Attribute label should be email");
            assert_eq!(i.try_get_string(), Some("a@b.com"));
        }
        cleanup(&dir);
    });
}

// -- 3. Query relation types ------------------------------------------------

#[test]
fn query_relation_types() {
    async_std::task::block_on(async {
        let dir = test_dir("rel_types");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    entity person, plays friendship:friend, plays employment:employee;
                    entity company, plays employment:employer;
                    relation friendship, relates friend;
                    relation employment, relates employee, relates employer;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match relation $r;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();

            let labels: Vec<&str> = rows
                .iter()
                .filter_map(|row| row.get("r").ok()?.map(|c| c.get_label()))
                .collect();

            assert!(
                labels.contains(&"friendship"),
                "Should contain friendship relation type, found: {:?}",
                labels
            );
            assert!(
                labels.contains(&"employment"),
                "Should contain employment relation type, found: {:?}",
                labels
            );
        }
        cleanup(&dir);
    });
}

// -- 4. Query attribute types with different value types --------------------

#[test]
fn query_attribute_types() {
    async_std::task::block_on(async {
        let dir = test_dir("attr_types");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    attribute name, value string;
                    attribute age, value integer;
                    attribute score, value double;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();
            let answer = tx.query("match attribute $a;").await.unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();

            let labels: Vec<&str> = rows
                .iter()
                .filter_map(|row| row.get("a").ok()?.map(|c| c.get_label()))
                .collect();

            assert!(
                labels.contains(&"name"),
                "Should contain name attribute type, found: {:?}",
                labels
            );
            assert!(
                labels.contains(&"age"),
                "Should contain age attribute type, found: {:?}",
                labels
            );
            assert!(
                labels.contains(&"score"),
                "Should contain score attribute type, found: {:?}",
                labels
            );
        }
        cleanup(&dir);
    });
}

// -- 5. Abstract type cannot be instantiated --------------------------------

#[test]
fn abstract_type_cannot_be_instantiated() {
    async_std::task::block_on(async {
        let dir = test_dir("abstract");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query("define entity thing @abstract;").await.unwrap();
            tx.commit().await.unwrap();

            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            let result = tx.query("insert $x isa thing;").await;
            assert!(
                result.is_err(),
                "Inserting an instance of an abstract type should fail"
            );
        }
        cleanup(&dir);
    });
}

// -- 6. Entity with multiple owned attributes -------------------------------

#[test]
fn entity_with_multiple_owned_attributes() {
    async_std::task::block_on(async {
        let dir = test_dir("multi_attrs");
        {
            let driver = driver_with_db(&dir);

            let tx = driver.transaction("test", TransactionType::Schema).await.unwrap();
            tx.query(
                "define
                    attribute first-name, value string;
                    attribute last-name, value string;
                    attribute age, value integer;
                    attribute email, value string;
                    attribute active, value boolean;
                    entity user, owns first-name, owns last-name, owns age, owns email, owns active;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Insert a user with all 5 attributes
            let tx = driver.transaction("test", TransactionType::Write).await.unwrap();
            tx.query(
                "insert $u isa user,
                    has first-name \"Jane\",
                    has last-name \"Doe\",
                    has age 28,
                    has email \"jane@example.com\",
                    has active true;",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Query back each attribute individually to verify all are accessible
            let tx = driver.transaction("test", TransactionType::Read).await.unwrap();

            // first-name
            let answer = tx
                .query("match $u isa user, has first-name $fn;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find user by first-name");
            assert_eq!(rows[0].get("fn").unwrap().unwrap().try_get_string(), Some("Jane"));

            // last-name
            let answer = tx
                .query("match $u isa user, has last-name $ln;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find user by last-name");
            assert_eq!(rows[0].get("ln").unwrap().unwrap().try_get_string(), Some("Doe"));

            // age
            let answer = tx
                .query("match $u isa user, has age $a;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find user by age");
            assert_eq!(rows[0].get("a").unwrap().unwrap().try_get_integer(), Some(28));

            // email
            let answer = tx
                .query("match $u isa user, has email $e;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find user by email");
            assert_eq!(
                rows[0].get("e").unwrap().unwrap().try_get_string(),
                Some("jane@example.com")
            );

            // active (boolean)
            let answer = tx
                .query("match $u isa user, has active $ac;")
                .await
                .unwrap();
            let rows: Vec<ConceptRow> = answer.into_rows().try_collect().await.unwrap();
            assert_eq!(rows.len(), 1, "Should find user by active");
            assert_eq!(rows[0].get("ac").unwrap().unwrap().try_get_boolean(), Some(true));
        }
        cleanup(&dir);
    });
}
