/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::collections::HashMap;
use std::sync::Arc;

use engine_answer::variable_value::VariableValue;
use engine_concept::{
    thing::{thing_manager::ThingManager, ThingAPI},
    type_::{type_manager::TypeManager, TypeAPI},
};
use encoding::value::value::Value as EngineValue;
use ir::pipeline::ParameterRegistry;
use storage::snapshot::ReadableSnapshot;

use crate::{
    answer::concept_document::{self, ConceptDocument as DriverConceptDocument, Leaf, Node},
    concept::{
        self,
        instance::{Attribute, Entity, Relation},
        type_::{AttributeType, EntityType, RelationType, RoleType},
        value::{Decimal, Duration, TimeZone, Value},
        Concept, Kind as DriverKind, ValueType,
    },
    IID,
};

/// Convert an engine VariableValue to a driver Concept.
///
/// Requires access to the snapshot and type_manager to resolve type labels,
/// and thing_manager for attribute values.
pub(crate) fn convert_variable_value(
    value: &VariableValue<'_>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
) -> Option<Concept> {
    match value {
        VariableValue::None => None,
        VariableValue::Type(engine_type) => Some(convert_type(engine_type, snapshot, type_manager)),
        VariableValue::Thing(engine_thing) => Some(convert_thing(engine_thing, snapshot, type_manager, thing_manager)),
        VariableValue::Value(engine_value) => Some(Concept::Value(convert_value(engine_value))),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => {
            // Lists are not yet supported in driver concept conversion
            None
        }
    }
}

fn convert_type(
    engine_type: &engine_answer::Type,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Concept {
    match engine_type {
        engine_answer::Type::Entity(et) => {
            let label = et
                .get_label(snapshot, type_manager)
                .ok()
                .map(|l| l.scoped_name.as_str().to_owned())
                .unwrap_or_else(|| Concept::UNKNOWN_LABEL.to_owned());
            Concept::EntityType(EntityType { label })
        }
        engine_answer::Type::Relation(rt) => {
            let label = rt
                .get_label(snapshot, type_manager)
                .ok()
                .map(|l| l.scoped_name.as_str().to_owned())
                .unwrap_or_else(|| Concept::UNKNOWN_LABEL.to_owned());
            Concept::RelationType(RelationType { label })
        }
        engine_answer::Type::Attribute(at) => {
            let label = at
                .get_label(snapshot, type_manager)
                .ok()
                .map(|l| l.scoped_name.as_str().to_owned())
                .unwrap_or_else(|| Concept::UNKNOWN_LABEL.to_owned());
            let value_type = at
                .get_value_type_declared(snapshot, type_manager)
                .ok()
                .flatten()
                .map(|vt| convert_value_type(&vt));
            Concept::AttributeType(AttributeType { label, value_type })
        }
        engine_answer::Type::RoleType(rt) => {
            let label = rt
                .get_label(snapshot, type_manager)
                .ok()
                .map(|l| l.scoped_name.as_str().to_owned())
                .unwrap_or_else(|| Concept::UNKNOWN_LABEL.to_owned());
            Concept::RoleType(RoleType { label })
        }
    }
}

fn convert_thing(
    engine_thing: &engine_answer::Thing,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
) -> Concept {
    match engine_thing {
        engine_answer::Thing::Entity(entity) => {
            let iid = IID::from(entity.iid().to_vec());
            let type_label = entity
                .type_()
                .get_label(snapshot, type_manager)
                .ok()
                .map(|l| l.scoped_name.as_str().to_owned());
            Concept::Entity(Entity {
                iid,
                type_: type_label.map(|label| EntityType { label }),
            })
        }
        engine_answer::Thing::Relation(relation) => {
            let iid = IID::from(relation.iid().to_vec());
            let type_label = relation
                .type_()
                .get_label(snapshot, type_manager)
                .ok()
                .map(|l| l.scoped_name.as_str().to_owned());
            Concept::Relation(Relation {
                iid,
                type_: type_label.map(|label| RelationType { label }),
            })
        }
        engine_answer::Thing::Attribute(attribute) => {
            let iid = IID::from(attribute.iid().to_vec());
            let type_label = attribute
                .type_()
                .get_label(snapshot, type_manager)
                .ok()
                .map(|l| l.scoped_name.as_str().to_owned());
            let value_type = attribute
                .type_()
                .get_value_type_declared(snapshot, type_manager)
                .ok()
                .flatten()
                .map(|vt| convert_value_type(&vt));
            let value = attribute
                .get_value(snapshot, thing_manager, engine_resource::profile::StorageCounters::DISABLED)
                .ok()
                .map(|v| convert_value(&v))
                .unwrap_or(Value::String(String::from("<error reading value>")));
            Concept::Attribute(Attribute {
                iid,
                type_: type_label.map(|label| AttributeType { label, value_type }),
                value,
            })
        }
    }
}

fn convert_value_type(vt: &encoding::value::value_type::ValueType) -> ValueType {
    match vt {
        encoding::value::value_type::ValueType::Boolean => ValueType::Boolean,
        encoding::value::value_type::ValueType::Integer => ValueType::Integer,
        encoding::value::value_type::ValueType::Double => ValueType::Double,
        encoding::value::value_type::ValueType::Decimal => ValueType::Decimal,
        encoding::value::value_type::ValueType::String => ValueType::String,
        encoding::value::value_type::ValueType::Date => ValueType::Date,
        encoding::value::value_type::ValueType::DateTime => ValueType::Datetime,
        encoding::value::value_type::ValueType::DateTimeTZ => ValueType::DatetimeTZ,
        encoding::value::value_type::ValueType::Duration => ValueType::Duration,
        encoding::value::value_type::ValueType::Struct(_) => ValueType::Struct(String::from("struct")),
    }
}

pub(crate) fn convert_value(engine_value: &EngineValue<'_>) -> Value {
    match engine_value {
        EngineValue::Boolean(b) => Value::Boolean(*b),
        EngineValue::Integer(i) => Value::Integer(*i),
        EngineValue::Double(d) => Value::Double(*d),
        EngineValue::Decimal(d) => Value::Decimal(Decimal::new(d.integer_part(), d.fractional_part())),
        EngineValue::String(s) => Value::String(s.to_string()),
        EngineValue::Date(d) => Value::Date(*d),
        EngineValue::DateTime(dt) => Value::Datetime(*dt),
        EngineValue::DateTimeTZ(dt) => {
            // Convert engine TimeZone to driver TimeZone
            let driver_tz = convert_timezone(&dt.timezone());
            let naive = dt.naive_utc();
            use chrono::TimeZone as _;
            let driver_dt = driver_tz.from_utc_datetime(&naive);
            Value::DatetimeTZ(driver_dt)
        }
        EngineValue::Duration(d) => Value::Duration(Duration::new(d.months, d.days, d.nanos)),
        EngineValue::Struct(_) => {
            // Struct conversion is complex; return a placeholder for now
            Value::String(String::from("<struct>"))
        }
    }
}

fn convert_timezone(engine_tz: &encoding::value::timezone::TimeZone) -> TimeZone {
    match engine_tz {
        encoding::value::timezone::TimeZone::IANA(tz) => TimeZone::IANA(*tz),
        encoding::value::timezone::TimeZone::Fixed(fixed) => TimeZone::Fixed(*fixed),
    }
}

// ─── Document Conversion ────────────────────────────────────────────

/// Convert an engine ConceptDocument to a driver ConceptDocument.
pub(crate) fn convert_document(
    engine_doc: executor::document::ConceptDocument,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> DriverConceptDocument {
    let root = convert_document_node(engine_doc.root, snapshot, type_manager, thing_manager, parameters);
    // We create a header per-document but the caller will wrap with a shared header
    DriverConceptDocument::new(
        Arc::new(concept_document::ConceptDocumentHeader {
            query_type: crate::answer::QueryType::ReadQuery,
        }),
        Some(root),
    )
}

/// Convert an engine DocumentNode to a driver Node.
fn convert_document_node(
    node: executor::document::DocumentNode,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> Node {
    match node {
        executor::document::DocumentNode::List(list) => {
            let items: Vec<Node> = list
                .list
                .into_iter()
                .map(|n| convert_document_node(n, snapshot, type_manager, thing_manager, parameters))
                .collect();
            Node::List(items)
        }
        executor::document::DocumentNode::Map(map) => {
            let entries: HashMap<String, Node> = match map {
                executor::document::DocumentMap::UserKeys(map) => {
                    map.into_iter()
                        .map(|(key, value)| {
                            let key_name = parameters
                                .fetch_key(&key)
                                .cloned()
                                .unwrap_or_else(|| format!("{:?}", key));
                            let node = convert_document_node(value, snapshot, type_manager, thing_manager, parameters);
                            (key_name, node)
                        })
                        .collect()
                }
                executor::document::DocumentMap::GeneratedKeys(map) => {
                    map.into_iter()
                        .map(|(label, value)| {
                            let key_name = label.scoped_name.as_str().to_owned();
                            let node = convert_document_node(value, snapshot, type_manager, thing_manager, parameters);
                            (key_name, node)
                        })
                        .collect()
                }
            };
            Node::Map(entries)
        }
        executor::document::DocumentNode::Leaf(leaf) => {
            Node::Leaf(convert_document_leaf(leaf, snapshot, type_manager, thing_manager))
        }
    }
}

/// Convert an engine DocumentLeaf to a driver Leaf option.
fn convert_document_leaf(
    leaf: executor::document::DocumentLeaf,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
) -> Option<Leaf> {
    match leaf {
        executor::document::DocumentLeaf::Empty => Some(Leaf::Empty),
        executor::document::DocumentLeaf::Concept(concept) => {
            // engine answer::Concept has Type, Thing, Value variants
            match concept {
                engine_answer::Concept::Type(t) => {
                    let driver_concept = convert_type_from_answer_type(&t, snapshot, type_manager);
                    Some(Leaf::Concept(driver_concept))
                }
                engine_answer::Concept::Thing(t) => {
                    let driver_concept = convert_thing(&t, snapshot, type_manager, thing_manager);
                    Some(Leaf::Concept(driver_concept))
                }
                engine_answer::Concept::Value(v) => {
                    Some(Leaf::Concept(Concept::Value(convert_value(&v))))
                }
            }
        }
        executor::document::DocumentLeaf::Kind(kind) => {
            let driver_kind = match kind {
                encoding::graph::type_::Kind::Entity => DriverKind::Entity,
                encoding::graph::type_::Kind::Relation => DriverKind::Relation,
                encoding::graph::type_::Kind::Attribute => DriverKind::Attribute,
                encoding::graph::type_::Kind::Role => DriverKind::Role,
            };
            Some(Leaf::Kind(driver_kind))
        }
    }
}

/// Convert an engine answer::Type to a driver Concept (reusing existing convert_type logic).
fn convert_type_from_answer_type(
    engine_type: &engine_answer::Type,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Concept {
    convert_type(engine_type, snapshot, type_manager)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::borrow::Cow;

    use chrono::{FixedOffset, NaiveDate, NaiveDateTime, TimeZone as ChronoTimeZone};
    use chrono_tz::Tz;
    use encoding::{
        graph::{
            definition::definition_key::{DefinitionID, DefinitionKey},
            type_::Kind,
        },
        layout::prefix::Prefix,
        value::{
            decimal_value::Decimal as EngineDecimal,
            duration_value::Duration as EngineDuration,
            timezone::TimeZone as EngineTimeZone,
            value::Value as EngineValue,
            value_type::ValueType as EngineValueType,
        },
    };

    // ─── convert_value_type tests ──────────────────────────────────────

    #[test]
    fn test_convert_value_type_boolean() {
        assert!(matches!(convert_value_type(&EngineValueType::Boolean), ValueType::Boolean));
    }

    #[test]
    fn test_convert_value_type_integer() {
        assert!(matches!(convert_value_type(&EngineValueType::Integer), ValueType::Integer));
    }

    #[test]
    fn test_convert_value_type_double() {
        assert!(matches!(convert_value_type(&EngineValueType::Double), ValueType::Double));
    }

    #[test]
    fn test_convert_value_type_decimal() {
        assert!(matches!(convert_value_type(&EngineValueType::Decimal), ValueType::Decimal));
    }

    #[test]
    fn test_convert_value_type_string() {
        assert!(matches!(convert_value_type(&EngineValueType::String), ValueType::String));
    }

    #[test]
    fn test_convert_value_type_date() {
        assert!(matches!(convert_value_type(&EngineValueType::Date), ValueType::Date));
    }

    #[test]
    fn test_convert_value_type_datetime() {
        assert!(matches!(convert_value_type(&EngineValueType::DateTime), ValueType::Datetime));
    }

    #[test]
    fn test_convert_value_type_datetime_tz() {
        assert!(matches!(convert_value_type(&EngineValueType::DateTimeTZ), ValueType::DatetimeTZ));
    }

    #[test]
    fn test_convert_value_type_duration() {
        assert!(matches!(convert_value_type(&EngineValueType::Duration), ValueType::Duration));
    }

    #[test]
    fn test_convert_value_type_struct() {
        let key = DefinitionKey::build(Prefix::DefinitionStruct, DefinitionID::build(1));
        let result = convert_value_type(&EngineValueType::Struct(key));
        match result {
            ValueType::Struct(name) => assert_eq!(name, "struct"),
            other => panic!("expected ValueType::Struct, got {:?}", other),
        }
    }

    // ─── convert_value tests ───────────────────────────────────────────

    #[test]
    fn test_convert_value_boolean() {
        assert_eq!(convert_value(&EngineValue::Boolean(true)), Value::Boolean(true));
        assert_eq!(convert_value(&EngineValue::Boolean(false)), Value::Boolean(false));
    }

    #[test]
    fn test_convert_value_integer() {
        assert_eq!(convert_value(&EngineValue::Integer(0)), Value::Integer(0));
        assert_eq!(convert_value(&EngineValue::Integer(42)), Value::Integer(42));
        assert_eq!(convert_value(&EngineValue::Integer(-1)), Value::Integer(-1));
        assert_eq!(convert_value(&EngineValue::Integer(i64::MAX)), Value::Integer(i64::MAX));
        assert_eq!(convert_value(&EngineValue::Integer(i64::MIN)), Value::Integer(i64::MIN));
    }

    #[test]
    fn test_convert_value_double() {
        assert_eq!(convert_value(&EngineValue::Double(3.14)), Value::Double(3.14));
        assert_eq!(convert_value(&EngineValue::Double(0.0)), Value::Double(0.0));
        assert_eq!(convert_value(&EngineValue::Double(-1.5)), Value::Double(-1.5));
    }

    #[test]
    fn test_convert_value_decimal() {
        let engine_dec: EngineDecimal = "123.45".parse().unwrap();
        let result = convert_value(&EngineValue::Decimal(engine_dec));
        match result {
            Value::Decimal(d) => {
                assert_eq!(d.integer_part(), 123);
                assert_eq!(d.fractional_part(), engine_dec.fractional_part());
            }
            other => panic!("expected Value::Decimal, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_value_string() {
        let result = convert_value(&EngineValue::String(Cow::Borrowed("hello")));
        assert_eq!(result, Value::String(String::from("hello")));

        let result = convert_value(&EngineValue::String(Cow::Owned(String::from("world"))));
        assert_eq!(result, Value::String(String::from("world")));

        let result = convert_value(&EngineValue::String(Cow::Borrowed("")));
        assert_eq!(result, Value::String(String::new()));
    }

    #[test]
    fn test_convert_value_date() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let result = convert_value(&EngineValue::Date(date));
        assert_eq!(result, Value::Date(date));
    }

    #[test]
    fn test_convert_value_datetime() {
        let dt = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap();
        let result = convert_value(&EngineValue::DateTime(dt));
        assert_eq!(result, Value::Datetime(dt));
    }

    #[test]
    fn test_convert_value_datetime_tz_iana() {
        let tz = EngineTimeZone::IANA(Tz::UTC);
        let naive = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let engine_dt = tz.from_utc_datetime(&naive);
        let result = convert_value(&EngineValue::DateTimeTZ(engine_dt));
        match result {
            Value::DatetimeTZ(dt) => {
                assert_eq!(dt.naive_utc(), naive);
            }
            other => panic!("expected Value::DatetimeTZ, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_value_datetime_tz_fixed() {
        let offset = FixedOffset::east_opt(5 * 3600).unwrap();
        let tz = EngineTimeZone::Fixed(offset);
        let naive = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let engine_dt = tz.from_utc_datetime(&naive);
        let result = convert_value(&EngineValue::DateTimeTZ(engine_dt));
        match result {
            Value::DatetimeTZ(dt) => {
                assert_eq!(dt.naive_utc(), naive);
            }
            other => panic!("expected Value::DatetimeTZ, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_value_duration() {
        let engine_dur = EngineDuration::months(6);
        let result = convert_value(&EngineValue::Duration(engine_dur));
        match result {
            Value::Duration(d) => {
                assert_eq!(d.months(), 6);
                assert_eq!(d.days(), 0);
                assert_eq!(d.nanos(), 0);
            }
            other => panic!("expected Value::Duration, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_value_duration_complex() {
        let engine_dur = EngineDuration::days(10)
            .checked_add(EngineDuration::hours(5))
            .unwrap();
        let result = convert_value(&EngineValue::Duration(engine_dur));
        match result {
            Value::Duration(d) => {
                assert_eq!(d.months(), 0);
                assert_eq!(d.days(), 10);
                assert_eq!(d.nanos(), 5 * 3_600_000_000_000u64);
            }
            other => panic!("expected Value::Duration, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_value_struct_placeholder() {
        // Struct conversion returns a placeholder string
        let key = DefinitionKey::build(Prefix::DefinitionStruct, DefinitionID::build(1));
        let struct_val = encoding::value::value_struct::StructValue::new(key, Default::default());
        let result = convert_value(&EngineValue::Struct(Cow::Owned(struct_val)));
        assert_eq!(result, Value::String(String::from("<struct>")));
    }

    // ─── convert_timezone tests ────────────────────────────────────────

    #[test]
    fn test_convert_timezone_iana() {
        let tz = convert_timezone(&EngineTimeZone::IANA(Tz::US__Eastern));
        match tz {
            TimeZone::IANA(iana) => assert_eq!(iana, Tz::US__Eastern),
            other => panic!("expected TimeZone::IANA, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_timezone_fixed() {
        let offset = FixedOffset::west_opt(8 * 3600).unwrap();
        let tz = convert_timezone(&EngineTimeZone::Fixed(offset));
        match tz {
            TimeZone::Fixed(fixed) => assert_eq!(fixed, offset),
            other => panic!("expected TimeZone::Fixed, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_timezone_utc() {
        let tz = convert_timezone(&EngineTimeZone::IANA(Tz::UTC));
        match tz {
            TimeZone::IANA(iana) => assert_eq!(iana, Tz::UTC),
            other => panic!("expected TimeZone::IANA(UTC), got {:?}", other),
        }
    }
}
