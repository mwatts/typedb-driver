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

use engine_answer::variable_value::VariableValue;
use engine_concept::{
    thing::{thing_manager::ThingManager, ThingAPI},
    type_::{type_manager::TypeManager, TypeAPI},
};
use encoding::value::value::Value as EngineValue;
use storage::snapshot::ReadableSnapshot;

use crate::{
    concept::{
        self,
        instance::{Attribute, Entity, Relation},
        type_::{AttributeType, EntityType, RelationType, RoleType},
        value::{Decimal, Duration, TimeZone, Value},
        Concept, ValueType,
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
