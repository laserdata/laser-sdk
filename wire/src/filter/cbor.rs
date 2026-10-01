use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Number, Value};
use std::fmt;

/// Build the evaluator value directly, without an intermediate CBOR value tree.
pub(super) struct CborValue(pub(super) Value);

impl<'de> Deserialize<'de> for CborValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(ValueVisitor).map(Self)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a filter CBOR value")
    }

    fn visit_bool<E: Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E: Error>(self, value: i64) -> Result<Value, E> {
        Ok(Value::from(value))
    }

    fn visit_u64<E: Error>(self, value: u64) -> Result<Value, E> {
        Ok(Value::from(value))
    }

    fn visit_i128<E: Error>(self, value: i128) -> Result<Value, E> {
        i64::try_from(value)
            .map(Value::from)
            .map_err(|_| E::custom("integer outside i64"))
    }

    fn visit_u128<E: Error>(self, value: u128) -> Result<Value, E> {
        u64::try_from(value)
            .map(Value::from)
            .map_err(|_| E::custom("integer outside u64"))
    }

    fn visit_f64<E: Error>(self, value: f64) -> Result<Value, E> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite CBOR number"))
    }

    fn visit_str<E: Error>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E: Error>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Value, E> {
        Ok(Value::Array(
            value.iter().copied().map(Value::from).collect(),
        ))
    }

    fn visit_unit<E: Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E: Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(CborValue(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut values = Map::new();
        while let Some((key, CborValue(value))) = map.next_entry::<String, CborValue>()? {
            if values.insert(key, value).is_some() {
                return Err(A::Error::custom("duplicate CBOR map key"));
            }
        }
        Ok(Value::Object(values))
    }
}
