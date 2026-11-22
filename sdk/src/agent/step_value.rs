use crate::error::LaserError;
use serde::de::DeserializeOwned;
use serde::ser::{self, Serialize, Serializer};
use std::fmt;

/// The JSON a step's value is stored as. serde_json writes a non-finite float
/// as `null`, which would change the value, so such a float is refused.
pub(super) fn step_output<T: Serialize>(label: &str, value: &T) -> Result<Vec<u8>, LaserError> {
    json_output(value).map_err(|error| {
        LaserError::Codec(format!(
            "workflow step `{label}` returned a value JSON cannot hold: {error}"
        ))
    })
}

/// The JSON of the value a workflow body returned.
pub(super) fn workflow_value<T: Serialize>(name: &str, value: &T) -> Result<Vec<u8>, LaserError> {
    json_output(value).map_err(|error| {
        LaserError::Codec(format!(
            "workflow `{name}` returned a value JSON cannot hold: {error}"
        ))
    })
}

/// A step's value read back from its stored JSON.
pub(super) fn decode_step_output<T: DeserializeOwned>(
    label: &str,
    output: &[u8],
) -> Result<T, LaserError> {
    serde_json::from_slice(output).map_err(|error| {
        LaserError::Codec(format!(
            "the output of workflow step `{label}` does not decode as the JSON asked for: {error}"
        ))
    })
}

fn json_output<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    value
        .serialize(FiniteFloats)
        .map_err(|error| error.to_string())?;
    serde_json::to_vec(value).map_err(|error| error.to_string())
}

#[derive(Debug)]
struct NotJson(String);

impl fmt::Display for NotJson {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for NotJson {}

impl ser::Error for NotJson {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

// Walks a value and fails on the first non-finite float.
#[derive(Clone, Copy)]
struct FiniteFloats;

macro_rules! accept_scalars {
    ($($method:ident($scalar:ty)),* $(,)?) => {
        $(fn $method(self, _value: $scalar) -> Result<(), NotJson> {
            Ok(())
        })*
    };
}

macro_rules! walk_compound {
    ($($compound:ident { $($element:ident($($key:ident: $key_type:ty),*)),* }),* $(,)?) => {
        $(impl ser::$compound for FiniteFloats {
            type Ok = ();
            type Error = NotJson;

            $(fn $element<T: ?Sized + Serialize>(
                &mut self,
                $($key: $key_type,)*
                value: &T,
            ) -> Result<(), NotJson> {
                $(let _ = $key;)*
                value.serialize(FiniteFloats)
            })*

            fn end(self) -> Result<(), NotJson> {
                Ok(())
            }
        })*
    };
}

walk_compound! {
    SerializeSeq { serialize_element() },
    SerializeTuple { serialize_element() },
    SerializeTupleStruct { serialize_field() },
    SerializeTupleVariant { serialize_field() },
    SerializeStruct { serialize_field(key: &'static str) },
    SerializeStructVariant { serialize_field(key: &'static str) },
}

impl ser::SerializeMap for FiniteFloats {
    type Ok = ();
    type Error = NotJson;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), NotJson> {
        key.serialize(FiniteFloats)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), NotJson> {
        value.serialize(FiniteFloats)
    }

    fn end(self) -> Result<(), NotJson> {
        Ok(())
    }
}

impl Serializer for FiniteFloats {
    type Ok = ();
    type Error = NotJson;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    accept_scalars! {
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8]),
    }

    fn serialize_f32(self, value: f32) -> Result<(), NotJson> {
        self.serialize_f64(f64::from(value))
    }

    fn serialize_f64(self, value: f64) -> Result<(), NotJson> {
        if value.is_finite() {
            Ok(())
        } else {
            Err(NotJson(format!("the number {value} is not finite")))
        }
    }

    fn serialize_none(self) -> Result<(), NotJson> {
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), NotJson> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), NotJson> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), NotJson> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<(), NotJson> {
        Ok(())
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), NotJson> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<(), NotJson> {
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self, NotJson> {
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self, NotJson> {
        Ok(self)
    }

    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Self, NotJson> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, NotJson> {
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self, NotJson> {
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self, NotJson> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, NotJson> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn given_a_non_finite_float_at_any_depth_when_encoded_then_should_refuse_it() {
        let nested = BTreeMap::from([("ratio", vec![Some(1.5), Some(f64::NAN)])]);

        assert!(matches!(
            step_output("score", &Some(f64::INFINITY)),
            Err(LaserError::Codec(message)) if message.contains("`score`")
        ));
        assert!(step_output("score", &nested).is_err());
    }

    #[test]
    fn given_finite_values_when_encoded_then_should_write_their_json() {
        let value = BTreeMap::from([("ratio", vec![Some(1.5), None])]);

        assert_eq!(
            step_output("score", &value).expect("finite values encode"),
            br#"{"ratio":[1.5,null]}"#
        );
    }

    #[test]
    fn given_a_workflow_value_json_cannot_hold_when_encoded_then_should_name_the_workflow() {
        assert!(matches!(
            workflow_value("pipeline", &f64::NAN),
            Err(LaserError::Codec(message)) if message.starts_with("workflow `pipeline`")
        ));
    }
}
