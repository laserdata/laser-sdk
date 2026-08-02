use crate::cursor::Cursor;
use crate::error::LaserError;
use crate::message::Message;
use crate::stream::{Cbor, Codec, ContentType, Decoder, Json, PublishRequest, Topic};
use crate::types::MessageId;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;

impl Topic {
    /// The typed handle over this topic in the JSON serde form: no registry,
    /// the body type's `Serialize`/`Deserialize` is the contract. Publishes
    /// stamp `agdx.ct=json`, [`records`](TypedTopic::records) decodes each
    /// payload back into `T`.
    pub fn json<T>(&self) -> TypedTopic<T> {
        TypedTopic::new(self.clone(), Form::Json)
    }

    /// The typed handle in the CBOR serde form: same contract as
    /// [`json`](Self::json) with a binary self-describing body and
    /// `agdx.ct=cbor`.
    pub fn cbor<T>(&self) -> TypedTopic<T> {
        TypedTopic::new(self.clone(), Form::Cbor)
    }

    /// The typed handle bound to a registered writer schema: resolves the
    /// definition from LaserData Cloud's registry and compiles it ONCE for the
    /// handle's lifetime, so every publish validates against it client-side
    /// before any byte leaves the process and stamps `agdx.ct` + `agdx.sid`.
    /// A body that stops matching the schema fails at the producer with the
    /// schema's error, not downstream in the projector. Feature
    /// `schema-codecs`, and the registry is managed, so this form needs
    /// LaserData Cloud.
    #[cfg(feature = "schema-codecs")]
    pub async fn schema<T>(&self, schema_id: u32) -> Result<TypedTopic<T>, LaserError> {
        let info = self
            .laser()
            .schemas()
            .get(schema_id)
            .await?
            .ok_or_else(|| LaserError::Invalid(format!("schema {schema_id} is not registered")))?;
        let compiled = crate::schema_codecs::CompiledSchema::compile(&info.schema)?;
        Ok(TypedTopic::new(
            self.clone(),
            Form::Schema {
                id: schema_id,
                compiled,
            },
        ))
    }
}

/// One topic seen through one body type: encode, validate, and stamp on the
/// way in, decode with the log position attached on the way out. Build it with
/// [`Topic::json`], [`Topic::cbor`], or [`Topic::schema`]. The handle is a
/// plain wrapper over [`Topic`], so it is free to construct and clone, and the
/// raw verbs on the untyped handle stay one accessor away.
#[derive(Clone)]
pub struct TypedTopic<T> {
    topic: Topic,
    form: Form,
    body: PhantomData<fn() -> T>,
}

impl<T> TypedTopic<T> {
    /// Encode `body` under the handle's form and open the publish builder with
    /// the payload, `agdx.ct`, and (for the schema-bound form) `agdx.sid`
    /// already stamped. Chain `.index(..)`, `.partition_key(..)`, or any other
    /// record option, then `.send()`. Encoding and schema validation happen
    /// HERE, so a body the schema rejects never reaches the wire.
    pub fn publish(&self, body: &T) -> Result<PublishRequest<'_>, LaserError>
    where
        T: Serialize,
    {
        let (payload, content_type, schema_id) = self.form.encode(body)?;
        let mut request = self.topic.publish().raw_bytes(payload, content_type);
        if let Some(id) = schema_id {
            request = request.schema_id(id);
        }
        Ok(request)
    }

    /// The typed reader over this topic: a resumable [`Cursor`] under the
    /// caller-chosen identity `reader_name`, decoding each record into `T` as
    /// it is drained. This is not Apache Iggy consumer-group delivery. Own the
    /// offsets exactly like the raw cursor: persist
    /// [`offsets`](TypedRecords::offsets) and resume with
    /// [`from_offsets`](TypedRecords::from_offsets), so the read is bounded by
    /// construction and never a silent rescan from zero.
    pub fn records(&self, reader_name: &str) -> Result<TypedRecords<T>, LaserError>
    where
        T: DeserializeOwned,
    {
        let cursor = self.topic.replay()?.consumer_named(reader_name)?;
        Ok(TypedRecords {
            cursor,
            buffered: VecDeque::new(),
            form: self.form.clone(),
            body: PhantomData,
        })
    }

    /// The untyped handle underneath, for the verbs the typed form does not
    /// wrap (raw send, batches, the substrate builders).
    pub fn topic(&self) -> &Topic {
        &self.topic
    }

    fn new(topic: Topic, form: Form) -> Self {
        Self {
            topic,
            form,
            body: PhantomData,
        }
    }
}

/// The typed consume side of a [`TypedTopic`]: each [`next`](Self::next)
/// yields the next record decoded as `T`, or the position-carrying
/// [`TypedDecodeError`] for a record that does not decode (the reader moves
/// past it, so one bad record never wedges the stream). `None` means caught
/// up: everything appended so far has been yielded, call again later to see
/// new records.
///
/// The reliable consumer path composes instead of duplicating this: an
/// [`Agent`](crate::agent::Agent) handler decodes inside the handler with the
/// same error shape, and an undecodable record routes to the existing
/// dead-letter policy.
pub struct TypedRecords<T> {
    cursor: Cursor,
    buffered: VecDeque<Message>,
    form: Form,
    body: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> TypedRecords<T> {
    /// Resume from previously persisted per-partition offsets, exactly what an
    /// earlier [`offsets`](Self::offsets) returned.
    #[must_use]
    pub fn from_offsets(mut self, offsets: Vec<u64>) -> Self {
        self.cursor = self.cursor.from_offsets(offsets);
        self
    }

    /// Read at most `batch` messages per partition per underlying poll.
    #[must_use]
    pub fn batch(mut self, batch: u32) -> Self {
        self.cursor = self.cursor.batch(batch);
        self
    }

    /// The next offset to read on each partition. Persist this to resume later
    /// with [`from_offsets`](Self::from_offsets).
    pub fn offsets(&self) -> &[u64] {
        self.cursor.offsets()
    }

    /// The next record, decoded. `None` when the reader is caught up. A record
    /// that does not decode yields the error with its log position (taken from
    /// the record's own header, never a batch watermark) and the reader keeps
    /// going. A failed poll surfaces the same way with no position, and the
    /// next call polls again.
    pub async fn next(&mut self) -> Option<Result<TypedRecord<T>, TypedDecodeError>> {
        if self.buffered.is_empty() {
            match self.cursor.poll().await {
                Ok(messages) => self.buffered.extend(messages),
                Err(error) => {
                    return Some(Err(TypedDecodeError {
                        position: None,
                        source: error,
                    }));
                }
            }
        }
        let message = self.buffered.pop_front()?;
        Some(decode_message(&self.form, message))
    }

    /// This reader as a [`Stream`](futures::Stream) of decoded records, one at a
    /// time, ending once caught up (the same shape as the async typed reader in
    /// the Python binding). Each item is a value or its position-carrying decode
    /// error, so one poison record does not end the stream. Drive it with
    /// `futures::StreamExt`.
    pub fn stream(self) -> impl futures::Stream<Item = Result<TypedRecord<T>, TypedDecodeError>> {
        futures::stream::unfold(self, |mut records| async move {
            records.next().await.map(|item| (item, records))
        })
    }

    /// One bounded poll, decoded: everything appended since the last read (at
    /// most [`batch`](Self::batch) messages per partition), each record either
    /// a value or its position-carrying decode error. An empty vec means
    /// caught up. The batch altitude under [`next`](Self::next), for callers
    /// that checkpoint per poll.
    pub async fn poll(
        &mut self,
    ) -> Result<Vec<Result<TypedRecord<T>, TypedDecodeError>>, LaserError> {
        let mut decoded: Vec<_> = self
            .buffered
            .drain(..)
            .map(|message| decode_message(&self.form, message))
            .collect();
        if decoded.is_empty() {
            decoded = self
                .cursor
                .poll()
                .await?
                .into_iter()
                .map(|message| decode_message(&self.form, message))
                .collect();
        }
        Ok(decoded)
    }
}

/// One record decoded off the log: the typed body plus where it sits
/// (partition and offset) and its user headers.
#[derive(Clone, Debug)]
pub struct TypedRecord<T> {
    /// The payload decoded as the handle's body type.
    pub value: T,
    /// The record's log position, from its own message header.
    pub position: MessageId,
    /// The record's user headers decoded to strings.
    pub headers: BTreeMap<String, String>,
}

/// A typed read that could not produce a value: a record whose payload does
/// not decode as the handle's type (`position` names the exact record on the
/// log), or a poll that failed before reaching any record (`position` is
/// `None`).
#[derive(Debug, thiserror::Error)]
pub struct TypedDecodeError {
    /// The failed record's log position. `None` when the poll itself failed.
    pub position: Option<MessageId>,
    /// What went wrong: a codec failure for a positioned record, the transport
    /// error for a failed poll.
    pub source: LaserError,
}

/// Folds into the codec family so `records.next()` composes with `?` in a
/// `Result<_, LaserError>` function, the position kept in the message.
impl From<TypedDecodeError> for LaserError {
    fn from(error: TypedDecodeError) -> Self {
        match error.position {
            Some(_) => LaserError::Codec(error.to_string()),
            None => error.source,
        }
    }
}

impl std::fmt::Display for TypedDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.position {
            Some(position) => write!(
                formatter,
                "record at {position} does not decode: {}",
                self.source
            ),
            None => write!(formatter, "typed read failed to poll: {}", self.source),
        }
    }
}

fn decode_message<T: DeserializeOwned>(
    form: &Form,
    message: Message,
) -> Result<TypedRecord<T>, TypedDecodeError> {
    match form.decode::<T>(&message.payload) {
        Ok(value) => Ok(TypedRecord {
            value,
            position: message.id,
            headers: message.headers,
        }),
        Err(source) => Err(TypedDecodeError {
            position: Some(message.id),
            source,
        }),
    }
}

// How the handle turns bodies into payloads and back. The compiled schema is
// resolved and parsed once at handle construction, cloned (cheaply, the
// expensive parts are shared) into each reader.
#[derive(Clone)]
enum Form {
    Json,
    Cbor,
    #[cfg(feature = "schema-codecs")]
    Schema {
        id: u32,
        compiled: crate::schema_codecs::CompiledSchema,
    },
}

impl Form {
    fn encode<T: Serialize>(
        &self,
        body: &T,
    ) -> Result<(Vec<u8>, ContentType, Option<u32>), LaserError> {
        match self {
            Self::Json => Ok((Json::encode(body)?, ContentType::Json, None)),
            Self::Cbor => Ok((Cbor::encode(body)?, ContentType::Cbor, None)),
            #[cfg(feature = "schema-codecs")]
            Self::Schema { id, compiled } => {
                use crate::schema_codecs::CompiledSchema;
                match compiled {
                    CompiledSchema::Avro(_) => {
                        Ok((compiled.encode_avro(body)?, ContentType::Avro, Some(*id)))
                    }
                    CompiledSchema::Json(_) => {
                        let value = serde_json::to_value(body).map_err(|error| {
                            LaserError::Codec(format!("body does not lower to JSON: {error}"))
                        })?;
                        if !compiled.validate_value(&value) {
                            return Err(LaserError::Codec(format!(
                                "body fails schema {id}'s JSON Schema"
                            )));
                        }
                        let payload = serde_json::to_vec(&value).map_err(|error| {
                            LaserError::Codec(format!("encode JSON payload: {error}"))
                        })?;
                        Ok((payload, ContentType::Json, Some(*id)))
                    }
                    CompiledSchema::Protobuf(_) => Err(LaserError::Invalid(format!(
                        "schema {id} is Protobuf: encode the body with prost and publish it via \
                         .raw_bytes(bytes, ContentType::Protobuf).schema_id({id}). The typed \
                         handle still decodes Protobuf records"
                    ))),
                }
            }
        }
    }

    fn decode<T: DeserializeOwned>(&self, payload: &[u8]) -> Result<T, LaserError> {
        match self {
            Self::Json => Ok(<Json as Decoder<T>>::decode(payload)?),
            Self::Cbor => Ok(<Cbor as Decoder<T>>::decode(payload)?),
            #[cfg(feature = "schema-codecs")]
            Self::Schema { compiled, .. } => {
                let value = compiled.decode(payload)?;
                serde_json::from_value(value).map_err(|error| {
                    LaserError::Codec(format!(
                        "decoded record does not fit the body type: {error}"
                    ))
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Order {
        customer: String,
        amount: i64,
    }

    fn order() -> Order {
        Order {
            customer: "alice".to_owned(),
            amount: 42,
        }
    }

    #[test]
    fn given_the_json_form_when_encoded_then_should_round_trip_with_the_json_tag() {
        let form = Form::Json;
        let (payload, content_type, schema_id) = form.encode(&order()).expect("body encodes");
        assert_eq!(content_type, ContentType::Json);
        assert_eq!(schema_id, None);
        let back: Order = form.decode(&payload).expect("payload decodes");
        assert_eq!(back, order());
    }

    #[test]
    fn given_the_cbor_form_when_encoded_then_should_round_trip_with_the_cbor_tag() {
        let form = Form::Cbor;
        let (payload, content_type, schema_id) = form.encode(&order()).expect("body encodes");
        assert_eq!(content_type, ContentType::Cbor);
        assert_eq!(schema_id, None);
        let back: Order = form.decode(&payload).expect("payload decodes");
        assert_eq!(back, order());
    }

    #[test]
    fn given_a_wrong_payload_when_decoded_then_should_fail_as_codec() {
        let form = Form::Json;
        let result: Result<Order, _> = form.decode(b"not json");
        assert!(matches!(result, Err(LaserError::Codec(_))));
    }

    #[cfg(feature = "schema-codecs")]
    mod schema_bound {
        use super::*;
        use crate::query::{SchemaDef, SchemaSource};
        use crate::schema_codecs::CompiledSchema;

        const ORDER_AVRO: &str = r#"{
            "type":"record","name":"Order",
            "fields":[
                {"name":"customer","type":"string"},
                {"name":"amount","type":"long"}
            ]
        }"#;

        fn schema_form(source: SchemaSource) -> Form {
            let compiled = CompiledSchema::compile(&SchemaDef {
                id: 7,
                source,
                name: None,
                version: None,
            })
            .expect("schema compiles");
            Form::Schema { id: 7, compiled }
        }

        #[test]
        fn given_an_avro_schema_when_encoded_then_should_round_trip_stamping_the_id() {
            let form = schema_form(SchemaSource::Avro {
                schema: ORDER_AVRO.to_owned(),
            });
            let (payload, content_type, schema_id) = form.encode(&order()).expect("body encodes");
            assert_eq!(content_type, ContentType::Avro);
            assert_eq!(schema_id, Some(7));
            let back: Order = form.decode(&payload).expect("datum decodes");
            assert_eq!(back, order());
        }

        #[test]
        fn given_a_json_schema_when_the_body_fails_it_then_should_refuse_before_send() {
            let form = schema_form(SchemaSource::JsonSchema {
                schema: r#"{
                    "type":"object",
                    "required":["customer","amount"],
                    "properties":{"amount":{"type":"integer","minimum":100}}
                }"#
                .to_owned(),
            });
            assert!(matches!(form.encode(&order()), Err(LaserError::Codec(_))));
        }

        #[test]
        fn given_a_json_schema_when_the_body_passes_then_should_stamp_json_plus_id() {
            let form = schema_form(SchemaSource::JsonSchema {
                schema: r#"{"type":"object","required":["customer","amount"]}"#.to_owned(),
            });
            let (payload, content_type, schema_id) = form.encode(&order()).expect("body encodes");
            assert_eq!(content_type, ContentType::Json);
            assert_eq!(schema_id, Some(7));
            let back: Order = form.decode(&payload).expect("payload decodes");
            assert_eq!(back, order());
        }
    }

    #[test]
    fn given_a_positioned_failure_when_displayed_then_should_name_the_record() {
        let error = TypedDecodeError {
            position: Some(MessageId::new(2, 17)),
            source: LaserError::Codec("bad".to_owned()),
        };
        assert!(error.to_string().contains("2:17"));
    }
}
