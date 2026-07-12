use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

macro_rules! string_newtype {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
        }
    };
}

string_newtype!(EventId);
string_newtype!(CallId);
string_newtype!(ItemId);
string_newtype!(ResponseId);
string_newtype!(Base64Pcm);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SampleRate(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AudioFormat {
    #[serde(rename = "pcm16")]
    Pcm16,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum ClientEvent {
    #[serde(rename = "session.update")]
    SessionUpdate {
        #[serde(skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
        session: SessionUpdate,
    },
    #[serde(rename = "input_audio_buffer.append")]
    InputAudioBufferAppend {
        #[serde(skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
        audio: Base64Pcm,
    },
    #[serde(rename = "input_audio_buffer.commit")]
    InputAudioBufferCommit {
        #[serde(skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
    },
    #[serde(rename = "response.create")]
    ResponseCreate {
        #[serde(skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        response: Option<ResponseCreateOptions>,
    },
    #[serde(rename = "response.cancel")]
    ResponseCancel {
        #[serde(skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        response_id: Option<ResponseId>,
    },
    #[serde(rename = "conversation.item.create")]
    ConversationItemCreate {
        #[serde(skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
        item: ConversationItem,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionUpdate {
    pub modalities: Vec<Modality>,
    pub voice: String,
    pub input_audio_format: AudioFormat,
    pub output_audio_format: AudioFormat,
    pub input_audio_sample_rate: SampleRate,
    pub output_audio_sample_rate: SampleRate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Modality {
    Text,
    Audio,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ResponseCreateOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ConversationItem {
    #[serde(rename = "function_call_output")]
    FunctionCallOutput {
        call_id: CallId,
        output: FunctionCallOutput,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FunctionCallOutput(pub String);

#[derive(Debug, Clone, PartialEq)]
pub enum ServerEvent {
    SessionUpdated(SessionUpdatedEvent),
    ResponseAudioDelta(ResponseAudioDeltaEvent),
    ResponseAudioDone(ResponseAudioDoneEvent),
    ResponseAudioTranscriptDelta(ResponseAudioTranscriptDeltaEvent),
    ResponseAudioTranscriptDone(ResponseAudioTranscriptDoneEvent),
    ResponseFunctionCallArgumentsDone(ResponseFunctionCallArgumentsDoneEvent),
    Error(ErrorEvent),
    Unknown { event_type: String },
}

impl<'de> Deserialize<'de> for ServerEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::missing_field("type"))?;

        macro_rules! decode {
            ($variant:ident, $ty:ty) => {
                serde_json::from_value::<$ty>(value)
                    .map(Self::$variant)
                    .map_err(D::Error::custom)
            };
        }

        match event_type {
            "session.updated" => decode!(SessionUpdated, SessionUpdatedEvent),
            "response.audio.delta" => decode!(ResponseAudioDelta, ResponseAudioDeltaEvent),
            "response.audio.done" => decode!(ResponseAudioDone, ResponseAudioDoneEvent),
            "response.audio_transcript.delta" => decode!(
                ResponseAudioTranscriptDelta,
                ResponseAudioTranscriptDeltaEvent
            ),
            "response.audio_transcript.done" => {
                decode!(
                    ResponseAudioTranscriptDone,
                    ResponseAudioTranscriptDoneEvent
                )
            }
            "response.function_call_arguments.done" => decode!(
                ResponseFunctionCallArgumentsDone,
                ResponseFunctionCallArgumentsDoneEvent
            ),
            "error" => decode!(Error, ErrorEvent),
            other => Ok(Self::Unknown {
                event_type: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SessionUpdatedEvent {
    #[serde(default)]
    pub event_id: Option<EventId>,
    pub session: SessionUpdate,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ResponseAudioDeltaEvent {
    #[serde(default)]
    pub event_id: Option<EventId>,
    pub response_id: ResponseId,
    pub item_id: ItemId,
    pub output_index: u32,
    pub content_index: u32,
    pub delta: Base64Pcm,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ResponseAudioDoneEvent {
    #[serde(default)]
    pub event_id: Option<EventId>,
    pub response_id: ResponseId,
    pub item_id: ItemId,
    pub output_index: u32,
    pub content_index: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ResponseAudioTranscriptDeltaEvent {
    #[serde(default)]
    pub event_id: Option<EventId>,
    pub response_id: ResponseId,
    pub item_id: ItemId,
    pub output_index: u32,
    pub content_index: u32,
    pub delta: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ResponseAudioTranscriptDoneEvent {
    #[serde(default)]
    pub event_id: Option<EventId>,
    pub response_id: ResponseId,
    pub item_id: ItemId,
    pub output_index: u32,
    pub content_index: u32,
    pub transcript: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ResponseFunctionCallArgumentsDoneEvent {
    #[serde(default)]
    pub event_id: Option<EventId>,
    pub response_id: ResponseId,
    pub item_id: ItemId,
    pub output_index: u32,
    pub call_id: CallId,
    pub name: String,
    pub arguments: FunctionArguments,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FunctionArguments(pub String);

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ErrorEvent {
    #[serde(default)]
    pub event_id: Option<EventId>,
    pub error: RealtimeError,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RealtimeError {
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub code: Option<String>,
    pub message: String,
    #[serde(default)]
    pub param: Option<String>,
    #[serde(default)]
    pub event_id: Option<EventId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serializes_audio_append_with_typed_payload() {
        let event = ClientEvent::InputAudioBufferAppend {
            event_id: Some(EventId::new("evt-1")),
            audio: Base64Pcm::new("AQID"),
        };

        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({"type":"input_audio_buffer.append","event_id":"evt-1","audio":"AQID"})
        );
    }

    #[test]
    fn round_trips_function_call_output() {
        let event = ClientEvent::ConversationItemCreate {
            event_id: None,
            item: ConversationItem::FunctionCallOutput {
                call_id: CallId::new("call-1"),
                output: FunctionCallOutput("{\"ok\":true}".to_string()),
            },
        };

        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["type"], "conversation.item.create");
        assert_eq!(value["item"]["type"], "function_call_output");
        assert_eq!(value["item"]["call_id"], "call-1");
    }

    #[test]
    fn deserializes_audio_delta() {
        let event: ServerEvent = serde_json::from_value(json!({
            "type":"response.audio.delta",
            "event_id":"evt-2",
            "response_id":"resp-1",
            "item_id":"item-1",
            "output_index":0,
            "content_index":0,
            "delta":"AQID"
        }))
        .unwrap();

        let ServerEvent::ResponseAudioDelta(delta) = event else {
            panic!("expected audio delta");
        };
        assert_eq!(delta.delta, Base64Pcm::new("AQID"));
        assert_eq!(delta.response_id, ResponseId::new("resp-1"));
    }

    #[test]
    fn preserves_unknown_event_type_without_untyped_payload() {
        let event: ServerEvent = serde_json::from_value(json!({
            "type":"response.future_extension",
            "arbitrary":{"nested":true}
        }))
        .unwrap();

        assert_eq!(
            event,
            ServerEvent::Unknown {
                event_type: "response.future_extension".to_string()
            }
        );
    }

    #[test]
    fn deserializes_structured_error() {
        let event: ServerEvent = serde_json::from_value(json!({
            "type":"error",
            "event_id":"evt-3",
            "error": {
                "type":"invalid_request_error",
                "code":"invalid_audio",
                "message":"audio is invalid",
                "param":"audio",
                "event_id":"evt-client"
            }
        }))
        .unwrap();

        let ServerEvent::Error(error) = event else {
            panic!("expected error event");
        };
        assert_eq!(error.error.code.as_deref(), Some("invalid_audio"));
        assert_eq!(error.error.event_id, Some(EventId::new("evt-client")));
    }
}
