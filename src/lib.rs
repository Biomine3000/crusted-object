use std::cmp::PartialEq;
use std::collections::BTreeMap;
use std::io::{Read,Write,BufRead};
use std::net::TcpStream;

extern crate bufstream;
use bufstream::BufStream;

extern crate rustc_serialize;
use rustc_serialize::json::{self, ToJson, Json};


pub struct BusinessObject {
    pub event: Option<String>,
    pub _type: Option<String>,
    pub size: Option<u64>,
    pub payload: Option<Payload>,
    pub metadata: BTreeMap<String,Json>
}


#[derive(Eq, PartialEq, Debug)]
pub enum Payload {
    Bytes(Vec<u8>)
}


#[derive(Eq, PartialEq, Debug)]
pub enum ReadBusinessObjectError {
    NoInputError(&'static str),

    JsonSemanticsError(&'static str),
    JsonSyntaxError(String, String),
    BufferCharacterDecodingError,

    PayloadReadingError
}


impl PartialEq for BusinessObject {
    fn eq(&self, other: &BusinessObject) -> bool {
        self.event == other.event &&
            self._type == other._type &&
            self.size == other.size &&
            self.payload == other.payload
    }
}


impl ToJson for BusinessObject {
    fn to_json(&self) -> Json {
        let mut d = BTreeMap::new();

        for (key, value) in self.metadata.iter() {
            d.insert(key.to_string(), value.clone());
        }

        if self._type.is_some() { d.insert("type".to_string(), (&self._type).clone().unwrap().to_json()); }
        if self.size.is_some() { d.insert("size".to_string(), (&self.size).clone().unwrap().to_json()); }
        if self.event.is_some() { d.insert("event".to_string(), (&self.event).clone().unwrap().to_json()); }

        Json::Object(d)
    }
}


impl BusinessObject {
    pub fn from_json(obj: &Json) -> Result<BusinessObject, ReadBusinessObjectError> {
        match obj.as_object() {
            Some(btree_obj) => Ok(btree_obj.to_business_object()),
            None => Err(ReadBusinessObjectError::JsonSemanticsError("Unsupported JSON type"))
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = self.to_json().to_string().into_bytes();
        result.push(b'\0');
        result
    }
}


trait ToBusinessObject {
    fn to_business_object(&self) -> BusinessObject;
}


impl ToBusinessObject for BTreeMap<String,Json> {
    fn to_business_object(&self) -> BusinessObject {
        let mut result = BusinessObject {
            event: None,
            _type: None,
            size: None,
            payload: None,
            metadata: BTreeMap::new()
        };

        let event = self.get("event");
        if event.is_some() {
            let value = event.unwrap().as_string();
            if value.is_some() {
                result.event = Some(value.unwrap().to_string());
            }
        }

        let _type = self.get("type");
        if _type.is_some() {
            let value = _type.unwrap().as_string();
            if value.is_some() {
                result._type = Some(value.unwrap().to_string());
            }
        }

        let size = self.get("size");
        if size.is_some() {
            let value = size.unwrap().as_u64();
            if value.is_some() {
                let s = value.unwrap();
                if s > -1 {
                    result.size = Some(s);
                }
            }
        }

        for (key, value) in self.iter() {
            if key == "event" || key == "type" || key == "size" {
                continue;
            }

            result.metadata.insert(key.to_string(), value.to_json());
        }

        result
    }
}


pub trait ReadBusinessObject<E> {
    fn read_business_object(&mut self) -> Result<BusinessObject, E>;
}


impl ReadBusinessObject<ReadBusinessObjectError> for TcpStream {
    fn read_business_object(&mut self) -> Result<BusinessObject, ReadBusinessObjectError> {
        let mut buf_stream = BufStream::new(self);
        let mut buffer = Vec::new();

        match buf_stream.read_until(b'\0', &mut buffer) {
            Ok(_) => {
                // Drop the null from the end.
                let buffer_size = buffer.len();
                match buffer_size.checked_sub(1) {
                    Some(new_size) => {
                        buffer.truncate(new_size);
                        match String::from_utf8(buffer) {
                            Ok(utf8_string) => match Json::from_str(&utf8_string) {
                                Ok(json_obj) => match BusinessObject::from_json(&json_obj) {
                                    Ok(obj) => {
                                        if obj.size.is_some() && obj.size.unwrap() > 0 {
                                            let mut payload_buf = Vec::new();
                                            match buf_stream.take(obj.size.unwrap()).read_to_end(&mut payload_buf) {
                                                Ok(_) => {
                                                    Ok(BusinessObject {
                                                        payload: Some(Payload::Bytes(payload_buf)),
                                                        .. obj })
                                                },
                                                Err(_) => Err(ReadBusinessObjectError::PayloadReadingError)
                                            }
                                        } else {
                                            Ok(obj)
                                        }
                                    },
                                    Err(e) => Err(e)
                                },
                                Err(e) => Err(ReadBusinessObjectError::JsonSyntaxError(
                                    format!("{}", e), utf8_string))
                            },
                            Err(_) => Err(ReadBusinessObjectError::BufferCharacterDecodingError)
                        }
                    },
                    None => Err(ReadBusinessObjectError::NoInputError("Object started with null"))
                }
            },
            Err(_) => Err(ReadBusinessObjectError::NoInputError("null not reached"))
        }
    }
}


#[test]
fn smoke_test_serialization_and_deserialization() {
    let mut metadata = BTreeMap::new();
    metadata.insert("subscriptions".to_string(),
                    vec!["@routing/*".to_string(), "@services/*".to_string(),
                         "@ping".to_string(), "@pong".to_string()].to_json());
    metadata.insert("subscriptions".to_string(), vec!["*".to_string()].to_json());

    let subscription = BusinessObject {
        _type: None,
        payload: None,
        size: None,
        event: Some("routing/subscribe".to_string()),
        metadata: metadata,
    };

    let json_repr_from = subscription.to_json();
    let string_repr = json_repr_from.to_string();
    let json_repr_to = Json::from_str(&string_repr).unwrap();
    let back = BusinessObject::from_json(&json_repr_to).unwrap();

    assert!(json_repr_from == json_repr_to);
    assert!(subscription == back);
}
