use std::io::{Read,Write,BufRead};

extern crate bufstream;
use self::bufstream::BufStream;

extern crate rustc_serialize;
use rustc_serialize::json::{Json};

use ::object::{BusinessObject, Payload, ReadBusinessObjectError};


pub trait ReadBusinessObject<E> {
    fn read_business_object(&mut self) -> Result<BusinessObject, E>;
}


impl <S: Read + Write> ReadBusinessObject <ReadBusinessObjectError> for S {
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
