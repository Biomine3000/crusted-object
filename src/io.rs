use std::io::{Read, Write};
use std::io;
use std::net as std_net;

use mio::tcp as mio_tcp;

use rustc_serialize::json::{Json};

use ::object::{BusinessObject, Payload, ReadBusinessObjectError};


const NUL: u8 = '\0' as u8;
const READ_BUF_SIZE: usize = 1024 * 1024;


pub trait ReadBusinessObject {
    fn read_business_objects(&mut self) -> Result<Vec<BusinessObject>, ReadBusinessObjectError>;
}


pub struct BusinessObjectStream<S: Read + Write> {
    read_buffer: Vec<u8>,
    pub socket: S,
}


impl <S: Read + Write> BusinessObjectStream<S> {
    pub fn new(socket: S) -> BusinessObjectStream<S> {
        BusinessObjectStream {
            read_buffer: Vec::new(),
            socket: socket,
        }
    }
}


impl Write for BusinessObjectStream<mio_tcp::TcpStream> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.socket.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}


impl Write for BusinessObjectStream<std_net::TcpStream> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.socket.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}


fn parse_one_object(buffer: &[u8]) -> Result<BusinessObject, ReadBusinessObjectError> {
    let mut vec: Vec<u8> = Vec::with_capacity(buffer.len());
    vec.extend(buffer);

    match String::from_utf8(vec) {
        Ok(utf8_string) => match Json::from_str(&utf8_string) {
            Ok(json_obj) => BusinessObject::from_json(&json_obj),
            Err(e) => Err(ReadBusinessObjectError::JsonSyntaxError(
                format!("{}", e), utf8_string))
        },
        Err(_) => Err(ReadBusinessObjectError::BufferCharacterDecodingError)
    }
}

enum ReadOneResult {
    Ok(BusinessObject, usize),
    NoNull,
    NotEnoughInput,
    NotEnoughPayloadInput,
    Error(ReadBusinessObjectError)
}


fn read_one_object(buffer:&[u8]) -> ReadOneResult {
    let nul_position = buffer.iter().position(|item| item == &NUL);

    if nul_position.is_none() {
        return ReadOneResult::NoNull;
    }
    let nul_pos = nul_position.unwrap();

    let metadata_part: &[u8] = &buffer[0 .. nul_pos];

    if metadata_part.len() == 0 {
        return ReadOneResult::NotEnoughInput;
    }

    // println!("metadata_part: {:?}", metadata_part);
    match parse_one_object(metadata_part) {
        Ok(obj) => {
            if obj.has_payload() {
                // println!("buf: {:?}", buffer);
                let payload_part: &[u8] = &buffer[nul_pos + 1 .. buffer.len()];
                // println!("payload_part: {:?}", payload_part);
                if obj.size.unwrap() > payload_part.len() {
                    debug!("Not enough input for size {}", payload_part.len());
                    return ReadOneResult::NotEnoughPayloadInput;
                }

                let size = obj.size.unwrap();
                let mut payload_vec: Vec<u8> = Vec::with_capacity(size);
                for item in buffer[nul_pos + 1 ..  nul_pos + 1 + size].iter() {
                    payload_vec.push(item.clone());
                }

                let result = BusinessObject { payload: Some(Payload::Bytes(payload_vec)),
                                              .. obj };
                ReadOneResult::Ok(result, nul_pos + 1 + size)
            } else {
                ReadOneResult::Ok(obj, nul_pos + 1)
            }
        },
        Err(e) => ReadOneResult::Error(e)
    }
}


fn read_objects(buffer: &[u8]) -> Result<(Vec<BusinessObject>, usize), ReadBusinessObjectError> {
    let mut result = Vec::new();

    let mut start = 0;
    loop {
        // println!("start: {:?}", start);
        match read_one_object(&buffer[start .. buffer.len()]) {
            ReadOneResult::Ok(obj, consumed) => {
                result.push(obj);
                start += consumed;
            },
            ReadOneResult::NoNull => {
            },
            ReadOneResult::Error(e) => {
                return Err(e);
            },
            ReadOneResult::NotEnoughInput => {
                break;
            },
            ReadOneResult::NotEnoughPayloadInput => {
            }
        }

        if start == buffer.len() {
            break;
        }
    }

    Ok((result, start))
}


impl <S: Read + Write> ReadBusinessObject for BusinessObjectStream<S> {
    fn read_business_objects(&mut self) -> Result<Vec<BusinessObject>, ReadBusinessObjectError> {
        let mut read_buf = [0; READ_BUF_SIZE];

        match self.socket.read(&mut read_buf) {
            Ok(0) => {
                warn!("Likely can't read from this socket any more!");
            },
            Ok(bytes_read) => {
                // println!("Bytes read: {}", bytes_read);
                for item in read_buf[0 .. bytes_read].iter() {
                    self.read_buffer.push(*item);
                }
            },
            Err(e) => {
                return Err(ReadBusinessObjectError::ReadError(e));
            }
        };

        match read_objects(&self.read_buffer) {
            Ok((objects, _)) => {
                // TODO: actual buffer management with consumed et al
                debug!("Got result: {:?}", objects);
                Ok(objects)
            },
            Err(e) => Err(e)
        }
    }
}


#[cfg(test)]
mod tests {
    use ::super::{read_objects, NUL};
    use ::object::{BusinessObject, Payload};


    fn nth_parsed_object (buffer: &Vec<u8>, index: usize) -> BusinessObject {
        let objs_result = read_objects(&buffer);
        
        match objs_result {
            Err(e) => {
                println!("{:?}", e);
                panic!()
            },
            _ => {}
        }

        let (objects, _) = objs_result.unwrap();
        (*objects.get(index).unwrap()).clone()
    }

    #[test]
    fn should_read_an_object_without_payload() {
        let mut buf: Vec<u8> = Vec::new();

        buf.extend(r#"{"event": "foo/bar"}"#.to_string().into_bytes());
        buf.push(NUL);
        assert_eq!("foo/bar", nth_parsed_object(&buf, 0).event.unwrap());

        buf.extend(r#"{"event": "bar/foo"}"#.to_string().into_bytes());
        buf.push(NUL);
        assert_eq!("foo/bar", nth_parsed_object(&buf, 0).event.unwrap());
        assert_eq!("bar/foo", nth_parsed_object(&buf, 1).event.unwrap());
    }

    #[test]
    fn should_read_two_objects_without_payload() {
        let mut buf: Vec<u8> = Vec::new();

        buf.extend(r#"{"event": "foo/bar"}"#.to_string().into_bytes());
        buf.push(NUL);
        buf.extend(r#"{"event": "bar/foo"}"#.to_string().into_bytes());
        buf.push(NUL);

        assert_eq!("foo/bar", nth_parsed_object(&buf, 0).event.unwrap());
        assert_eq!("bar/foo", nth_parsed_object(&buf, 1).event.unwrap());
    }

    #[test]
    fn should_read_an_object_with_payload() {
        let mut buf: Vec<u8> = Vec::new();

        buf.extend(r#"{"event": "foo/bar", "size": 5, "type": "text/plain"}"#.to_string().into_bytes());
        buf.push(NUL);
        let payload = "ABCDE".to_string().into_bytes();
        buf.extend(&payload);

        let obj = nth_parsed_object(&buf, 0);
        assert_eq!("foo/bar", obj.event.unwrap());

        match obj.payload.unwrap() {
            Payload::Bytes(bytes) => {
                assert_eq!(payload, bytes);
            }
        }
    }

    #[test]
    fn should_read_two_objects_with_payloads() {
        let mut buf: Vec<u8> = Vec::new();

        buf.extend(r#"{"event": "foo/bar", "size": 5, "type": "text/plain"}"#.to_string().into_bytes());
        buf.push(NUL);
        let payload1 = "ABCDE".to_string().into_bytes();
        buf.extend(&payload1);

        buf.extend(r#"{"event": "bar/foo", "size": 5, "type": "text/plain"}"#.to_string().into_bytes());
        buf.push(NUL);
        let payload2 = "EDCBA".to_string().into_bytes();
        buf.extend(&payload2);

        let obj = nth_parsed_object(&buf, 0);
        assert_eq!("foo/bar", obj.event.unwrap());

        match obj.payload.unwrap() {
            Payload::Bytes(bytes) => {
                assert_eq!(payload1, bytes);
            }
        }

        let obj = nth_parsed_object(&buf, 1);
        assert_eq!("bar/foo", obj.event.unwrap());

        match obj.payload.unwrap() {
            Payload::Bytes(bytes) => {
                assert_eq!(payload2, bytes);
            }
        }
    }
}
