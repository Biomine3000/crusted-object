use std::collections::BTreeMap;
use std::io::Write;
use std::net::TcpStream;

extern crate rustc_serialize;
use rustc_serialize::json::{ToJson};

extern crate object_system;
use object_system::BusinessObject;
use object_system::io::*;


fn main() {
    let socket_stream = TcpStream::connect("localhost:7890").unwrap();
    // socket_stream.set_nodelay(true);
    let mut stream = BusinessObjectStream::new(socket_stream);

    let mut metadata = BTreeMap::new();
    // metadata.insert("subscriptions".to_string(),
    //                 vec!["@routing/*".to_string(), "@services/*".to_string(),
    //                      "@ping".to_string(), "@pong".to_string()].to_json());
    metadata.insert("subscriptions".to_string(), vec!["*".to_string()].to_json());

    let subscription = BusinessObject {
        _type: None,
        payload: None,
        size: None,
        event: Some("routing/subscribe".to_string()),
        metadata: metadata,
    };

    match stream.write(&subscription.to_bytes()) {
        Ok(bytes) => {
            println!("Send ok: {} bytes", bytes);
        },
        Err(m) => {
            println!("Send err: {}", m);
        },
    };

    let _ = stream.flush();

    let obj = stream.read_business_objects().unwrap();
    println!("Got: {:?}", &obj.to_json());

    let ping = BusinessObject {
        _type: None,
        payload: None,
        size: None,
        event: Some("ping".to_string()),
        metadata: BTreeMap::new(),
    };

    println!("Wrote {} bytes.", stream.write(&ping.to_bytes()).unwrap());

    let obj = stream.read_business_objects().unwrap();
    println!("Got: {:?}", &obj.to_json());
}
