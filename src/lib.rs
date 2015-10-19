extern crate rustc_serialize;

mod object;
mod subscription;
pub mod io;

pub use object::{BusinessObject, Payload};

pub mod server {
    pub use subscription::*;
}


#[test]
fn smoke_test_serialization_and_deserialization() {
    use std::collections::BTreeMap;

    use rustc_serialize::json::{Json};

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
