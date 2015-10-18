use std::collections::BTreeMap;
use std::io::{Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

extern crate rustc_serialize;
use rustc_serialize::json::{Json, ToJson};

extern crate time;
use time::{Timespec, Duration, get_time};

extern crate object_system;
use object_system::{BusinessObject, ReadBusinessObject};


#[derive(Debug)]
enum BusinessSubscription {
    List(Vec<BusinessSubscription>),
    String(String)
}


#[derive(Debug)]
enum BusinessSubscriptionError {
    JsonTypeError(Json),
    NoSubscriptionMetadataKey,
    SubscriptionNotEvent,
    UnknownSubscriptionEvent,
}


struct BusinessClient {
    subscription: Option<BusinessSubscription>,
    last_activity: Timespec,
}


impl BusinessClient {
    pub fn new() -> BusinessClient {
        BusinessClient {
            subscription: Option::None,
            last_activity: time::get_time(),
        }
    }
}


impl ToJson for BusinessSubscription {
    fn to_json(&self) -> Json {
        match *self {
            BusinessSubscription::List(ref subs) => {
                let mut result = Vec::new();

                for item in subs.iter() {
                    result.push(item.to_json())
                }

                result.to_json()
            },
            BusinessSubscription::String(ref s) => {
                s.to_json()
            }
        }
    }
}


fn handle_object(client: &mut BusinessClient, mut obj: &BusinessObject) {
    client.last_activity = time::get_time();
    println!("Handling object: {:?}", &obj.to_json());
}


fn parse_subscriptions(subscriptions: &Json) -> Result<BusinessSubscription, BusinessSubscriptionError> {
    if subscriptions.is_string() {
        Ok(BusinessSubscription::String(String::from(subscriptions.as_string().unwrap())))
    } else if subscriptions.as_array().is_some() {
        let array = subscriptions.as_array().unwrap();

        let mut result = Vec::new();
        let mut error: Option<BusinessSubscriptionError> = None;
        for item in array.iter() {
            match parse_subscriptions(item) {
                Ok(sub) => { result.push(sub); },
                Err(e) => {
                    error = Some(e);
                }
            }
        }

        if !error.is_some() {
            Ok(BusinessSubscription::List(result))
        } else {
            Err(error.unwrap())
        }
    } else {
        Err(BusinessSubscriptionError::JsonTypeError(subscriptions.to_json()))
    }
}


fn handle_subscription(obj: &BusinessObject) -> Result<BusinessSubscription, BusinessSubscriptionError> {
    // println!("Handling subscription: {:?}", &obj.to_json());
    match obj.event {
        Some(ref event) => {
            if event == "routing/subscribe" {
                match obj.metadata.get("subscriptions") {
                    Some(subscriptions) => {
                        match parse_subscriptions(subscriptions) {
                            Ok(subs) => Ok(subs),
                            Err(e) => Err(e)
                        }
                    },
                    None => Err(BusinessSubscriptionError::NoSubscriptionMetadataKey)
                }
            } else {
                Err(BusinessSubscriptionError::UnknownSubscriptionEvent)
            }
        },
        None => Err(BusinessSubscriptionError::SubscriptionNotEvent)
    }
}


fn subscription_reply(subscriptions: &BusinessSubscription, response: &BusinessObject) -> BusinessObject {
    let mut metadata = BTreeMap::new();
    metadata.insert("subscriptions".to_string(), subscriptions.to_json());

    match response.metadata.get("id") {
        Some(id) => {
            if id.is_string() {
                metadata.insert("in-reply-to".to_string(), id.as_string().unwrap().to_json());
            }
        },
        None => {}
    }

    BusinessObject {
        _type: None,
        payload: None,
        size: None,
        event: Some("routing/subscribe/reply".to_string()),
        metadata: metadata,
    }
}


fn handle_client(mut stream: TcpStream) {
    let mut client = BusinessClient::new();

    loop {
        match stream.read_business_object() {
            Ok(obj) => {
                match client.subscription {
                    Some(_) => {
                        handle_object(&mut client, &obj);
                    },
                    None => {
                        let mut subscription_result = handle_subscription(&obj);

                        if subscription_result.is_ok() {
                            let mut subscription = subscription_result.unwrap();
                            println!("Subscription OK: {:?}", &subscription);


                            let reply: BusinessObject = subscription_reply(&subscription, &obj);
                            println!("Reply JSON: {:?}", reply.to_json());

                            client.subscription = Some(subscription);
                            client.last_activity = time::get_time();
                            // client = BusinessClient { subscription: Some(subscription),
                            //                           last_activity: time::get_time(),
                            //                           .. client };

                            match stream.write(&reply.to_bytes()) {
                                Ok(bytes) => {
                                    println!("Reply sent ok: {} bytes", bytes);
                                },
                                Err(m) => {
                                    println!("Send err: {}", m);
                                },
                            };
                            stream.flush();
                        } else {
                            let e = subscription_result.unwrap_err();
                            println!("Ignored object: {:?}, error: {:?}", &obj.to_json(), &e);
                        }
                    }
                }
            }
            Err(_) => {}
        }

        thread::sleep_ms(500);

        if client.last_activity - time::get_time() >= Duration::seconds(1) {
            println!("Disconnecting!");
            break;
        }
    }
}


fn main() {
    let listener = TcpListener::bind("127.0.0.1:7890").unwrap();

    for stream in listener.incoming() {
        // stream.set_nodelay(true);
        match stream {
            Ok(stream) => {
                thread::spawn(move|| {
                    // connection succeeded
                    handle_client(stream)
                });
            }
            Err(e) => { /* connection failed */ }
        }
    }

    // close the socket server
    drop(listener);
}
