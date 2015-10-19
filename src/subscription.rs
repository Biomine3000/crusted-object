extern crate rustc_serialize;
use rustc_serialize::json::{Json, ToJson};

#[derive(Debug)]
pub enum BusinessSubscription {
    List(Vec<BusinessSubscription>),
    String(String)
}


#[derive(Debug)]
pub enum BusinessSubscriptionError {
    JsonTypeError(Json),
    NoSubscriptionMetadataKey,
    SubscriptionNotEvent,
    UnknownSubscriptionEvent,
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


pub fn parse_subscriptions(subscriptions: &Json) -> Result<BusinessSubscription, BusinessSubscriptionError> {
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
