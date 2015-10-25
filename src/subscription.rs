use rustc_serialize::json::{Json, ToJson};


#[derive(Eq, PartialEq, Debug, Clone)]
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


pub fn parse_subscription(subscription: &Json) -> Result<BusinessSubscription, BusinessSubscriptionError> {
    if subscription.is_string() {
        Ok(BusinessSubscription::String(String::from(subscription.as_string().unwrap())))
    } else if subscription.as_array().is_some() {
        let array = subscription.as_array().unwrap();

        let mut result = Vec::new();
        let mut error: Option<BusinessSubscriptionError> = None;
        for item in array.iter() {
            match parse_subscription(item) {
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
        Err(BusinessSubscriptionError::JsonTypeError(subscription.to_json()))
    }
}


fn match_hierarchical(matcher: &str, matchable: &str) -> bool {
    let matcher_parts: Vec<&str> = matcher.split('/').collect();
    let matchable_parts: Vec<&str> = matchable.split('/').collect();

    for (index, matcher_part) in matcher_parts.iter().enumerate() {
        if matcher_part == &"*" {
            return true;
        }

        if index >= matchable_parts.len() {
            return false;
        }

        let matchable_part = matchable_parts[index];
        if matcher_part != &matchable_part {
            return false;
        }
    }

    true
}


pub fn match_hierarchical_subscription(matcher: BusinessSubscription,
                                       matchable: BusinessSubscription) -> bool {
    match (matcher, matchable) {
        (BusinessSubscription::String(mr), BusinessSubscription::String(me)) => {
            return match_hierarchical(&mr, &me);
        }
        (_, _) => {}
    }

    false
}


fn subscription_vec_to_str_vec<'a>(subscription_vec: &'a Vec<BusinessSubscription>) -> Option<Vec<&str>> {
    let mut result: Vec<&str> = Vec::new();

    for item in subscription_vec.iter() {
        match *item {
            BusinessSubscription::String(ref s) => {
                result.push(s);
            },
            _ => {
                return None;
            }
        }
    }

    Some(result)
}


fn routing_decision_aux(natures: Option<Vec<&str>>, event: Option<&str>, payload_type: Option<&str>,
                        subscription_rules: &Vec<BusinessSubscription>) -> bool {
    let rules_opt = subscription_vec_to_str_vec(&subscription_rules);
    if rules_opt.is_none() {
        return false;
    }
    let rules = rules_opt.unwrap();

    let mut pass = false;

    for mut rule in rules {
        let is_negative_rule = rule.starts_with("!");
        if is_negative_rule {
            rule = &rule[1..rule.len()];
        }

        if rule.starts_with("#") {
            rule = &rule[1..rule.len()];
            match natures {
                Some(ref nature_list) => {
                    for nature in nature_list {
                        if match_hierarchical(rule, nature) {
                            pass = ! is_negative_rule;
                            break;
                        }
                    }
                },
                None => {}
            }
        } else if rule.starts_with("@") {
            rule = &rule[1..rule.len()];
            match event {
                Some(event) => {
                    if match_hierarchical(rule, event) {
                        pass = ! is_negative_rule
                    }
                },
                None => {}
            }
        } else if rule == "*" || match payload_type { Some(payload_type) => match_hierarchical(rule, payload_type),
                                                      None => false } {
            pass = ! is_negative_rule
        }
    }

    pass
}


pub fn routing_decision(natures: Option<Vec<&str>>, event: Option<&str>, payload_type: Option<&str>,
                        subscription: &BusinessSubscription) -> bool {
    let mut payload_type_aux = payload_type;

    // Remove trailing extra qualifiers for type for matching purposes
    match payload_type {
        Some(val) => {
            if val.contains(";") {
                let parts: Vec<&str> = val.split(";").collect();
                payload_type_aux = Some(parts[0].trim());
            }
        },
        None => {}
    };

    match subscription {
        &BusinessSubscription::List(ref rule_list) =>
            routing_decision_aux(natures, event, payload_type_aux, rule_list),
        _ => { false }
    }
}


#[cfg(test)]
mod tests {
    use super::{BusinessSubscription, match_hierarchical_subscription, routing_decision};

    fn bs(bs: &str) -> BusinessSubscription {
        BusinessSubscription::String(bs.to_string())
    }

    fn bs_list(bs_vec: Vec<BusinessSubscription>) -> BusinessSubscription {
        BusinessSubscription::List(bs_vec)
    }

    #[test]
    fn match_hierarchical_equal() {
        assert!(match_hierarchical_subscription(bs("routing/subscribe"),
                                                bs("routing/subscribe")) == true);
    }

    #[test]
    fn match_hierarchical_kleene_star() {
        assert!(match_hierarchical_subscription(bs("routing/*"),
                                                bs("routing/subscribe")));
    }

    #[test]
    fn match_hierarchical_kleene_star_shouldnt_work() {
        assert!(!match_hierarchical_subscription(bs("routing/subscribe"),
                                                 bs("routing/*")));
    }

    #[test]
    fn match_hierarchical_kleene_star_should_match_anything() {
        assert!(match_hierarchical_subscription(bs("*"),
                                                bs("routing/subscribe")));
        assert!(match_hierarchical_subscription(bs("*"),
                                                bs("services/discovery")));
        assert!(match_hierarchical_subscription(bs("*"),
                                                bs("")));
    }

    #[test]
    fn routing_decision_should_work_with_events() {
        assert!(routing_decision(None,
                                 Some("routing/announcement"),
                                 None,
                                 &bs_list(vec!(bs("@routing/*")))));

        assert!(!
                routing_decision(None,
                                 Some("routing/announcement"),
                                 None,
                                 &bs_list(vec!(bs("!@routing/*")))));

        assert!(!
                routing_decision(None,
                                 Some("services/discovery"),
                                 None,
                                 &bs_list(vec!(bs("@routing/*")))));
    }

    #[test]
    fn routing_decision_should_work_with_natures() {
        assert!(routing_decision(Some(vec!("hasselhoff")),
                                 None,
                                 None,
                                 &bs_list(vec!(bs("#hasselhoff")))));

        assert!(!
                routing_decision(Some(vec!("hasselhoff")),
                                 None,
                                 None,
                                 &bs_list(vec!(bs("!#hasselhoff")))));
    }

    #[test]
    fn routing_decision_should_work_with_types() {
        assert!(routing_decision(None,
                                 None,
                                 Some("text/plain"),
                                 &bs_list(vec!(bs("text/*")))));

        assert!(!
                routing_decision(None,
                                 None,
                                 Some("text/plain"),
                                 &bs_list(vec!(bs("!text/*")))));
    }
}
