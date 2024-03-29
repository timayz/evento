use serde::{de::DeserializeOwned, Serialize};

use crate::store::Event;

pub trait Applier: Default + Serialize + DeserializeOwned {
    fn apply(&mut self, event: &'_ Event);
}

pub trait Aggregate {
    fn aggregate_type() -> &'static str;
    fn aggregate_version() -> &'static str;

    fn to_aggregate_id<I: Into<String>>(id: I) -> String {
        format!("{}#{}", Self::aggregate_type(), id.into())
    }

    fn from_aggregate_id<I: Into<String>>(aggregate_id: I) -> String {
        let id: String = aggregate_id.into();

        id.replacen(&format!("{}#", Self::aggregate_type()), "", 1)
    }
}
