use crate::store::Event;

pub trait Aggregate: Default {
    fn apply(&mut self, event: &'_ Event);
    fn aggregate_type<'a>() -> &'a str;

    fn aggregate_id<I: Into<String>>(id: I) -> String {
        format!("{}#{}", Self::aggregate_type(), id.into())
    }

    fn to_id<I: Into<String>>(aggregate_id: I) -> String {
        let id: String = aggregate_id.into();

        id.replacen(&format!("{}#", Self::aggregate_type()), "", 1)
    }
}
