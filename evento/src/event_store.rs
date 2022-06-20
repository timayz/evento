pub trait EventStore {}

pub struct SQLiteEventStore {}

impl EventStore for SQLiteEventStore {}

pub struct PostgresEventStore {}

impl EventStore for PostgresEventStore {}
