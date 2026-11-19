use evento::Cursor;
use sea_query::Iden;

#[derive(Iden, Clone)]
pub enum ContactAdmin {
    Id,
}

#[derive(Cursor)]
pub struct AdminView {
    #[cursor(by_name(ContactAdmin::Id, 1, 2))]
    pub id: String,
}

fn main() {}
