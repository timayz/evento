use askama::Template;

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate<'a> {
    _name: &'a str,
}

pub async fn index() -> IndexTemplate<'static> {
    IndexTemplate { _name: "" }
}
