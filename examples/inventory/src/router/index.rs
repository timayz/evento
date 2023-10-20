use askama::Template;

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate<'a> {
    name: &'a str,
}

pub async fn index() -> IndexTemplate<'static> {
    IndexTemplate { name: "" }
}
