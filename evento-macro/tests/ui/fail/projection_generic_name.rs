#[evento::projection(name = "myapp/View")]
pub struct View<T: Default + Clone> {
    pub value: T,
}

fn main() {}
