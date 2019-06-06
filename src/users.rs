pub struct User {
    pub key: String,
}

impl User {
    pub fn new<S: Into<String>>(s: S) -> User {
        User { key: s.into() }
    }
}
