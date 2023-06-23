/// Return the SDK version
pub fn version_string() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_string() {
        assert!(!version_string().is_empty());
    }
}
