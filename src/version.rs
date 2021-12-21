use super::built_info;

pub fn version_string() -> &'static str {
    built_info::PKG_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_string() {
        assert!(!version_string().is_empty());
    }
}
