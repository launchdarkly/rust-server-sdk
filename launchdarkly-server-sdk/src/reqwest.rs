pub fn is_http_error_recoverable(status: u16) -> bool {
    if status < 400 || status >= 500 {
        return true;
    }

    matches!(
        status,
        400 | 408 | 429 // BAD_REQUEST | REQUEST_TIMEOUT | TOO_MANY_REQUESTS
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case("130.65331632653061", 130.65331632653061)]
    #[test_case("130.65331632653062", 130.65331632653061)]
    #[test_case("130.65331632653063", 130.65331632653064)]
    fn json_float_serialization_matches_go(float_as_string: &str, expected: f64) {
        let parsed: f64 = serde_json::from_str(float_as_string).unwrap();
        assert_eq!(expected, parsed);
    }

    #[test_case(100, true; "CONTINUE_STATUS")]
    #[test_case(200, true; "OK")]
    #[test_case(300, true; "MULTIPLE_CHOICES")]
    #[test_case(400, true; "BAD_REQUEST")]
    #[test_case(401, false; "UNAUTHORIZED")]
    #[test_case(408, true; "REQUEST_TIMEOUT")]
    #[test_case(409, false; "CONFLICT")]
    #[test_case(429, true; "TOO_MANY_REQUESTS")]
    #[test_case(431, false; "REQUEST_HEADER_FIELDS_TOO_LARGE")]
    #[test_case(500, true; "INTERNAL_SERVER_ERROR")]
    fn can_determine_recoverable_errors(status: u16, is_recoverable: bool) {
        assert_eq!(is_recoverable, is_http_error_recoverable(status));
    }
}
