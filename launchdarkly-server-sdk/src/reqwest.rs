use hyper::StatusCode;

pub fn is_http_error_recoverable(status: u16) -> bool {
    if let Ok(status) = StatusCode::from_u16(status) {
        if !status.is_client_error() {
            return true;
        }

        return matches!(
            status,
            StatusCode::BAD_REQUEST | StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
        );
    }

    warn!("Unable to determine if status code is recoverable");
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::StatusCode;
    use test_case::test_case;

    #[test_case("130.65331632653061", 130.65331632653061)]
    #[test_case("130.65331632653062", 130.65331632653061)]
    #[test_case("130.65331632653063", 130.65331632653064)]
    fn json_float_serialization_matches_go(float_as_string: &str, expected: f64) {
        let parsed: f64 = serde_json::from_str(float_as_string).unwrap();
        assert_eq!(expected, parsed);
    }

    #[test_case(StatusCode::CONTINUE, true)]
    #[test_case(StatusCode::OK, true)]
    #[test_case(StatusCode::MULTIPLE_CHOICES, true)]
    #[test_case(StatusCode::BAD_REQUEST, true)]
    #[test_case(StatusCode::UNAUTHORIZED, false)]
    #[test_case(StatusCode::REQUEST_TIMEOUT, true)]
    #[test_case(StatusCode::CONFLICT, false)]
    #[test_case(StatusCode::TOO_MANY_REQUESTS, true)]
    #[test_case(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE, false)]
    #[test_case(StatusCode::INTERNAL_SERVER_ERROR, true)]
    fn can_determine_recoverable_errors(status: StatusCode, is_recoverable: bool) {
        assert_eq!(is_recoverable, is_http_error_recoverable(status.as_u16()));
    }
}
