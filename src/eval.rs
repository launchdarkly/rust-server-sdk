use std::fmt::{self, Display, Formatter};

#[derive(Debug, PartialEq)]
pub struct Detail<T> {
    pub value: Option<T>,
    pub reason: Reason,
}

impl<T> Detail<T> {
    pub fn new(value: T, reason: Reason) -> Detail<T> {
        Detail {
            value: Some(value),
            reason,
        }
    }

    pub fn should(value: Option<T>, reason: Reason) -> Detail<T> {
        value
            .map(|value| Detail::new(value, reason))
            .unwrap_or(Detail::err(Error::MalformedFlag))
    }

    pub fn maybe(value: Option<T>, reason: Reason) -> Detail<T> {
        Detail { value, reason }
    }

    pub fn err(e: Error) -> Detail<T> {
        Detail {
            value: None,
            reason: Reason::Error(e),
        }
    }

    pub fn map<U, F>(self, f: F) -> Detail<U>
    where
        F: FnOnce(T) -> U,
    {
        Detail {
            value: self.value.map(f),
            reason: self.reason,
        }
    }

    pub fn try_map<U, F>(self, f: F, e: Error) -> Detail<U>
    where
        F: FnOnce(T) -> Option<U>,
    {
        if self.value.is_none() {
            return Detail {
                value: None,
                reason: self.reason,
            };
        }
        match f(self.value.unwrap()) {
            Some(v) => Detail {
                value: Some(v),
                reason: self.reason,
            },
            None => Detail::err(e),
        }
    }

    pub fn or(self, default: T) -> Detail<T> {
        match self.value {
            Some(_) => self,
            None => Detail {
                value: Some(default),
                reason: self.reason,
            },
        }
    }

    pub fn or_else<F>(self, default: F) -> Detail<T>
    where
        F: Fn() -> T,
    {
        match self.value {
            Some(_) => self,
            None => Detail {
                value: Some(default()),
                reason: self.reason,
            },
        }
    }
}

// Reason describes the reason that a flag evaluation producted a particular value.
#[derive(Debug, PartialEq)]
pub enum Reason {
    // Off indicates that the flag was off and therefore returned its configured off value.
    Off,
    // TargetMatch indicates that the user key was specifically targeted for this flag.
    TargetMatch,
    // RuleMatch indicates that the user matched one of the flag's rules.
    // TODO include ruleIndex and ruleId
    RuleMatch,
    // PrerequisiteFailed indicates that the flag was considered off because it had at
    // least one prerequisite flag that either was off or did not return the desired variation.
    // TODO include prerequisiteKey
    PrerequisiteFailed,
    // Fallthrough indicates that the flag was on but the user did not match any targets
    // or rules.
    Fallthrough,
    // EvalReasonError indicates that the flag could not be evaluated, e.g. because it does not
    // exist or due to an unexpected error. In this case the result value will be the default value
    // that the caller passed to the client.
    Error(Error),
}

impl Display for Reason {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.code())
    }
}

impl Reason {
    fn code(&self) -> &'static str {
        match self {
            Reason::Off => "OFF",
            Reason::TargetMatch => "TARGET_MATCH",
            Reason::RuleMatch => "RULE_MATCH",
            Reason::PrerequisiteFailed => "PREREQUISITE_FAILED",
            Reason::Fallthrough => "FALLTHROUGH",
            Reason::Error(_) => "ERROR",
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Error {
    // ClientNotReady indicates that the caller tried to evaluate a flag before the client
    // had successfully initialized.
    ClientNotReady,
    // FlagNotFound indicates that the caller provided a flag key that did not match any
    // known flag.
    FlagNotFound,
    // MalformedFlag indicates that there was an internal inconsistency in the flag data,
    // e.g. a rule specified a nonexistent variation.
    MalformedFlag,
    // UserNotSpecified indicates that the caller passed a user without a key for the user
    // parameter.
    UserNotSpecified,
    // WrongType indicates that the result value was not of the requested type, e.g. you
    // called BoolVariationDetail but the value was an integer.
    WrongType,
    // Exception indicates that an unexpected error stopped flag evaluation; check the
    // log for details.
    Exception,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.code())
    }
}

impl Error {
    fn code(&self) -> &'static str {
        match self {
            // ClientNotReady indicates that the caller tried to evaluate a flag before the client
            // had successfully initialized.
            Error::ClientNotReady => "CLIENT_NOT_READY",
            // FlagNotFound indicates that the caller provided a flag key that did not match any
            // known flag.
            Error::FlagNotFound => "FLAG_NOT_FOUND",
            // MalformedFlag indicates that there was an internal inconsistency in the flag data,
            // e.g. a rule specified a nonexistent variation.
            Error::MalformedFlag => "MALFORMED_FLAG",
            // UserNotSpecified indicates that the caller passed a user without a key for the user
            // parameter.
            Error::UserNotSpecified => "USER_NOT_SPECIFIED",
            // WrongType indicates that the result value was not of the requested type, e.g. you
            // called BoolVariationDetail but the value was an integer.
            Error::WrongType => "WRONG_TYPE",
            // Exception indicates that an unexpected error stopped flag evaluation; check the
            // log for details.
            Error::Exception => "EXCEPTION",
        }
    }
}
