use serde::Serialize;

pub type VariationIndex = usize;

#[derive(Clone, Debug, PartialEq)]
pub struct Detail<T> {
    pub value: Option<T>,
    pub variation_index: Option<VariationIndex>,
    pub reason: Reason,
}

impl<T> Detail<T> {
    pub fn empty(reason: Reason) -> Detail<T> {
        Detail {
            value: None,
            variation_index: None,
            reason,
        }
    }

    pub fn err_default(error: Error, default: T) -> Detail<T> {
        Detail {
            value: Some(default),
            variation_index: None,
            reason: Reason::Error { error },
        }
    }

    pub fn err(error: Error) -> Detail<T> {
        Detail::empty(Reason::Error { error })
    }

    pub fn map<U, F>(self, f: F) -> Detail<U>
    where
        F: FnOnce(T) -> U,
    {
        Detail {
            value: self.value.map(f),
            variation_index: self.variation_index,
            reason: self.reason,
        }
    }

    pub fn should_have_value(mut self, e: Error) -> Detail<T> {
        if self.value.is_none() {
            self.reason = Reason::Error { error: e };
        }
        self
    }

    pub fn try_map<U, F>(self, f: F, e: Error) -> Detail<U>
    where
        F: FnOnce(T) -> Option<U>,
    {
        if self.value.is_none() {
            return Detail {
                value: None,
                variation_index: self.variation_index,
                reason: self.reason,
            };
        }
        match f(self.value.unwrap()) {
            Some(v) => Detail {
                value: Some(v),
                variation_index: self.variation_index,
                reason: self.reason,
            },
            None => Detail::err(e),
        }
    }

    pub fn or(mut self, default: T) -> Detail<T> {
        if self.value.is_none() {
            self.value = Some(default);
            self.variation_index = None;
            // TODO reset reason?
        }
        self
    }

    pub fn or_else<F>(mut self, default: F) -> Detail<T>
    where
        F: Fn() -> T,
    {
        if self.value.is_none() {
            self.value = Some(default());
            self.variation_index = None;
        }
        self
    }
}

// Reason describes the reason that a flag evaluation producted a particular value.
// The Serialize implementation is used internally in cases where LaunchDarkly
// needs to unmarshal a Reason value from JSON.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "kind")]
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
    PrerequisiteFailed {
        #[serde(rename = "prerequisiteKey")]
        prerequisite_key: String,
    },
    // Fallthrough indicates that the flag was on but the user did not match any targets
    // or rules.
    Fallthrough,
    // EvalReasonError indicates that the flag could not be evaluated, e.g. because it does not
    // exist or due to an unexpected error. In this case the result value will be the default value
    // that the caller passed to the client.
    Error {
        #[serde(rename = "errorKind")]
        error: Error,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
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
    // WrongType indicates that the result value was not of the requested type, e.g. you
    // called BoolVariationDetail but the value was an integer.
    WrongType,
    // Exception indicates that an unexpected error stopped flag evaluation; check the
    // log for details.
    Exception,
}
