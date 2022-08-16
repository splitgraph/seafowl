use crate::config::schema::{str_to_hex_hash, AccessSettings, HttpFrontend};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    Anonymous,
    Writer,
    Reader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resource {
    Database,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessPolicy {
    pub read: AccessSettings,
    pub write: AccessSettings,
}

impl AccessPolicy {
    pub fn from_config(config: &HttpFrontend) -> Self {
        Self {
            read: config.read_access.clone(),
            write: config.write_access.clone(),
        }
    }

    pub fn free_for_all() -> Self {
        Self {
            read: AccessSettings::Any,
            write: AccessSettings::Any,
        }
    }

    pub fn with_read_disabled(self) -> Self {
        Self {
            read: AccessSettings::Off,
            write: self.write,
        }
    }

    pub fn with_read_password(self, password: &str) -> Self {
        Self {
            read: AccessSettings::Password {
                sha256_hash: str_to_hex_hash(password),
            },
            write: self.write,
        }
    }

    pub fn with_write_disabled(self) -> Self {
        Self {
            read: self.read,
            write: AccessSettings::Off,
        }
    }

    pub fn with_write_password(self, password: &str) -> Self {
        Self {
            read: self.read,
            write: AccessSettings::Password {
                sha256_hash: str_to_hex_hash(password),
            },
        }
    }
}

pub fn token_to_principal(
    token: Option<String>,
    policy: &AccessPolicy,
    // TODO: error enums instead of strings
) -> Result<Principal, String> {
    match (token, &policy.write, &policy.read) {
        // If both read and write require a password and the user didn't pass a token: error
        (
            None,
            AccessSettings::Off | AccessSettings::Password { sha256_hash: _ },
            AccessSettings::Off | AccessSettings::Password { sha256_hash: _ },
        ) => Err("UNAUTHORIZED".to_string()),
        (None, _, _) => Ok(Principal::Anonymous),
        // If password auth is disabled and the user passed a token: error
        (
            Some(_),
            AccessSettings::Any | AccessSettings::Off,
            AccessSettings::Any | AccessSettings::Off,
        ) => Err("TOKEN_NOT_NEEDED".to_string()),

        (Some(t), AccessSettings::Password { sha256_hash }, _)
            if str_to_hex_hash(&t) == sha256_hash.as_str() =>
        {
            Ok(Principal::Writer)
        }
        (Some(t), _, AccessSettings::Password { sha256_hash })
            if str_to_hex_hash(&t) == sha256_hash.as_str() =>
        {
            Ok(Principal::Reader)
        }
        // If the token's hash didn't match: error (TODO 401?)
        (Some(_), _, _) => Err("WRONG_PASSWORD".to_string()),
    }
}

pub fn can_perform_action(
    principal: &Principal,
    action: Action,
    _: Resource,
    policy: &AccessPolicy,
) -> bool {
    matches!(
        (principal, action, &policy.read, &policy.write),
        // Writer can do anything (note we don't issue Writer/Reader if the policy for Write/Read doesn't have a password)
        (Principal::Writer, _, _, _)
        // Reader can always read
            | (Principal::Reader, Action::Read, _, _)
        // Anyone can read if we enabled reads for everyone
            | (_, Action::Read, AccessSettings::Any, _)
        // Anyone can write if we enabled writes for everyone
            | (_, Action::Write, _, AccessSettings::Any)
    )
}

pub struct UserContext {
    pub principal: Principal,
    pub policy: AccessPolicy,
}

impl UserContext {
    pub fn can_perform_action(&self, action: Action) -> bool {
        can_perform_action(&self.principal, action, Resource::Database, &self.policy)
    }
}

#[cfg(test)]
mod tests {
    use crate::auth::{Action, UserContext};

    use super::{token_to_principal, AccessPolicy, Principal};

    const READ_PW: &str = "read_password";
    const WRITE_PW: &str = "write_password";

    fn free_for_all() -> AccessPolicy {
        AccessPolicy::free_for_all()
    }

    fn need_write_pw() -> AccessPolicy {
        AccessPolicy::free_for_all().with_write_password(WRITE_PW)
    }

    fn read_only_write_off() -> AccessPolicy {
        AccessPolicy::free_for_all().with_write_disabled()
    }

    fn read_pw_write_off() -> AccessPolicy {
        AccessPolicy::free_for_all()
            .with_write_disabled()
            .with_read_password(READ_PW)
    }

    fn read_pw_write_pw() -> AccessPolicy {
        AccessPolicy::free_for_all()
            .with_write_password(WRITE_PW)
            .with_read_password(READ_PW)
    }

    #[test]
    fn test_all_allowed_disallows_token() {
        assert_eq!(
            token_to_principal(Some(READ_PW.to_string()), &free_for_all()),
            Err("TOKEN_NOT_NEEDED".to_string())
        )
    }

    #[test]
    fn test_all_allowed_anon() {
        let policy = free_for_all();
        assert_eq!(token_to_principal(None, &policy), Ok(Principal::Anonymous));

        let context = UserContext {
            principal: Principal::Anonymous,
            policy,
        };

        assert!(context.can_perform_action(Action::Read));
        assert!(context.can_perform_action(Action::Write));
    }

    #[test]
    fn test_write_pw_wrong_token() {
        let policy = need_write_pw();
        assert_eq!(
            token_to_principal(Some(READ_PW.to_string()), &policy),
            Err("WRONG_PASSWORD".to_string())
        );
    }

    #[test]
    fn test_write_pw_correct_token_can_read_write() {
        let policy = need_write_pw();
        assert_eq!(
            token_to_principal(Some(WRITE_PW.to_string()), &policy),
            Ok(Principal::Writer)
        );

        let context = UserContext {
            principal: Principal::Writer,
            policy,
        };
        assert!(context.can_perform_action(Action::Read));
        assert!(context.can_perform_action(Action::Write));
    }

    #[test]
    fn test_write_pw_anonymous_only_read() {
        let policy = need_write_pw();
        assert_eq!(token_to_principal(None, &policy), Ok(Principal::Anonymous));

        let context = UserContext {
            principal: Principal::Anonymous,
            policy,
        };

        assert!(context.can_perform_action(Action::Read));
        assert!(!context.can_perform_action(Action::Write));
    }

    #[test]
    fn test_read_only_disallows_token() {
        assert_eq!(
            token_to_principal(Some(READ_PW.to_string()), &read_only_write_off()),
            Err("TOKEN_NOT_NEEDED".to_string())
        )
    }

    #[test]
    fn test_read_only_can_read_cant_write() {
        let policy = read_only_write_off();
        assert_eq!(token_to_principal(None, &policy), Ok(Principal::Anonymous));

        let context = UserContext {
            principal: Principal::Anonymous,
            policy,
        };

        assert!(context.can_perform_action(Action::Read));
        assert!(!context.can_perform_action(Action::Write));
    }

    #[test]
    fn test_read_pw_write_off_disallows_anon() {
        assert_eq!(
            token_to_principal(None, &read_pw_write_off()),
            Err("UNAUTHORIZED".to_string())
        );
    }

    #[test]
    fn test_read_pw_write_off_only_read() {
        let policy = read_pw_write_off();
        assert_eq!(
            token_to_principal(Some(READ_PW.to_string()), &policy),
            Ok(Principal::Reader)
        );
        let context = UserContext {
            principal: Principal::Reader,
            policy,
        };

        assert!(context.can_perform_action(Action::Read));
        assert!(!context.can_perform_action(Action::Write));
    }

    #[test]
    fn test_read_write_pw_disallows_anon() {
        assert_eq!(
            token_to_principal(None, &read_pw_write_pw()),
            Err("UNAUTHORIZED".to_string())
        );
    }

    #[test]
    fn test_read_write_pw_reader_can_only_read() {
        let policy = read_pw_write_pw();
        assert_eq!(
            token_to_principal(Some(READ_PW.to_string()), &policy),
            Ok(Principal::Reader)
        );
        let context = UserContext {
            principal: Principal::Reader,
            policy,
        };

        assert!(context.can_perform_action(Action::Read));
        assert!(!context.can_perform_action(Action::Write));
    }

    #[test]
    fn test_read_write_pw_writer_can_read_write() {
        let policy = read_pw_write_pw();
        assert_eq!(
            token_to_principal(Some(WRITE_PW.to_string()), &policy),
            Ok(Principal::Writer)
        );
        let context = UserContext {
            principal: Principal::Writer,
            policy,
        };

        assert!(context.can_perform_action(Action::Read));
        assert!(context.can_perform_action(Action::Write));
    }
}
