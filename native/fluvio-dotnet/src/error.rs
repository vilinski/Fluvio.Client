// native/fluvio-dotnet/src/error.rs
pub mod codes {
    pub const GENERIC: i32 = 1;
    pub const CONNECTION: i32 = 2;
    pub const TOPIC_NOT_FOUND: i32 = 3;
    pub const TOPIC_ALREADY_EXISTS: i32 = 4;
    pub const CANCELLED: i32 = 5;
    pub const INVALID_ARGUMENT: i32 = 6;
    pub const UNAUTHORIZED: i32 = 7;
}

pub fn to_ffi(e: &anyhow::Error) -> (i32, String) {
    let msg = e.chain().map(|c| c.to_string()).collect::<Vec<_>>().join(": ");
    let lower = msg.to_lowercase();
    let code = if lower.contains("already exists") {
        codes::TOPIC_ALREADY_EXISTS
    } else if lower.contains("not found") || lower.contains("unknowntopic") {
        codes::TOPIC_NOT_FOUND
    } else if lower.contains("unauthorized") || lower.contains("permission") {
        codes::UNAUTHORIZED
    } else if lower.contains("invalid") {
        codes::INVALID_ARGUMENT
    } else if lower.contains("connect") || lower.contains("timeout") || lower.contains("timed out") {
        codes::CONNECTION
    } else {
        codes::GENERIC
    };
    (code, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_already_exists() {
        let e = anyhow::anyhow!("Topic 'foo' already exists");
        assert_eq!(to_ffi(&e).0, codes::TOPIC_ALREADY_EXISTS);
    }

    #[test]
    fn classifies_not_found() {
        let e = anyhow::anyhow!("topic not found: foo");
        assert_eq!(to_ffi(&e).0, codes::TOPIC_NOT_FOUND);
    }

    #[test]
    fn classifies_connection_failure() {
        let e = anyhow::anyhow!("failed to connect to cluster: timed out");
        assert_eq!(to_ffi(&e).0, codes::CONNECTION);
    }

    #[test]
    fn defaults_to_generic() {
        let e = anyhow::anyhow!("something unexpected happened");
        assert_eq!(to_ffi(&e).0, codes::GENERIC);
    }
}
