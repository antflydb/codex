//! Ordered key encoding.
//!
//! Antfly scans return documents in byte order of their keys, so stores build
//! secondary orderings by encoding sort fields into keys. Components are
//! separated by `:` and escaped so that a separator never appears inside one.

/// Escapes one key component: bytes outside `[A-Za-z0-9._-]` become `%XX`.
pub fn escape(component: &str) -> String {
    let mut out = String::with_capacity(component.len());
    for byte in component.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Reverses [`escape`]. Returns `None` for malformed input.
pub fn unescape(component: &str) -> Option<String> {
    let bytes = component.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = component.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Joins already-escaped components with `:`.
pub fn join(components: &[&str]) -> String {
    components.join(":")
}

/// Fixed-width ascending encoding of an unsigned ordinal.
pub fn ordinal(value: u64) -> String {
    format!("{value:020}")
}

/// Fixed-width ascending encoding of a signed value such as a timestamp.
pub fn ascending(value: i64) -> String {
    // Shift into unsigned space so negative values sort first.
    ordinal((value as u64) ^ (1u64 << 63))
}

/// Fixed-width descending encoding: larger values sort first.
pub fn descending(value: i64) -> String {
    ordinal(!((value as u64) ^ (1u64 << 63)))
}

/// Decodes [`ascending`].
pub fn decode_ascending(encoded: &str) -> Option<i64> {
    let raw: u64 = encoded.parse().ok()?;
    Some((raw ^ (1u64 << 63)) as i64)
}

/// Decodes [`descending`].
pub fn decode_descending(encoded: &str) -> Option<i64> {
    let raw: u64 = encoded.parse().ok()?;
    Some(((!raw) ^ (1u64 << 63)) as i64)
}

/// Exclusive upper bound for a scan over every key that starts with `prefix`.
pub fn prefix_end(prefix: &str) -> String {
    // Escaped components are ASCII, so U+10FFFF (0xF4 0x8F 0xBF 0xBF) sorts
    // after every key that shares the prefix.
    format!("{prefix}\u{10FFFF}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn escape_round_trips() {
        for value in ["plain", "a:b", "/Users/me/my repo", "ünïcode", "%41"] {
            let escaped = escape(value);
            assert!(!escaped.contains(':'));
            assert_eq!(unescape(&escaped).as_deref(), Some(value));
        }
    }

    #[test]
    fn orderings_sort_as_bytes() {
        let values = [i64::MIN, -5, 0, 7, 1_700_000_000_000, i64::MAX];
        let asc: Vec<String> = values.iter().map(|v| ascending(*v)).collect();
        let mut sorted = asc.clone();
        sorted.sort();
        assert_eq!(asc, sorted);

        let desc: Vec<String> = values.iter().map(|v| descending(*v)).collect();
        let mut sorted = desc.clone();
        sorted.sort();
        sorted.reverse();
        assert_eq!(desc, sorted);

        for value in values {
            assert_eq!(decode_ascending(&ascending(value)), Some(value));
            assert_eq!(decode_descending(&descending(value)), Some(value));
        }
    }

    #[test]
    fn prefix_end_bounds_prefix() {
        let end = prefix_end("thread:");
        assert!("thread:zzzz" < end.as_str());
        assert!("threae" > "thread:");
    }
}
