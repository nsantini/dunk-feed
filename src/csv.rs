//! The one CSV field escaper `dunk validate` and `dunk dump` both render
//! through (spec `2026-09-21-dump`, BC12). Moved out of `src/validate.rs`,
//! which used to hold a private copy, so the two commands can never drift
//! apart on the quoting rule.

/// Wraps `field` in `"..."` when it holds a `,`, a `"`, a `\n` or a `\r`,
/// doubling each inner `"`. `\r` is a wider rule than `validate`'s own
/// former copy checked: no row `validate` renders today carries one, so
/// adding it changes nothing there, and `dump`'s CSV can carry a post's
/// original text indirectly through no field of its own, but the rule
/// covers the character regardless.
pub fn quote(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::quote;

    #[test]
    fn plain_field_is_unquoted() {
        assert_eq!(quote("plain"), "plain");
    }

    #[test]
    fn comma_forces_quoting() {
        assert_eq!(quote("a,b"), "\"a,b\"");
    }

    #[test]
    fn inner_quote_is_doubled() {
        assert_eq!(quote("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn newline_forces_quoting() {
        assert_eq!(quote("a\nb"), "\"a\nb\"");
    }

    #[test]
    fn carriage_return_forces_quoting() {
        assert_eq!(quote("a\rb"), "\"a\rb\"");
    }
}
