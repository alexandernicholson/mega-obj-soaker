use glob_match::glob_match;

pub fn should_process_object(
    key: &str,
    prefix: &str,
    include_patterns: &[String],
    exclude_patterns: &[String],
) -> bool {
    let relative_key = key
        .strip_prefix(prefix)
        .unwrap_or(key)
        .trim_start_matches('/');

    if !include_patterns.is_empty()
        && !include_patterns
            .iter()
            .any(|p| glob_match(p, relative_key))
    {
        return false;
    }

    if !exclude_patterns.is_empty()
        && exclude_patterns
            .iter()
            .any(|p| glob_match(p, relative_key))
    {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_patterns_matches_all() {
        assert!(should_process_object("prefix/file.txt", "prefix/", &[], &[]));
    }

    #[test]
    fn include_pattern_filters() {
        let include = vec!["*.txt".to_string()];
        assert!(should_process_object("p/file.txt", "p/", &include, &[]));
        assert!(!should_process_object("p/file.jpg", "p/", &include, &[]));
    }

    #[test]
    fn exclude_pattern_filters() {
        let exclude = vec!["*.log".to_string()];
        assert!(should_process_object("p/file.txt", "p/", &[], &exclude));
        assert!(!should_process_object("p/file.log", "p/", &[], &exclude));
    }

    #[test]
    fn include_and_exclude_combined() {
        let include = vec!["*.txt".to_string()];
        let exclude = vec!["secret*".to_string()];
        assert!(should_process_object("p/readme.txt", "p/", &include, &exclude));
        assert!(!should_process_object("p/secret.txt", "p/", &include, &exclude));
        assert!(!should_process_object("p/image.jpg", "p/", &include, &exclude));
    }
}
