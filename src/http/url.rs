use url::Url;

pub fn ensure_directory(url: &Url) -> Url {
    let mut result = url.clone();

    if let Ok(mut segments) = result.path_segments_mut() {
        segments.pop_if_empty().push("");
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_trailing_slash() {
        let url1 = Url::parse("https://example.net/a/b/c/").unwrap();
        let url2 = Url::parse("https://example.net/a/b/c/").unwrap();
        assert_eq!(ensure_directory(&url1), url2);
    }

    #[test]
    fn test_path_no_trailing_slash() {
        let url1 = Url::parse("https://example.net/a/b/c").unwrap();
        let url2 = Url::parse("https://example.net/a/b/c/").unwrap();
        assert_eq!(ensure_directory(&url1), url2);
    }

    #[test]
    fn test_root_trailing_slash() {
        let url1 = Url::parse("https://example.net/").unwrap();
        let url2 = Url::parse("https://example.net/").unwrap();
        assert_eq!(ensure_directory(&url1), url2);
    }

    #[test]
    fn test_root_no_trailing_slash() {
        let url1 = Url::parse("https://example.net").unwrap();
        let url2 = Url::parse("https://example.net/").unwrap();
        assert_eq!(ensure_directory(&url1), url2);
    }

    #[test]
    fn test_query_trailing_slash() {
        let url1 = Url::parse("https://example.net/a/b/c/?query=asdf").unwrap();
        let url2 = Url::parse("https://example.net/a/b/c/?query=asdf").unwrap();
        assert_eq!(ensure_directory(&url1), url2);
    }

    #[test]
    fn test_query_no_trailing_slash() {
        let url1 = Url::parse("https://example.net/a/b/c?query=asdf").unwrap();
        let url2 = Url::parse("https://example.net/a/b/c/?query=asdf").unwrap();
        assert_eq!(ensure_directory(&url1), url2);
    }

    #[test]
    fn test_fragment_trailing_slash() {
        let url1 = Url::parse("https://example.net/a/b/c/#fragment").unwrap();
        let url2 = Url::parse("https://example.net/a/b/c/#fragment").unwrap();
        assert_eq!(ensure_directory(&url1), url2);
    }

    #[test]
    fn test_fragment_no_trailing_slash() {
        let url1 = Url::parse("https://example.net/a/b/c#fragment").unwrap();
        let url2 = Url::parse("https://example.net/a/b/c/#fragment").unwrap();
        assert_eq!(ensure_directory(&url1), url2);
    }

    #[test]
    fn test_mailto() {
        let url1 = Url::parse("mailto:user@example.com").unwrap();
        let url2 = Url::parse("mailto:user@example.com").unwrap();
        assert_eq!(ensure_directory(&url1), url2);
    }
}
