pub(crate) trait StringExt {
    fn push_join(&mut self, separator: &str, items: impl IntoIterator<Item = impl AsRef<str>>);
}

impl StringExt for String {
    fn push_join(&mut self, separator: &str, items: impl IntoIterator<Item = impl AsRef<str>>) {
        for (idx, v) in items.into_iter().enumerate() {
            if idx > 0 {
                self.push_str(separator);
            }
            self.push_str(v.as_ref());
        }
    }
}
