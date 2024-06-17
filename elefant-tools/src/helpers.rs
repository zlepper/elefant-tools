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

pub(crate) static IMPORT_PREFIX: &str = r#"
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET check_function_bodies = false;
SET xmloption = content;
SET row_security = off;
"#;
