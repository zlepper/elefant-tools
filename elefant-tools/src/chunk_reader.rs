use tokio::io::{AsyncBufRead, AsyncBufReadExt};

/// A trait for reading chunks of strings from a reader until a separator line is encountered.
pub(crate) trait StringChunkReader {
    // separator should include the newline
    fn read_lines_until_separator_line(&mut self, separator: &str, s: &mut String) -> impl std::future::Future<Output=std::io::Result<ChunkResult>> + Send;
}

impl<R> StringChunkReader for R
    where R: AsyncBufRead + Send + Unpin
{
    
    async fn read_lines_until_separator_line(&mut self, separator: &str, s: &mut String) ->std::io::Result<ChunkResult> {
        
        let mut total_read = 0;
        let separator_length = separator.len();
        
        loop {
            let read = self.read_line(s).await?;
            
            if read == 0 {
                return Ok(ChunkResult::End(total_read));
            }
            
            
            if read == separator_length && s.ends_with(&separator) {
                s.truncate(s.len() - separator_length);
                return Ok(ChunkResult::Chunk(total_read))
            }

            total_read += read;
        }
        
        
    }
}



#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum ChunkResult {
    /// A chunk of data was read, however we haven't reached the end of the file yet.
    Chunk(usize),
    /// A chunk of data was read, and we have reached the end of the file.
    End(usize),
}



#[cfg(test)]
mod tests {

        use super::*;
        use tokio::test;
        
        #[test]
        async fn test_read_lines_until_separator_line() {
            let bytes = "hello\n|\nworld\n|\n".as_bytes();
            let mut reader = tokio::io::BufReader::new(bytes);

            let mut s = String::new();
            let result = reader.read_lines_until_separator_line("|\n", &mut s).await;
            assert_eq!(result.unwrap(), ChunkResult::Chunk(6));
            assert_eq!(s, "hello\n");

            let mut s = String::new();
            let result = reader.read_lines_until_separator_line("|\n", &mut s).await;
            assert_eq!(result.unwrap(), ChunkResult::Chunk(6));
            assert_eq!(s, "world\n");
            
            let mut s = String::new();
            let result = reader.read_lines_until_separator_line("|\n", &mut s).await;
            assert_eq!(result.unwrap(), ChunkResult::End(0));
            assert_eq!(s, "");
        }
        
        #[test]
        async fn multi_line_chunks() {
            let bytes = "hello\nworld\n|\nhej\nverden\n|\n".as_bytes();
            let mut reader = tokio::io::BufReader::new(bytes);

            let mut s = String::new();
            let result = reader.read_lines_until_separator_line("|\n", &mut s).await;
            assert_eq!(result.unwrap(), ChunkResult::Chunk(12));
            assert_eq!(s, "hello\nworld\n");

            let mut s = String::new();
            let result = reader.read_lines_until_separator_line("|\n", &mut s).await;
            assert_eq!(result.unwrap(), ChunkResult::Chunk(11));
            assert_eq!(s, "hej\nverden\n");
            
            let mut s = String::new();
            let result = reader.read_lines_until_separator_line("|\n", &mut s).await;
            assert_eq!(result.unwrap(), ChunkResult::End(0));
            assert_eq!(s, "");
        }
        
        #[test]
        async fn end_of_file() {
            let bytes = "hello\nworld\n|\nhej\nverden\n".as_bytes();
            let mut reader = tokio::io::BufReader::new(bytes);

            let mut s = String::new();
            let result = reader.read_lines_until_separator_line("|\n", &mut s).await;
            assert_eq!(result.unwrap(), ChunkResult::Chunk(12));
            assert_eq!(s, "hello\nworld\n");

            let mut s = String::new();
            let result = reader.read_lines_until_separator_line("|\n", &mut s).await;
            assert_eq!(result.unwrap(), ChunkResult::End(11));
            assert_eq!(s, "hej\nverden\n");
        }
        
    }