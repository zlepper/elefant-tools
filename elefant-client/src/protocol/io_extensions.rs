use std::borrow::Cow;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

macro_rules! impl_read_integer {
    ($name:ident, $ty:ty) => {
        #[inline]
        async fn $name(&mut self) -> Result<$ty, std::io::Error> {
            let buf = self.read_bytes::< {std::mem::size_of::<$ty>() }>().await?;
            Ok(<$ty>::from_be_bytes(buf))
        }
    };
}
pub(crate) trait AsyncReadExt2: AsyncRead + Unpin {
    impl_read_integer!(read_u32, u32);
    impl_read_integer!(read_u16, u16);
    impl_read_integer!(read_u8, u8);
    impl_read_integer!(read_i32, i32);
    impl_read_integer!(read_i16, i16);
    impl_read_integer!(read_i8, i8);

    #[inline]
    async fn read_bytes<const N: usize>(&mut self) -> Result<[u8; N], std::io::Error> {
        let mut buf = [0; N];
        self.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

impl<R: AsyncRead + Unpin> AsyncReadExt2 for R {}


pub(crate) trait AsyncWriteExt2: AsyncWrite + Unpin {
    async fn write_i32(&mut self, value: i32) -> Result<(), std::io::Error> {
        self.write_all(&value.to_be_bytes()).await
    }

    async fn write_i16(&mut self, value: i16) -> Result<(), std::io::Error> {
        self.write_all(&value.to_be_bytes()).await
    }

    async fn write_i8(&mut self, value: i8) -> Result<(), std::io::Error> {
        self.write_all(&value.to_be_bytes()).await
    }

    async fn write_u32(&mut self, value: u32) -> Result<(), std::io::Error> {
        self.write_all(&value.to_be_bytes()).await
    }

    async fn write_u16(&mut self, value: u16) -> Result<(), std::io::Error> {
        self.write_all(&value.to_be_bytes()).await
    }

    async fn write_u8(&mut self, value: u8) -> Result<(), std::io::Error> {
        self.write_all(&value.to_be_bytes()).await
    }
    
    async fn write_null_terminated_string(&mut self, value: &str) -> Result<(), std::io::Error> {
        self.write_all(value.as_bytes()).await?;
        self.write_u8(0).await
    }
}

impl<W: AsyncWrite + Unpin> AsyncWriteExt2 for W {}

pub(crate) trait ByteSliceExt<const N: usize>: Sized {
    fn from_be_bytes(bytes: Self) -> Self {
        bytes
    }

    fn to_be_bytes(self) -> Self {
        self
    }
}

impl<const N: usize> ByteSliceExt<N> for [u8; N] {

}

pub(crate) struct ByteSliceReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> ByteSliceReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            cursor: 0,
        }
    }
    
    pub fn read_null_terminated_string(&mut self) -> Result<Cow<'a, str>, std::io::Error> {
        let start = self.cursor;
        loop {
            if self.bytes.len() <= self.cursor {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "No end of null-terminated string"));
            }
            if self.bytes[self.cursor] == 0 {
                break;
            }
            self.cursor += 1;
        }
        let result = String::from_utf8_lossy(&self.bytes[start..self.cursor]);
        self.cursor += 1;
        Ok(result)
    }
    
    pub fn read_i32(&mut self) -> Result<i32, std::io::Error> {
        if self.bytes.len() < self.cursor + 4 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes to read i32"));
        }
        
        let result = i32::from_be_bytes([
            self.bytes[self.cursor],
            self.bytes[self.cursor + 1],
            self.bytes[self.cursor + 2],
            self.bytes[self.cursor + 3],
        ]);
        self.cursor += 4;
        Ok(result)
    }
    
    pub fn read_i16(&mut self) -> Result<i16, std::io::Error> {
        if self.bytes.len() < self.cursor + 2 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes to read i16"));
        }
        
        let result = i16::from_be_bytes([
            self.bytes[self.cursor],
            self.bytes[self.cursor + 1],
        ]);
        self.cursor += 2;
        Ok(result)
    }
    
    pub fn read_u8(&mut self) -> Result<u8, std::io::Error> {
        if self.bytes.len() < self.cursor + 1 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes to read u8"));
        }
        
        let result = self.bytes[self.cursor];
        self.cursor += 1;
        Ok(result)
    }
    
    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], std::io::Error> {
        if self.bytes.len() < self.cursor + n {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes to read"));
        }
        
        let result = &self.bytes[self.cursor..self.cursor + n];
        self.cursor += n;
        Ok(result)
    }
}



