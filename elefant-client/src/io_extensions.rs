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
}

impl<W: AsyncWrite + Unpin> AsyncWriteExt2 for W {}

// pub(crate) trait AsyncBufReadExt2: AsyncBufRead + Unpin {
//     async fn read_null_terminated_string(&mut self) -> Result<(String, usize), std::io::Error> {
//         let mut buf = Vec::new();
//         let bytes_read = self.read_until(b'\0', &mut buf).await?;
//         buf.pop();
//
//         String::from_utf8(buf)
//             .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
//             .map(|s| (s, bytes_read))
//     }
// }
//
// impl<R: AsyncBufRead + Unpin> AsyncBufReadExt2 for R {}

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

