use std::borrow::Cow;
use std::num::NonZeroUsize;

pub trait Decoder<'a, O: 'a> {
    type Error: From<std::io::Error>;

    fn decode(buffer: &mut ByteSliceReader<'a>) -> DecodeResult<O, Self::Error>;
}

pub type DecodeResult<T, E> = Result<T, DecodeError<E>>;

#[derive(Debug)]
pub enum DecodeError<E> {
    NeedsMoreData(Option<NonZeroUsize>),
    Error(E),
}

impl<E> From<ByteSliceError> for DecodeError<E> {
    fn from(value: ByteSliceError) -> Self {
        match value {
            ByteSliceError::NeedsMoreData(n) => DecodeError::NeedsMoreData(n),
        }
    }
}

pub trait DecodeErrorError {}

impl<E: DecodeErrorError> From<E> for DecodeError<E> {
    fn from(value: E) -> Self {
        DecodeError::Error(value)
    }
}

#[derive(Debug)]
pub enum ByteSliceError {
    NeedsMoreData(Option<NonZeroUsize>),
}

pub struct ByteSliceReader<'a> {
    bytes: &'a [u8],
    read_bytes: usize,
}

impl<'a> ByteSliceReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            read_bytes: 0,
        }
    }

    #[inline]
    pub fn get_read_bytes(&self) -> usize {
        self.read_bytes
    }

    pub fn read_null_terminated_string(&mut self) -> Result<Cow<'a, str>, ByteSliceError> {
        let mut position = 0;
        loop {
            if self.bytes.len() <= position {
                return Err(ByteSliceError::NeedsMoreData(None));
            }
            if self.bytes[position] == 0 {
                break;
            }
            position += 1;
        }
        let (byt, remaining) = self.bytes.split_at(position);
        self.bytes = &remaining[1..];
        let result = String::from_utf8_lossy(byt);
        self.read_bytes += position + 1;
        Ok(result)
    }

    pub fn read_i32(&mut self) -> Result<i32, ByteSliceError> {
        if self.bytes.len() < 4 {
            return Err(ByteSliceError::NeedsMoreData(NonZeroUsize::new(4)));
        }

        let result =
            i32::from_be_bytes([self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3]]);
        self.bytes = &self.bytes[4..];
        self.read_bytes += 4;
        Ok(result)
    }

    pub fn read_i16(&mut self) -> Result<i16, ByteSliceError> {
        if self.bytes.len() < 2 {
            return Err(ByteSliceError::NeedsMoreData(NonZeroUsize::new(2)));
        }

        let result = i16::from_be_bytes([self.bytes[0], self.bytes[1]]);
        self.bytes = &self.bytes[2..];
        self.read_bytes += 2;
        Ok(result)
    }

    pub fn read_i8(&mut self) -> Result<i8, ByteSliceError> {
        if self.bytes.is_empty() {
            return Err(ByteSliceError::NeedsMoreData(NonZeroUsize::new(1)));
        }

        let result = i8::from_be_bytes([self.bytes[0]]);
        self.bytes = &self.bytes[1..];
        self.read_bytes += 1;
        Ok(result)
    }

    pub fn read_u8(&mut self) -> Result<u8, ByteSliceError> {
        if self.bytes.is_empty() {
            return Err(ByteSliceError::NeedsMoreData(NonZeroUsize::new(1)));
        }

        let result = self.bytes[0];
        self.bytes = &self.bytes[1..];
        self.read_bytes += 1;
        Ok(result)
    }

    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], ByteSliceError> {
        if n == 0 {
            return Ok(&[]);
        }

        match self.bytes.split_at_checked(n) {
            Some((byt, remaining)) => {
                self.bytes = remaining;
                self.read_bytes += n;
                Ok(byt)
            }
            None => Err(ByteSliceError::NeedsMoreData(NonZeroUsize::new(n))),
        }
    }

    pub fn read_exact(&mut self, slice: &mut [u8]) -> Result<(), ByteSliceError> {
        if slice.is_empty() {
            return Ok(());
        }

        match self.bytes.split_at_checked(slice.len()) {
            Some((byt, remaining)) => {
                slice.copy_from_slice(byt);
                self.bytes = remaining;
                self.read_bytes += slice.len();
                Ok(())
            }
            None => Err(ByteSliceError::NeedsMoreData(NonZeroUsize::new(
                slice.len(),
            ))),
        }
    }

    pub fn require_bytes(&self, required: usize) -> Result<(), ByteSliceError> {
        if self.bytes.len() < required {
            Err(ByteSliceError::NeedsMoreData(NonZeroUsize::new(required)))
        } else {
            Ok(())
        }
    }
}
