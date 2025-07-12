use crate::protocol::async_io::{ElefantAsyncRead, ElefantAsyncWrite};

mod decoder;
mod encoder;

pub use decoder::*;
pub use encoder::*;
use crate::profiler::Profiler;
use crate::reborrow_until_polonius;



pub(crate) struct Framed<S> {
    stream: S,
    read_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    start: usize,
    end: usize,
    needs_more: bool,
}

impl<S> Framed<S>  {

    pub fn new(stream: S) -> Self {
        Self {
            stream,
            read_buffer: vec![0; KB8],
            write_buffer: Vec::with_capacity(KB8),
            start: 0,
            end: 0,
            needs_more: false,
        }
    }
}

const KB8: usize = 8192;

impl<S: ElefantAsyncRead> Framed<S>  {

    pub async fn read_frame<'a, C: Decoder<'a, M>, M: 'a>(&'a mut self) -> Result<M, C::Error> {
        let _read_frame = Profiler::start("read_frame");
        loop {

            // let _read_frame_inner = Profiler::start("read_frame_inner");
            let me: &mut Framed<S> = reborrow_until_polonius!(self);
            match me.try_read_frame::<C, M>().await {
                Ok(Some(m)) => return Ok(m),
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }


    }

    async fn try_read_frame<'a, C: Decoder<'a, M>, M: 'a>(&'a mut self) -> Result<Option<M>, C::Error> {

        // let _try_read_frame = Profiler::start("try_read_frame");

        if self.needs_more {
            // let _needs_more = Profiler::start("needs_more");
            let read = self.stream.read(&mut self.read_buffer[self.end..]).await?;
            self.end += read;
            self.needs_more = false;
        }

        // When we are at the end of the current read, we need to read more data
        if self.start == self.end {
            // let _read_more_data = Profiler::start("read_more_data");
            self.end = self.stream.read(&mut self.read_buffer).await?;
            self.start = 0;
        }

        let me: &mut Framed<S> = reborrow_until_polonius!(self);

        let mut reader = ByteSliceReader::new(&self.read_buffer[self.start..self.end]);

        // let _dec = Profiler::start("decode");
        let part = C::decode(&mut reader);
        // drop(_dec);

        match part {
            Ok(m) => {
                self.start += reader.get_read_bytes();
                debug_assert!(self.start <= self.end);
                Ok(Some(m))
            },
            Err(DecodeError::NeedsMoreData(expected_more)) => {
                // let _needs_more_data = Profiler::start("needs_more_data");
                me.needs_more = true;
                let expected_more = expected_more.map(|u| u.get()).unwrap_or(KB8);
                let expected = reader.get_read_bytes() + expected_more;
                me.handle_need_for_more_data(expected);
                Ok(None)
            }
            Err(DecodeError::Error(e)) => {
                Err(e)
            },
        }

    }


    fn handle_need_for_more_data(&mut self, expected_total: usize) {
        // While it might be inefficient to always move the values to the start of the buffer,
        // We are going to have to wait for more values to arrive from the underlying reader,
        // so we might as well spend the time doing something useful.
        self.move_current_values_to_start_of_buffer();

        if expected_total > self.read_buffer.len() {
            let additional = expected_total - self.read_buffer.len();
            self.grow_buffer(additional);
        }
    }

    fn grow_buffer(&mut self, additional: usize) {
        self.read_buffer.reserve(additional);
        self.fill_buffer_to_capacity();
    }

    fn fill_buffer_to_capacity(&mut self) {
        let additional = self.read_buffer.capacity() - self.read_buffer.len();
        self.read_buffer.extend(std::iter::repeat_n(0, additional));
        debug_assert_eq!(self.read_buffer.len(), self.read_buffer.capacity());
    }

    fn move_current_values_to_start_of_buffer(&mut self) {
        if self.start > 0 && self.end > self.start {
            let remaining = self.end - self.start;
            self.read_buffer.copy_within(self.start..self.end, 0);
            self.end = remaining;
            self.start = 0;
        } else {
            // Values are already at the beginning of the buffer (Or we have no values,
            // in which case we also don't need to move anything)
        }
    }

}

impl<W: ElefantAsyncWrite> Framed<W> {

    pub async fn write_frame<'a, C: Encoder<'a, M>, M: 'a>(&'a mut self, message: M) -> Result<(), C::Error> {
        let _write_frame = Profiler::start("write_frame");
        self.write_buffer.clear();
        let _e = Profiler::start("encode");
        let mut writer = ByteSliceWriter::new(&mut self.write_buffer);
        C::encode(&mut writer, message)?;
        drop(_e);
        let _write_all = Profiler::start("write_all");
        self.stream.write_all(&self.write_buffer).await?;
        drop(_write_all);
        Ok(())
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush().await
    }
}



#[cfg(all(test, feature = "futures"))]
mod tests {
    use std::borrow::Cow;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use futures::{pin_mut, AsyncRead};
    use futures::io::Cursor;
    use crate::protocol::frame_reader::{ByteSliceReader, DecodeErrorError, Framed};
    use crate::protocol::frame_reader::decoder::{DecodeResult, Decoder};
    use crate::protocol::frame_reader::encoder::{ByteSliceWriter, Encoder};


    /// A reader that only reads up to a certain limit, even though
    /// the underlying reader might have more data available.
    struct LimitedReader<R> {
        reader: R,
        limit: usize,
    }

    impl LimitedReader<Cursor<Vec<u8>>> {
        fn new(data: Vec<u8>, limit: usize) -> LimitedReader<Cursor<Vec<u8>>> {
            Self {
                reader: Cursor::new(data),
                limit,
            }
        }
    }

    impl AsyncRead for LimitedReader<Cursor<Vec<u8>>> {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
            let safe_limit = std::cmp::min(self.limit, buf.len());

            let buf = &mut buf[..safe_limit];
            let reader = &mut self.reader;
            pin_mut!(reader);
            reader.poll_read(cx, buf)
        }
    }

    struct TestCodec;

    #[derive(Debug)]
    enum TestCodecError {
        IoError(std::io::Error),
        UnknownMessageType
    }

    impl From<std::io::Error> for TestCodecError {
        fn from(value: std::io::Error) -> Self {
            TestCodecError::IoError(value)
        }
    }

    impl DecodeErrorError for TestCodecError {}

    #[derive(Debug, PartialEq)]
    enum TestMessage<'a> {
        Ints(i32, i16, u8),
        String(Cow<'a, str>),
        Bytes(&'a [u8]),
    }

    impl<'a> Decoder<'a, TestMessage<'a>> for TestCodec {
        type Error = TestCodecError;

        fn decode(buffer: &mut ByteSliceReader<'a>) -> DecodeResult<TestMessage<'a>, Self::Error> {

            let typ = buffer.read_u8()?;

            match typ {
                1 => Ok(TestMessage::Ints(buffer.read_i32()?, buffer.read_i16()?, buffer.read_u8()?)),
                2 => {
                    let string = buffer.read_null_terminated_string()?;
                    Ok(TestMessage::String(string))
                },
                3 => {
                    let length = buffer.read_i32()?;
                    let bytes = buffer.read_bytes(length as usize)?;
                    Ok(TestMessage::Bytes(bytes))
                },
                _ => Err(TestCodecError::UnknownMessageType)?,
            }

        }
    }

    impl<'a> Encoder<'a, TestMessage<'a>> for TestCodec {
        type Error = TestCodecError;

        fn encode(destination: &mut ByteSliceWriter, input: TestMessage<'a>) -> Result<(), Self::Error> {
            match input {
                TestMessage::Ints(i32, i16, u8) => {
                    destination.write_u8(1);
                    destination.write_i32(i32);
                    destination.write_i16(i16);
                    destination.write_u8(u8);
                    Ok(())
                },
                TestMessage::String(string) => {
                    destination.write_u8(2);
                    destination.write_null_terminated_string(&string);
                    Ok(())
                },
                TestMessage::Bytes(bytes) => {
                    destination.write_u8(3);
                    destination.write_i32(bytes.len() as i32);
                    destination.write_bytes(bytes);
                    Ok(())
                }
            }
        }
    }

    #[test]
    fn byte_slice_handles_ints() {
        let mut buffer = Vec::<u8>::new();

        let mut w = ByteSliceWriter::new(&mut buffer);
        TestCodec::encode(&mut w, TestMessage::Ints(1, 1, 1)).unwrap();
        assert_eq!(buffer.len(), 8);

        let mut r = ByteSliceReader::new(&buffer);
        let result = TestCodec::decode(&mut r).unwrap();

        assert_eq!(result, TestMessage::Ints(1, 1, 1));
        assert_eq!(r.get_read_bytes(), 8);
    }

    #[test]
    fn byte_slice_handles_string() {
        let mut buffer = Vec::<u8>::new();

        let mut w = ByteSliceWriter::new(&mut buffer);
        TestCodec::encode(&mut w, TestMessage::String("Hello, world!".into())).unwrap();
        assert_eq!(buffer.len(), 15);

        let mut r = ByteSliceReader::new(&buffer);
        let result = TestCodec::decode(&mut r).unwrap();

        assert_eq!(result, TestMessage::String("Hello, world!".into()));
        assert_eq!(r.get_read_bytes(), 15);
    }

    #[test]
    fn byte_slice_handles_byte_array() {
        let mut buffer = Vec::<u8>::new();

        let mut w = ByteSliceWriter::new(&mut buffer);
        TestCodec::encode(&mut w, TestMessage::Bytes(&[1, 2, 3, 4])).unwrap();
        assert_eq!(buffer.len(), 9);

        let mut r = ByteSliceReader::new(&buffer);
        let result = TestCodec::decode(&mut r).unwrap();

        assert_eq!(result, TestMessage::Bytes(&[1, 2, 3, 4]));
        assert_eq!(r.get_read_bytes(), 9);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn framed_handles_ints() {
        for buffer_read_limit in 1..50 {
            let mut buffer = Vec::<u8>::new();

            let mut w = Framed::new(Cursor::new(&mut buffer));
            w.write_frame::<TestCodec, _>(TestMessage::Ints(1, 1, 1)).await.unwrap();
            w.write_frame::<TestCodec, _>(TestMessage::Ints(2, 3, 4)).await.unwrap();

            let reader = LimitedReader::new(buffer, buffer_read_limit);
            let mut frame_reader = Framed::new(reader);
            let frame = frame_reader.read_frame::<TestCodec, _>().await.unwrap();
            assert_eq!(frame, TestMessage::Ints(1, 1, 1));

            let frame = frame_reader.read_frame::<TestCodec, _>().await.unwrap();
            assert_eq!(frame, TestMessage::Ints(2, 3, 4));
        }
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn frame_reader_handles_strings() {
        for buffer_read_limit in 1..50 {
            let mut buffer = Vec::<u8>::new();

            let mut w = Framed::new(Cursor::new(&mut buffer));
            w.write_frame::<TestCodec, _>(TestMessage::String("Hello, world!".into())).await.unwrap();
            w.write_frame::<TestCodec, _>(TestMessage::String("Goodbye, world!".into())).await.unwrap();

            let reader = LimitedReader::new(buffer, buffer_read_limit);
            let mut frame_reader = Framed::new(reader);
            let frame = frame_reader.read_frame::<TestCodec, _>().await.unwrap();
            assert_eq!(frame, TestMessage::String("Hello, world!".into()));

            let frame = frame_reader.read_frame::<TestCodec, _>().await.unwrap();
            assert_eq!(frame, TestMessage::String("Goodbye, world!".into()));
        }
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn frame_reader_handles_byte_arrays() {
        for buffer_read_limit in 1..50 {
            let mut buffer = Vec::<u8>::new();

            let mut w = Framed::new(Cursor::new(&mut buffer));
            w.write_frame::<TestCodec, _>(TestMessage::Bytes(&[1, 2, 3, 4])).await.unwrap();
            w.write_frame::<TestCodec, _>(TestMessage::Bytes(&[5, 6, 7, 8, 9, 10])).await.unwrap();

            let reader = LimitedReader::new(buffer, buffer_read_limit);
            let mut frame_reader = Framed::new(reader);
            let frame = frame_reader.read_frame::<TestCodec, _>().await.unwrap();
            assert_eq!(frame, TestMessage::Bytes(&[1, 2, 3, 4]));

            let frame = frame_reader.read_frame::<TestCodec, _>().await.unwrap();
            assert_eq!(frame, TestMessage::Bytes(&[5, 6, 7, 8, 9, 10]));
        }
    }


}