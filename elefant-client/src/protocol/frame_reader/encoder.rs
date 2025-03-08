pub trait Encoder<'a, I: 'a> {
    type Error: From<std::io::Error>;

    fn encode(destination: &mut ByteSliceWriter, input: I) -> Result<(), Self::Error>;
}

pub struct ByteSliceWriter<'a> {
    buffer: &'a mut Vec<u8>,
}

impl<'a> ByteSliceWriter<'a> {
    pub fn new(buffer: &'a mut Vec<u8>) -> Self {
        Self {
            buffer,
        }
    }

    pub fn write_null_terminated_string(&mut self, string: &str) {
        self.buffer.extend_from_slice(string.as_bytes());
        self.buffer.push(0);
    }

    pub fn write_i32(&mut self, value: i32) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_i16(&mut self, value: i16) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u8(&mut self, value: u8) {
        self.buffer.push(value);
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }
}