pub struct PostgresConnection<C> {
    pub(super) connection: C,
    
    /// A buffer that can be reused when reading messages to avoid having to constantly resize 
    /// or allocate new memory.
    pub(super) read_buffer: Vec<u8>,
}

impl<C> PostgresConnection<C> {
    pub fn new(connection: C) -> Self {
        Self {
            connection,
            read_buffer: Vec::new(),
        }
    }
}

