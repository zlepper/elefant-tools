use crate::protocol::frame_reader::Framed;

pub struct PostgresConnection<C> {
    pub(super) connection: Framed<C>,
}

impl<C> PostgresConnection<C> {
    pub fn new(connection: C) -> Self {
        Self {
            connection: Framed::new(connection),
        }
    }
}

