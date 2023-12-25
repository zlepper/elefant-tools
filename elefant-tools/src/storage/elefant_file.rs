use async_trait::async_trait;
use crate::models::PostgresDatabase;
use crate::storage::CopyDestination;

pub struct ElefantFileStorage {
    pub file_path: String,
}

impl ElefantFileStorage {
    pub fn new(file_path: &str) -> Self {
        ElefantFileStorage {
            file_path: file_path.to_string(),
        }
    }
}

// #[async_trait]
// impl CopyDestination for ElefantFileStorage {
//     async fn apply_structure(&mut self, db: &PostgresDatabase) -> crate::Result<()> {
//         todo!()
//     }
// }