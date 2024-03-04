use std::sync::atomic::AtomicUsize;

/// A funky type that is always equal all other object ids
/// This allows for tracking where an object came from when cloned
/// even if the object has changed.
#[derive(Eq, Clone, Debug)]
pub struct ObjectId {
    value: usize,
}

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);


impl ObjectId {
    pub(crate) fn new(value: usize) -> Self {
        ObjectId { value }
    }
    
    pub fn next() -> Self {
        Self::new(NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
    
    /// Checks if the value of the object id is equal to the value of another object id
    pub fn actual_eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl PartialEq for ObjectId {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::next()
    }
}