use object_store::{memory::InMemory, ObjectStore};
use std::sync::Arc;

pub fn build_in_memory_storage() -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    let store = InMemory::new();
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_in_memory_storage() {
        let result = build_in_memory_storage();
        assert!(result.is_ok(), "Expected Ok, got Err: {:?}", result);
    }
}
