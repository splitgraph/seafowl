use object_store::{memory::InMemory, ObjectStore};

pub fn build_in_memory_storage() -> Result<Box<dyn ObjectStore>, object_store::Error> {
    let store = InMemory::new();
    Ok(Box::new(store))
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
