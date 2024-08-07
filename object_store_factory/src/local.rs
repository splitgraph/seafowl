use object_store::{local::LocalFileSystem, ObjectStore};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct LocalConfig {
    pub data_dir: String,
}

impl LocalConfig {
    pub fn from_hashmap(
        map: &HashMap<String, String>,
    ) -> Result<Self, object_store::Error> {
        Ok(Self {
            data_dir: map.get("data_dir").unwrap().clone(),
        })
    }

    pub fn to_hashmap(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("data_dir".to_string(), self.data_dir.clone());
        map
    }

    pub fn build_local_storage(
        &self,
    ) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
        let store = LocalFileSystem::new_with_prefix(self.data_dir.clone())?;
        Ok(Arc::new(store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_config_from_hashmap_with_data_dir() {
        let mut map = HashMap::new();
        map.insert("data_dir".to_string(), "/tmp/data".to_string());

        let config = LocalConfig::from_hashmap(&map)
            .expect("Failed to create config from hashmap");
        assert_eq!(config.data_dir, "/tmp/data".to_string());
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_config_from_hashmap_without_data_dir() {
        let map = HashMap::new();
        // This test will panic because data_dir is missing, which causes unwrap() to panic.
        LocalConfig::from_hashmap(&map).unwrap();
    }

    #[test]
    fn test_build_local_storage_with_valid_config() {
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let data_dir = temp_dir
            .path()
            .to_str()
            .expect("Failed to convert path to string");

        let result = LocalConfig {
            data_dir: data_dir.to_string(),
        }
        .build_local_storage();
        assert!(result.is_ok(), "Expected Ok, got Err: {:?}", result);
    }

    #[test]
    fn test_build_local_storage_with_invalid_path() {
        let result = LocalConfig {
            data_dir: "".to_string(),
        }
        .build_local_storage();
        assert!(result.is_err(), "Expected Err due to invalid path, got Ok");
    }

    #[test]
    fn test_to_hashmap() {
        let local_config = LocalConfig {
            data_dir: "path/to/data".to_string(),
        };

        let hashmap = local_config.to_hashmap();

        assert_eq!(hashmap.get("data_dir"), Some(&"path/to/data".to_string()));
    }
}
