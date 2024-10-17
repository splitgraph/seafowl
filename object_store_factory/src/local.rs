use object_store::{local::LocalFileSystem, ObjectStore};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

pub const DATA_DIR: &str = "data_dir";
pub const CREATE_DIR: &str = "create_dir";

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct LocalConfig {
    pub data_dir: String,
    #[serde(default = "default_false")]
    pub disable_hardlinks: bool,
    #[serde(default = "default_false")]
    pub create_dir: bool,
}

fn default_false() -> bool {
    false
}

impl LocalConfig {
    pub fn from_hashmap(
        map: &HashMap<String, String>,
    ) -> Result<Self, object_store::Error> {
        Ok(Self {
            data_dir: map
                .get(DATA_DIR)
                .ok_or_else(|| object_store::Error::Generic {
                    store: "local",
                    source: "Missing data_dir".into(),
                })?
                .clone(),
            disable_hardlinks: map
                .get("disable_hardlinks")
                .map(|s| s == "true")
                .unwrap_or(false),
            create_dir: map.get(CREATE_DIR).map(|s| s == "true").unwrap_or(false),
        })
    }

    pub fn to_hashmap(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("data_dir".to_string(), self.data_dir.clone());
        map.insert(
            "disable_hardlinks".to_string(),
            self.disable_hardlinks.to_string(),
        );
        map
    }

    pub fn build_local_storage(
        &self,
    ) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
        if self.create_dir {
            // `LocalFileSystem::new_with_prefix` will error out unless the path fully exists
            fs::create_dir_all(self.data_dir.clone()).map_err(|e| {
                object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(e),
                }
            })?;
        }

        let store = LocalFileSystem::new_with_prefix(self.data_dir.clone())?
            .with_automatic_cleanup(true);
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
        assert!(!config.disable_hardlinks); // Default value
    }

    #[test]
    fn test_config_from_hashmap_with_disable_hardlinks() {
        let mut map = HashMap::new();
        map.insert("data_dir".to_string(), "/tmp/data".to_string());
        map.insert("disable_hardlinks".to_string(), "true".to_string());

        let config = LocalConfig::from_hashmap(&map)
            .expect("Failed to create config from hashmap");
        assert_eq!(config.data_dir, "/tmp/data".to_string());
        assert!(config.disable_hardlinks);
    }

    #[test]
    fn test_config_from_hashmap_with_disable_hardlinks_false() {
        let mut map = HashMap::new();
        map.insert("data_dir".to_string(), "/tmp/data".to_string());
        map.insert("disable_hardlinks".to_string(), "false".to_string());

        let config = LocalConfig::from_hashmap(&map)
            .expect("Failed to create config from hashmap");
        assert_eq!(config.data_dir, "/tmp/data".to_string());
        assert!(!config.disable_hardlinks);
    }

    #[test]
    #[should_panic(expected = "called `Result::unwrap()` on an `Err` value")]
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
            disable_hardlinks: false,
            create_dir: false,
        }
        .build_local_storage();
        assert!(result.is_ok(), "Expected Ok, got Err: {:?}", result);
    }

    #[test]
    fn test_build_local_storage_with_invalid_path() {
        let result = LocalConfig {
            data_dir: "".to_string(),
            disable_hardlinks: false,
            create_dir: false,
        }
        .build_local_storage();
        assert!(result.is_err(), "Expected Err due to invalid path, got Ok");
    }

    #[test]
    fn test_to_hashmap() {
        let local_config = LocalConfig {
            data_dir: "path/to/data".to_string(),
            disable_hardlinks: true,
            create_dir: false,
        };

        let hashmap = local_config.to_hashmap();

        assert_eq!(hashmap.get("data_dir"), Some(&"path/to/data".to_string()));
        assert_eq!(hashmap.get("disable_hardlinks"), Some(&"true".to_string()));
    }

    #[test]
    fn test_default_false() {
        assert!(!default_false());
    }

    #[test]
    fn test_deserialize_with_default() {
        let json = r#"
        {
            "data_dir": "/tmp/data"
        }
        "#;

        let config: LocalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.data_dir, "/tmp/data");
        assert!(!config.disable_hardlinks);
    }

    #[test]
    fn test_deserialize_with_disable_hardlinks() {
        let json = r#"
        {
            "data_dir": "/tmp/data",
            "disable_hardlinks": true
        }
        "#;

        let config: LocalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.data_dir, "/tmp/data");
        assert!(config.disable_hardlinks);
    }
}
