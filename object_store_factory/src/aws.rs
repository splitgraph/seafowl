use object_store::{
    aws::resolve_bucket_region, aws::AmazonS3Builder, aws::AmazonS3ConfigKey,
    ClientOptions, ObjectStore,
};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tracing::info;
use url::Url;

pub struct Config {
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub endpoint: Option<String>,
    pub bucket: String,
    pub _prefix: Option<String>,
}

impl Config {
    pub fn from_hashmap(
        map: &HashMap<String, String>,
    ) -> Result<Self, object_store::Error> {
        Ok(Self {
            region: map.get("region").map(|s| s.to_string()),
            access_key_id: map.get("access_key_id").map(|s| s.to_string()),
            secret_access_key: map.get("secret_access_key").map(|s| s.to_string()),
            session_token: map.get("session_token").map(|s| s.to_string()),
            endpoint: map.get("endpoint").map(|s| s.to_string()),
            bucket: map.get("bucket").unwrap().clone(),
            _prefix: map.get("prefix").map(|s| s.to_string()),
        })
    }
}

pub fn build_amazon_s3_from_config(
    config: &Config,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    let mut builder = AmazonS3Builder::new()
        .with_region(config.region.clone().unwrap_or_default())
        .with_bucket_name(config.bucket.clone())
        .with_allow_http(true);

    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    }

    if let (Some(access_key_id), Some(secret_access_key)) =
        (&config.access_key_id, &config.secret_access_key)
    {
        builder = builder
            .with_access_key_id(access_key_id.clone())
            .with_secret_access_key(secret_access_key.clone());

        if let Some(token) = &config.session_token {
            builder = builder.with_token(token.clone())
        }
    } else {
        builder = builder.with_skip_signature(true)
    }

    let store = builder.build()?;
    Ok(Arc::new(store))
}

pub async fn add_amazon_s3_specific_options(
    url: &Url,
    mut options: HashMap<String, String>,
) -> HashMap<String, String> {
    if !options.contains_key(AmazonS3ConfigKey::Bucket.as_ref())
        && !options.contains_key(AmazonS3ConfigKey::Endpoint.as_ref())
    {
        let region = detect_region(url).await.unwrap();
        options.insert("region".to_string(), region.to_string());
    }
    options
}

pub fn add_amazon_s3_environment_variables(
    mut options: HashMap<String, String>,
) -> HashMap<String, String> {
    let env_vars = &[
        ("AWS_ACCESS_KEY_ID", "access_key_id"),
        ("AWS_SECRET_ACCESS_KEY", "secret_access_key"),
        ("AWS_DEFAULT_REGION", "region"),
        ("AWS_ENDPOINT", "endpoint"),
        ("AWS_SESSION_TOKEN", "session_token"),
        ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "credentials_uri"),
    ];

    for &(env_var_name, config_key) in env_vars.iter() {
        if let Ok(val) = env::var(env_var_name) {
            options.insert(config_key.to_string(), val);
        }
    }

    if env::var("AWS_ALLOW_HTTP") == Ok("true".to_string()) {
        options.insert("allow_http".to_string(), "true".to_string());
    }

    options
}

// For "real" S3, if we don't have a region passed to us, we have to figure it out
// ourselves (note this won't work with HTTP paths that are actually S3, but those
// usually include the region already).
pub async fn detect_region(url: &Url) -> Result<String, object_store::Error> {
    let bucket = url.host_str().ok_or(object_store::Error::Generic {
        store: "parse_url",
        source: format!("Could not find a bucket in S3 path {0}", url).into(),
    })?;

    info!("Autodetecting region for bucket {}", bucket);
    let region = resolve_bucket_region(bucket, &ClientOptions::new()).await?;

    info!("Using autodetected region {} for bucket {}", region, bucket);

    Ok(region)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_config_from_hashmap_with_all_fields() {
        let mut map = HashMap::new();
        map.insert("region".to_string(), "us-west-2".to_string());
        map.insert("access_key_id".to_string(), "access_key".to_string());
        map.insert("secret_access_key".to_string(), "secret_key".to_string());
        map.insert("session_token".to_string(), "session_token".to_string());
        map.insert("endpoint".to_string(), "http://localhost:9000".to_string());
        map.insert("bucket".to_string(), "my-bucket".to_string());
        map.insert("prefix".to_string(), "my-prefix".to_string());

        let config =
            Config::from_hashmap(&map).expect("Failed to create config from hashmap");
        assert_eq!(config.region, Some("us-west-2".to_string()));
        assert_eq!(config.access_key_id, Some("access_key".to_string()));
        assert_eq!(config.secret_access_key, Some("secret_key".to_string()));
        assert_eq!(config.session_token, Some("session_token".to_string()));
        assert_eq!(config.endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(config.bucket, "my-bucket".to_string());
        assert_eq!(config._prefix, Some("my-prefix".to_string()));
    }

    #[test]
    fn test_config_from_hashmap_with_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert("region".to_string(), "us-west-2".to_string());
        map.insert("bucket".to_string(), "my-bucket".to_string());

        let config =
            Config::from_hashmap(&map).expect("Failed to create config from hashmap");
        assert_eq!(config.region, Some("us-west-2".to_string()));
        assert!(config.access_key_id.is_none());
        assert!(config.secret_access_key.is_none());
        assert!(config.session_token.is_none());
        assert!(config.endpoint.is_none());
        assert_eq!(config.bucket, "my-bucket".to_string());
        assert!(config._prefix.is_none());
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_config_from_hashmap_without_required_fields() {
        let map = HashMap::new();
        Config::from_hashmap(&map).unwrap(); // Missing "region" and "bucket"
    }

    #[test]
    fn test_build_amazon_s3_from_config_with_all_fields() {
        let config = Config {
            region: Some("us-west-2".to_string()),
            access_key_id: Some("access_key".to_string()),
            secret_access_key: Some("secret_key".to_string()),
            session_token: Some("session_token".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            bucket: "my-bucket".to_string(),
            _prefix: Some("my-prefix".to_string()),
        };

        let result = build_amazon_s3_from_config(&config);

        assert!(result.is_ok(), "Expected Ok, got Err: {:?}", result);

        let store = result.unwrap();
        let debug_output = format!("{:?}", store);

        assert!(debug_output.contains("region: \"us-west-2\""));
        assert!(debug_output.contains("bucket: \"my-bucket\""));
        assert!(debug_output.contains("endpoint: Some(\"http://localhost:9000\")"));
        assert!(debug_output.contains("key_id: \"access_key\""));
        assert!(debug_output.contains("secret_key: \"secret_key\""));
        assert!(debug_output.contains("token: Some(\"session_token\")"));
    }

    #[test]
    fn test_build_amazon_s3_from_config_with_missing_optional_fields() {
        let config = Config {
            region: Some("us-west-2".to_string()),
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            endpoint: None,
            bucket: "my-bucket".to_string(),
            _prefix: None,
        };

        let result = build_amazon_s3_from_config(&config);

        assert!(result.is_ok(), "Expected Ok, got Err: {:?}", result);

        let store = result.unwrap();
        let debug_output = format!("{:?}", store);

        assert!(debug_output.contains("region: \"us-west-2\""));
        assert!(debug_output.contains("bucket: \"my-bucket\""));
        assert!(debug_output.contains("endpoint: None"));
        assert!(!debug_output.contains("key_id: \"\""));
        assert!(!debug_output.contains("secret_key: \"\""));
        assert!(!debug_output.contains("token: \"\""));
    }
}
