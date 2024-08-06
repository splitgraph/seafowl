use object_store::{
    aws::resolve_bucket_region, aws::AmazonS3Builder, aws::AmazonS3ConfigKey, path::Path,
    ClientConfigKey, ClientOptions, ObjectStore,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;
use url::Url;

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct S3Config {
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub endpoint: Option<String>,
    pub bucket: String,
    pub prefix: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default)]
    pub skip_signature: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            region: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            endpoint: None,
            bucket: "".to_string(),
            prefix: None,
            allow_http: true,
            skip_signature: true,
        }
    }
}

impl S3Config {
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
            prefix: map.get("prefix").map(|s| s.to_string()),
            allow_http: map.get("allow_http").map(|s| s != "false").unwrap_or(true),
            skip_signature: map
                .get("skip_signature")
                .map(|s| s != "false")
                .unwrap_or(true),
        })
    }

    pub fn from_bucket_and_options(
        bucket: String,
        map: &mut HashMap<String, String>,
    ) -> Result<Self, object_store::Error> {
        Ok(Self {
            region: map.remove("format.region"),
            access_key_id: map.remove("format.access_key_id"),
            secret_access_key: map.remove("format.secret_access_key"),
            session_token: map.remove("format.session_token"),
            endpoint: map.remove("format.endpoint"),
            bucket,
            prefix: None,
            allow_http: map
                .remove("format.allow_http")
                .map(|s| s != "false")
                .unwrap_or(true),
            skip_signature: map
                .remove("format.skip_signature")
                .map(|s| s != "false")
                .unwrap_or(true),
        })
    }

    pub fn to_hashmap(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(region) = &self.region {
            map.insert(
                AmazonS3ConfigKey::Region.as_ref().to_string(),
                region.clone(),
            );
        }
        if let Some(access_key_id) = &self.access_key_id {
            map.insert(
                AmazonS3ConfigKey::AccessKeyId.as_ref().to_string(),
                access_key_id.clone(),
            );
        }
        if let Some(secret_access_key) = &self.secret_access_key {
            map.insert(
                AmazonS3ConfigKey::SecretAccessKey.as_ref().to_string(),
                secret_access_key.clone(),
            );
        }
        if let Some(session_token) = &self.session_token {
            map.insert(
                AmazonS3ConfigKey::Token.as_ref().to_string(),
                session_token.clone(),
            );
        }
        if let Some(endpoint) = &self.endpoint {
            map.insert(
                AmazonS3ConfigKey::Endpoint.as_ref().to_string(),
                endpoint.clone(),
            );
        }
        map.insert(
            AmazonS3ConfigKey::Bucket.as_ref().to_string(),
            self.bucket.clone(),
        );
        if let Some(prefix) = &self.prefix {
            map.insert("prefix".to_string(), prefix.clone());
        }
        map.insert(
            AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp)
                .as_ref()
                .to_string(),
            self.allow_http.to_string(),
        );
        map.insert(
            AmazonS3ConfigKey::SkipSignature.as_ref().to_string(),
            self.skip_signature.to_string(),
        );
        map
    }

    pub fn bucket_to_url(&self) -> String {
        format!("s3://{}", &self.bucket)
    }
}

pub fn build_amazon_s3_from_config(
    config: &S3Config,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    assert!(config.allow_http, "allow_http must be true for S3");

    let mut builder = AmazonS3Builder::new()
        .with_region(config.region.clone().unwrap_or_default())
        .with_bucket_name(config.bucket.clone())
        .with_allow_http(config.allow_http);

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
        assert!(
            config.skip_signature,
            "Access key and secret key must be provided if skip_signature is false"
        );
        builder = builder.with_skip_signature(config.skip_signature)
    }

    let store = builder.build()?;
    Ok(Arc::new(store))
}

pub fn map_options_into_amazon_s3_config_keys(
    input_options: HashMap<String, String>,
) -> Result<HashMap<AmazonS3ConfigKey, String>, object_store::Error> {
    let mut mapped_keys = HashMap::new();

    for (key, value) in input_options {
        match AmazonS3ConfigKey::from_str(&key) {
            Ok(config_key) => {
                mapped_keys.insert(config_key, value);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    Ok(mapped_keys)
}

pub async fn add_amazon_s3_specific_options(
    url: &Url,
    options: &mut HashMap<AmazonS3ConfigKey, String>,
) {
    if !options.contains_key(&AmazonS3ConfigKey::Bucket)
        && !options.contains_key(&AmazonS3ConfigKey::Endpoint)
    {
        let region = detect_region(url).await.unwrap();
        options.insert(AmazonS3ConfigKey::Region, region.to_string());
    }
}

pub fn add_amazon_s3_environment_variables(
    options: &mut HashMap<AmazonS3ConfigKey, String>,
) {
    for (os_key, os_value) in std::env::vars_os() {
        if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
            if key.starts_with("AWS_") {
                if let Ok(config_key) = key.to_ascii_lowercase().parse() {
                    options.insert(config_key, value.to_string());
                }
            }
        }
    }

    if env::var(
        AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp)
            .as_ref()
            .to_uppercase(),
    ) == Ok("true".to_string())
    {
        options.insert(
            AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp),
            "true".to_string(),
        );
    }
}

// For "real" S3, if we don't have a region passed to us, we have to figure it out
// ourselves (note this won't work with HTTP paths that are actually S3, but those
// usually include the region already).
async fn detect_region(url: &Url) -> Result<String, object_store::Error> {
    let bucket = url.host_str().ok_or(object_store::Error::Generic {
        store: "parse_url",
        source: format!("Could not find a bucket in S3 path {0}", url).into(),
    })?;

    info!("Autodetecting region for bucket {}", bucket);
    let region = resolve_bucket_region(bucket, &ClientOptions::new()).await?;

    info!("Using autodetected region {} for bucket {}", region, bucket);

    Ok(region)
}

pub fn get_base_url(config: &S3Config) -> Option<Path> {
    config
        .prefix
        .as_ref()
        .map(|prefix| Path::from(prefix.as_ref()))
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
            S3Config::from_hashmap(&map).expect("Failed to create config from hashmap");
        assert_eq!(config.region, Some("us-west-2".to_string()));
        assert_eq!(config.access_key_id, Some("access_key".to_string()));
        assert_eq!(config.secret_access_key, Some("secret_key".to_string()));
        assert_eq!(config.session_token, Some("session_token".to_string()));
        assert_eq!(config.endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(config.bucket, "my-bucket".to_string());
        assert_eq!(config.prefix, Some("my-prefix".to_string()));
    }

    #[test]
    fn test_config_from_hashmap_with_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert("region".to_string(), "us-west-2".to_string());
        map.insert("bucket".to_string(), "my-bucket".to_string());

        let config =
            S3Config::from_hashmap(&map).expect("Failed to create config from hashmap");
        assert_eq!(config.region, Some("us-west-2".to_string()));
        assert!(config.access_key_id.is_none());
        assert!(config.secret_access_key.is_none());
        assert!(config.session_token.is_none());
        assert!(config.endpoint.is_none());
        assert_eq!(config.bucket, "my-bucket".to_string());
        assert!(config.prefix.is_none());
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_config_from_hashmap_without_required_fields() {
        let map = HashMap::new();
        S3Config::from_hashmap(&map).unwrap(); // Missing "region" and "bucket"
    }

    #[test]
    fn test_build_amazon_s3_from_config_with_all_fields() {
        let config = S3Config {
            region: Some("us-west-2".to_string()),
            access_key_id: Some("access_key".to_string()),
            secret_access_key: Some("secret_key".to_string()),
            session_token: Some("session_token".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            bucket: "my-bucket".to_string(),
            prefix: Some("my-prefix".to_string()),
            allow_http: true,
            skip_signature: true,
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
        assert!(debug_output.contains("allow_http: Parsed(true)"));
        assert!(debug_output.contains("skip_signature: false")); //Expected false as access_key_id and secret_access_key are provided
    }

    #[test]
    fn test_build_amazon_s3_from_config_with_missing_optional_fields() {
        let config = S3Config {
            region: Some("us-west-2".to_string()),
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            endpoint: None,
            bucket: "my-bucket".to_string(),
            prefix: None,
            allow_http: true,
            skip_signature: true,
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
        assert!(!debug_output.contains("prefix: \"\""));
        assert!(debug_output.contains("allow_http: Parsed(true)"));
        assert!(debug_output.contains("skip_signature: true"));
    }

    #[test]
    fn test_map_options_into_amazon_s3_config_keys_with_valid_keys() {
        let mut input_options = HashMap::new();
        input_options.insert("access_key_id".to_string(), "ACCESS_KEY".to_string());
        input_options.insert("secret_access_key".to_string(), "SECRET_KEY".to_string());
        input_options.insert("region".to_string(), "us-west-2".to_string());
        input_options.insert("bucket".to_string(), "my-bucket".to_string());

        let result = map_options_into_amazon_s3_config_keys(input_options);
        assert!(result.is_ok());

        let mapped_keys = result.unwrap();
        assert_eq!(
            mapped_keys.get(&AmazonS3ConfigKey::AccessKeyId),
            Some(&"ACCESS_KEY".to_string())
        );
        assert_eq!(
            mapped_keys.get(&AmazonS3ConfigKey::SecretAccessKey),
            Some(&"SECRET_KEY".to_string())
        );
        assert_eq!(
            mapped_keys.get(&AmazonS3ConfigKey::Region),
            Some(&"us-west-2".to_string())
        );
        assert_eq!(
            mapped_keys.get(&AmazonS3ConfigKey::Bucket),
            Some(&"my-bucket".to_string())
        );
    }

    #[test]
    fn test_map_options_into_amazon_s3_config_keys_with_invalid_key() {
        let mut input_options = HashMap::new();
        input_options.insert("invalid_key".to_string(), "some_value".to_string());

        let result = map_options_into_amazon_s3_config_keys(input_options);
        assert!(result.is_err());

        let error = result.err().unwrap();
        assert_eq!(
            error.to_string(),
            "Configuration key: 'invalid_key' is not valid for store 'S3'."
        )
    }

    #[test]
    fn test_map_options_into_amazon_s3_config_keys_with_mixed_keys() {
        let mut input_options = HashMap::new();
        input_options.insert("access_key_id".to_string(), "ACCESS_KEY".to_string());
        input_options.insert("invalid_key".to_string(), "some_value".to_string());
        input_options.insert("bucket".to_string(), "my-bucket".to_string());

        let result = map_options_into_amazon_s3_config_keys(input_options);
        assert!(result.is_err());

        let error = result.err().unwrap();
        assert_eq!(
            error.to_string(),
            "Configuration key: 'invalid_key' is not valid for store 'S3'."
        )
    }

    #[test]
    fn test_map_options_into_amazon_s3_config_keys_empty_input() {
        let input_options = HashMap::new();
        let result = map_options_into_amazon_s3_config_keys(input_options);
        assert!(result.is_ok());

        let mapped_keys = result.unwrap();
        assert!(mapped_keys.is_empty());
    }

    #[test]
    fn test_get_base_url_with_prefix() {
        let s3_config = S3Config {
            region: Some("us-west-1".to_string()),
            access_key_id: Some("ACCESS_KEY".to_string()),
            secret_access_key: Some("SECRET_KEY".to_string()),
            session_token: None,
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            bucket: "my_bucket".to_string(),
            prefix: Some("my_prefix".to_string()),
            allow_http: true,
            skip_signature: true,
        };

        let base_url = get_base_url(&s3_config);
        assert!(base_url.is_some());
        assert_eq!(base_url.unwrap(), Path::from("my_prefix"));
    }

    #[test]
    fn test_get_base_url_without_prefix() {
        let s3_config = S3Config {
            region: Some("us-west-1".to_string()),
            access_key_id: Some("ACCESS_KEY".to_string()),
            secret_access_key: Some("SECRET_KEY".to_string()),
            session_token: None,
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            bucket: "my_bucket".to_string(),
            prefix: None,
            allow_http: true,
            skip_signature: true,
        };

        let base_url = get_base_url(&s3_config);
        assert!(base_url.is_none());
    }

    #[test]
    fn test_get_base_url_with_empty_prefix() {
        let s3_config = S3Config {
            region: Some("us-west-1".to_string()),
            access_key_id: Some("ACCESS_KEY".to_string()),
            secret_access_key: Some("SECRET_KEY".to_string()),
            session_token: None,
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            bucket: "my_bucket".to_string(),
            prefix: Some("".to_string()),
            allow_http: true,
            skip_signature: true,
        };

        let base_url = get_base_url(&s3_config);
        assert!(base_url.is_some());
        assert_eq!(base_url.unwrap(), Path::from(""));
    }

    #[test]
    fn test_to_hashmap() {
        let s3_config = S3Config {
            region: Some("us-west-1".to_string()),
            access_key_id: Some("ACCESS_KEY".to_string()),
            secret_access_key: Some("SECRET_KEY".to_string()),
            session_token: Some("SESSION_TOKEN".to_string()),
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            bucket: "my_bucket".to_string(),
            prefix: Some("my_prefix".to_string()),
            allow_http: true,
            skip_signature: true,
        };

        let hashmap = s3_config.to_hashmap();

        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::Region.as_ref()),
            Some(&"us-west-1".to_string())
        );
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::AccessKeyId.as_ref()),
            Some(&"ACCESS_KEY".to_string())
        );
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::SecretAccessKey.as_ref()),
            Some(&"SECRET_KEY".to_string())
        );
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::Token.as_ref()),
            Some(&"SESSION_TOKEN".to_string())
        );
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::Endpoint.as_ref()),
            Some(&"https://s3.amazonaws.com".to_string())
        );
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::Bucket.as_ref()),
            Some(&"my_bucket".to_string())
        );
        assert_eq!(hashmap.get("prefix"), Some(&"my_prefix".to_string()));
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp).as_ref()),
            Some(&"true".to_string())
        );
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::SkipSignature.as_ref()),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_to_hashmap_with_none_fields() {
        let s3_config = S3Config {
            region: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            endpoint: None,
            bucket: "my_bucket".to_string(),
            prefix: None,
            allow_http: true,
            skip_signature: true,
        };

        let hashmap = s3_config.to_hashmap();

        assert_eq!(hashmap.get(AmazonS3ConfigKey::Region.as_ref()), None);
        assert_eq!(hashmap.get(AmazonS3ConfigKey::AccessKeyId.as_ref()), None);
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::SecretAccessKey.as_ref()),
            None
        );
        assert_eq!(hashmap.get(AmazonS3ConfigKey::Token.as_ref()), None);
        assert_eq!(hashmap.get(AmazonS3ConfigKey::Endpoint.as_ref()), None);
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::Bucket.as_ref()),
            Some(&"my_bucket".to_string())
        );
        assert_eq!(hashmap.get("prefix"), None);
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp).as_ref()),
            Some(&"true".to_string())
        );
        assert_eq!(
            hashmap.get(AmazonS3ConfigKey::SkipSignature.as_ref()),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_bucket_to_url() {
        let config = S3Config {
            region: Some("us-west-1".to_string()),
            access_key_id: Some("ACCESS_KEY".to_string()),
            secret_access_key: Some("SECRET_KEY".to_string()),
            session_token: Some("SESSION_TOKEN".to_string()),
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            bucket: "my_bucket".to_string(),
            prefix: Some("my_prefix".to_string()),
            allow_http: true,
            skip_signature: true,
        };

        let url = config.bucket_to_url();
        assert_eq!(url, "s3://my_bucket");
    }
}
