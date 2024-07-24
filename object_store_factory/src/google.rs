use object_store::{gcp::GoogleCloudStorageBuilder, ObjectStore};
use std::collections::HashMap;
use std::env;

pub struct Config {
    pub bucket: String,
    pub _prefix: Option<String>,
    pub google_application_credentials: Option<String>,
}

impl Config {
    pub fn from_hashmap(
        map: &HashMap<String, String>,
    ) -> Result<Self, object_store::Error> {
        Ok(Self {
            bucket: map.get("bucket").unwrap().clone(),
            _prefix: map.get("prefix").map(|s| s.to_string()),
            google_application_credentials: map
                .get("google_application_credentials")
                .map(|s| s.to_string()),
        })
    }
}

pub fn build_google_cloud_storage_from_config(
    config: &Config,
) -> Result<Box<dyn ObjectStore>, object_store::Error> {
    let mut builder: GoogleCloudStorageBuilder =
        GoogleCloudStorageBuilder::new().with_bucket_name(&config.bucket.clone());

    builder = if let Some(path) = &config.google_application_credentials {
        builder.with_service_account_path(path.clone())
    } else {
        builder
    };

    let store = builder.build()?;
    Ok(Box::new(store))
}

pub fn add_google_cloud_storage_environment_variables(
    mut options: HashMap<String, String>,
) -> HashMap<String, String> {
    let env_vars = &[
        ("GOOGLE_SERVICE_ACCOUNT", "service_account_file"),
        ("GOOGLE_SERVICE_ACCOUNT_PATH", "service_account_path"),
        ("SERVICE_ACCOUNT", "service_account_file"),
        ("GOOGLE_SERVICE_ACCOUNT_KEY", "service_account_key"),
        ("GOOGLE_BUCKET", "bucket"),
        ("GOOGLE_BUCKET_NAME", "bucket"),
    ];

    for &(env_var_name, config_key) in env_vars.iter() {
        if let Ok(val) = env::var(env_var_name) {
            options.insert(config_key.to_string(), val);
        }
    }

    options
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_from_hashmap_with_all_fields() {
        let mut map = HashMap::new();
        map.insert("bucket".to_string(), "my-bucket".to_string());
        map.insert("prefix".to_string(), "my-prefix".to_string());
        map.insert(
            "google_application_credentials".to_string(),
            "/path/to/credentials.json".to_string(),
        );

        let config =
            Config::from_hashmap(&map).expect("Failed to create config from hashmap");
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config._prefix, Some("my-prefix".to_string()));
        assert_eq!(
            config.google_application_credentials,
            Some("/path/to/credentials.json".to_string())
        );
    }

    #[test]
    fn test_config_from_hashmap_with_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert("bucket".to_string(), "my-bucket".to_string());

        let config =
            Config::from_hashmap(&map).expect("Failed to create config from hashmap");
        assert_eq!(config.bucket, "my-bucket");
        assert!(config._prefix.is_none());
        assert!(config.google_application_credentials.is_none());
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_config_from_hashmap_without_bucket() {
        let map = HashMap::new();
        Config::from_hashmap(&map).unwrap();
    }

    #[test]
    fn test_build_google_cloud_storage_from_config_with_all_fields() {
        // Create a temporary file for the credentials
        let temp_file = NamedTempFile::new().expect("Failed to create temporary file");

        // Write some mock credentials to the file
        let credentials_content = r#"{
            "type": "service_account",
            "project_id": "gcloud-123456",
            "private_key_id": "123456",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCxjzFuu7kO+sfY\nXPq0EZo1Oth9YjCyrhIQr6XavJQyD/OT9gcd9Q5+/VvLwCXBijEgVdXFQf5Tcsh2\ndpp/hOjGuc7Lh9Kk+DtebUDZ9AIF92LvRX2yKJJ4a6zqV9iEqCfxAhSrwsYMLnp0\nGbxG0ACUR/VdLv8U2ctNDG4DL8jk6yYowABbsL/074GOFWtwW99w1BJb09+l0f2l\njIom15iY897W1gjOBskM7fsHm3WwlCwD/+4PPodp8PRIjvefnMwx7E0Lu6IcJ8Kg\n4Rhm1Rk5hJWKWEgQHmZ4ik4kc/FKdHRMGERkMY5VVYoZ6bUx7OdhF7Vt3HVZDA88\nsx9fbTBxAgMBAAECggEAAWSAHMA4KVfqLVY9WSAyN2yougMFIsGevqbCBD8qYmIh\npO1vDNsZLAHMsIJnSWdOD1TdAlkMJ5dk3xj7CTj/ol9esdX03vpbbNgqhAsX4PgZ\nvIqs+7K5w1wE1SmvNwsilQ9RHi++4eWTbEmvYlbLSl5uHDb8JSu4HniUfE3po3H5\nWDj01OMSe9dhaXrzhqOn2qo37XJ9xF1VCSkY3JRj3cY7W7crVE3UmDyYT+ZE1Tei\nyYhrZh1QDFeQVCFiHEP3RA1T/MYaFn1ylkwGcvgFvoB81vOJaVEXh1Xldwx/6KZC\nyrXBlnVqa//IuCtEE4zTl146G99kRdQFrAdqTadlSQKBgQDauQefH+zCpxTaO03E\nlzGoXr9mxo6Rzhim60e+uDgkCnDhElc3rqiuxFH6QNORa2/A/zvc7iHYZsu8QAvB\n776S9rrpxHoc1271fLqzMBR6gDkTzh/MjUJnsPNjnfehE2h6U8Zoeq755Xv9S85I\nuk9bIJzs5JH6xBEDxnIb/ier5wKBgQDP0i9jTb5TgrcqYYpjURsHGQRv+6lOaZrC\nD94vNDmhTLg3kW5b2BD0ZeZwGCwiSOSqL/5fjlRie94pPnIn6pm5uGgndgdRLQvw\nIdpRyvAUAOY7SnoLhZjVue4syzwV3k7+d4x7LrzpZclBH8uc3sLU3vOSsmFRIkf+\nfK9qcVv15wKBgQDL2fHRi/algQW9U9JqbKQakZwAVQThvd1aDSVECvxAEv8btnVV\nb1LF+DGTdUH6YdC5ZujLQ6KFx2ERZfvPV/wdixmv8LADG4LOB98WTLR5a/JGlDEs\n+2ctr01YxgzasnUItfXQwK8+N3U1Iab0P7jgbOf1Hh80QfK9uwH1Nw6QdwKBgCuP\nigFNpWxJxOzsPx6sPHcTZlu2q3lVJ2wv+Ul5r+7AbwiuwiwcMQmZZmDuoCmbj9qg\nbrhG1CdEgX+xqCn3wbstDR/gXI5GW+88mU91szbuLVQWO1i46x05eNQI0ZJf47zx\nABA97rkZbcLp0DsUclA+X13LaByii+aq6fXsxvLXAoGBALzkBzJ/SOvotz/UnBxl\nGU9QWmptZttaqtLKizPNQZpY1KO9VxeyoGbkTnN0M58ktpIp8LGlSJejk/tkRKBG\nUFRW/v49GW3eCgl4D+MOTFLCJDT68D2lp4F9hdBHsoH17ZdHy8rennmJN3QExIjx\n0xoq6OYjjzNwhFqkPl0H6HrM\n-----END PRIVATE KEY-----\n",
            "client_email": "mail@gcloud-123456.iam.gserviceaccount.com",
            "client_id": "102784232161964177687",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gcloud-acc%40gcloud-123456.iam.gserviceaccount.com"
            }"#;

        fs::write(temp_file.path(), credentials_content)
            .expect("Failed to write to temporary file");

        let config = Config {
            bucket: "my-bucket".to_string(),
            _prefix: Some("my-prefix".to_string()),
            google_application_credentials: Some(
                temp_file.path().to_str().unwrap().to_string(),
            ),
        };

        let result = build_google_cloud_storage_from_config(&config);

        assert!(result.is_ok(), "Expected Ok, got Err: {:?}", result);

        let store = result.unwrap();
        let debug_output = format!("{:?}", store);

        println!("{:?}", debug_output);

        // Check the configuration values
        assert!(debug_output.contains("bucket_name: \"my-bucket\""));
    }

    #[test]
    fn test_build_google_cloud_storage_from_config_with_missing_optional_fields() {
        let config = Config {
            bucket: "my-bucket".to_string(),
            _prefix: None,
            google_application_credentials: None,
        };

        let result = build_google_cloud_storage_from_config(&config);

        assert!(result.is_ok(), "Expected Ok, got Err: {:?}", result);

        let store = result.unwrap();
        let debug_output = format!("{:?}", store);

        // Check the configuration values
        assert!(debug_output.contains("bucket_name: \"my-bucket\""));
    }
}
