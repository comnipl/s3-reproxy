use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    targets: Vec<S3Target>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct S3Credential {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

const fn default_priority() -> u32 {
    1
}

const fn default_read_request() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct S3Target {
    /// The name of the target
    name: String,

    /// Read priority of this target.
    /// Read Requests to s3-reproxy are issued in order of priority.
    #[serde(default = "default_priority")]
    priority: u32,

    /// Whether this target is allowed to read?
    /// For requests to search for or retrieve a file, if all targets with read_request true respond "does not exist", s3-reproxy will not search for the file any further and will respond "does not exist".
    /// However, if all targets with read_request true are down, the one with read_request false and highest priority will be used for reading.
    #[serde(default = "default_read_request")]
    read_request: bool,

    s3: S3Credential,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn parse_target_with_default() {
        let yaml = r#"
            name: cloudflare-r2
            s3:
              endpoint: http://localhost:8080
              access_key: abcabc
              secret_key: defdef
        "#;

        let target: S3Target = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            target,
            S3Target {
                name: "cloudflare-r2".to_string(),
                priority: 1,
                read_request: true,
                s3: S3Credential {
                    endpoint: "http://localhost:8080".to_string(),
                    access_key: "abcabc".to_string(),
                    secret_key: "defdef".to_string(),
                },
            }
        );
    }

    #[test]
    fn parse_config() {
        let yaml = r#"
            targets:
            - name: cloudflare-r2
              priority: 3
              read_request: false
              s3:
                endpoint: http://localhost:8080
                access_key: abcabc
                secret_key: defdef
            - name: local-minio
              priority: 5
              read_request: true
              s3:
                endpoint: http://localhost:8080
                access_key: abcabc
                secret_key: defdef
        "#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            config.targets,
            vec![
                S3Target {
                    name: "cloudflare-r2".to_string(),
                    priority: 3,
                    read_request: false,
                    s3: S3Credential {
                        endpoint: "http://localhost:8080".to_string(),
                        access_key: "abcabc".to_string(),
                        secret_key: "defdef".to_string(),
                    },
                },
                S3Target {
                    name: "local-minio".to_string(),
                    priority: 5,
                    read_request: true,
                    s3: S3Credential {
                        endpoint: "http://localhost:8080".to_string(),
                        access_key: "abcabc".to_string(),
                        secret_key: "defdef".to_string(),
                    },
                },
            ]
        );
    }
}
