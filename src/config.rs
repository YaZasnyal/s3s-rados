use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub api: Api,
    pub db: DatabaseConfig,
    pub storage: Storage,
    pub auth: Auth,
}

#[derive(Debug, Deserialize)]
pub struct Api {
    pub host: String,
    pub port: u16,
    pub domain: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub db_name: String,
    pub user: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct Storage {
    pub host: String,
    pub port: u16,
    pub access_key: String,
    pub secret_key: String,
    pub insecure: bool,
    pub bucket: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Auth {
    pub access_key: String,
    pub secret_key: String,
}

impl Settings {
    pub fn new(config_path: &str) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let s = Config::builder()
            .add_source(File::with_name(config_path))
            .add_source(Environment::with_prefix("S3PROXY"))
            .build()?;

        Ok(s.try_deserialize()?)
    }
}
