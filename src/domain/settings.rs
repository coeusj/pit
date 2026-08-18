use serde::Deserialize;
use config::{Config, File};
use anyhow::Result;

#[derive(Deserialize)]
pub struct UdpConfiguration {
    pub receiver_address: String,
    pub sender_address: String,
    pub socket_read_timeout_seconds: u64,
    pub buffer_size: usize
}

#[derive(Deserialize)]
pub struct MQConfiguration {
    pub address: String,
    pub user: String,
    pub password: String
}

#[derive(Deserialize)]
pub struct SensorConfiguration {
    pub id: u16,
    pub frequency_hz: u64
}

#[derive(Deserialize)]
pub struct Settings {
    pub udp_conf: UdpConfiguration,
    pub mq_conf: MQConfiguration,
    pub sensors: Vec<SensorConfiguration>
}

impl Settings {
    pub fn load() -> Result<Self> {
        let config = Config::builder()
            .add_source(File::with_name("Settings"))
            .build()?;

        Ok(config.try_deserialize()?)
    }
}

