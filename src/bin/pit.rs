use anyhow::Result;
use lapin::{Connection, ConnectionProperties};

use pit::{infrastructure::{dispatcher::Dispatcher, receiver::Receiver}, utils::settings::Settings};

#[tokio::main]
async fn main() -> Result<()> {
    const BUFFER_SIZE: usize = 40;

    let config: Settings = Settings::new().expect("Failed to load settings");

    let (buff_sender, buff_receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(BUFFER_SIZE);

    let rabbit_conn = Connection::connect(
        &config.mq_conf.address,
        ConnectionProperties::default().enable_auto_recover()).await?;
    let rabbit_channel = rabbit_conn.create_channel().await?;
    let exchange_name = String::from("");
    let routing_key = String::from("pit");

    let mut dispatcher = Dispatcher::new(
        Some(buff_receiver),
        rabbit_channel,
        exchange_name,
        routing_key);
    dispatcher.consume().await?;

    let mut receiver = Receiver::new(&config, buff_sender)?;
    receiver.consume();

    Ok(())
}