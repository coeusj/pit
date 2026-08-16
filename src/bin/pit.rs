use anyhow::Result;
use lapin::{Connection, ConnectionProperties};

use pit::{infrastructure::{mq_dispatcher::Dispatcher, udp_receiver::Receiver}, utils::settings::Settings};

#[tokio::main]
async fn main() -> Result<()> {
    let config: Settings = Settings::new().expect("Failed to load settings");

    let (buff_sender, buff_receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(config.udp_conf.buffer_size);

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

    let mut receiver = Receiver::new(
        &config.udp_conf.receiver_address,
        buff_sender).await?;
    receiver.consume(Some(config.udp_conf.socket_read_timeout_seconds.clone()));

    tokio::signal::ctrl_c().await.expect("failed to wait for ctrl-c");
    Ok(())
}