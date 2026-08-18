use std::{sync::Arc, time::Duration};
use rand::RngExt;
use anyhow::Result;
use tokio::{time::sleep, net::UdpSocket};
use tokio_util::sync::CancellationToken;

use pit::{domain::{payload::Payload, settings::Settings}, utils::now_seconds};

#[tokio::main]
async fn main() -> Result<()> {
    let conf = Settings::load().expect("Could not load settings");

    let udp_socket = Arc::new(UdpSocket::bind(conf.udp_conf.sender_address).await?);
    udp_socket.connect(conf.udp_conf.receiver_address).await?;

    let cancellation_token = CancellationToken::new();

    for sensor in conf.sensors {
        let socket = Arc::clone(&udp_socket);
        let ct = cancellation_token.clone();

        tokio::spawn(async move {
            let mut packet_sequence: u64 = 0;

            loop {
                let temp: f32 = rand::rng().random_range(32.0..37.0);
                let payload = Payload {
                    sequence_num: packet_sequence,
                    id: sensor.id,
                    timestamp: now_seconds(),
                    value: temp
                }.to_newtwork_bytes();

                match socket.send(&payload).await {
                    Ok(size) => { println!("Sent sensor '{}' data: {:?} - size: {}", sensor.id, payload, size); },
                    Err(_) => { println!("Could not send telemetry")}
                }

                packet_sequence += 1;

                tokio::select! {
                    _ = ct.cancelled() => {
                        println!("Sensor {} received cancellation signal.", sensor.id);
                        break;
                    }
                    _ = sleep(Duration::from_millis(1000 / sensor.frequency_hz)) => {}
                }
            }
        });
    }

    tokio::signal::ctrl_c().await.expect("failed to wait for ctrl-c");
    cancellation_token.cancel();
    println!("Shutting down car simulator");

    Ok(())
}