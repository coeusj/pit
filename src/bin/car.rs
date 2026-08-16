use std::{net::UdpSocket, thread, time::Duration};
use rand::RngExt;
use anyhow::Result;

use pit::{domain::payload::Payload, utils::{now_seconds, settings::Settings}};

fn main() -> Result<()> {
    let conf = Settings::new().expect("Could not load settings");

    let udp_socket = UdpSocket::bind(conf.udp_conf.sender_address)?;
    udp_socket.connect(conf.udp_conf.receiver_address)?;

    let mut rng = rand::rng();
    let mut packet_sequence: u64 = 0;
    loop {
        let temp: f32 = rng.random_range(32.0..37.0);
        let payload = Payload {
            sequence_num: packet_sequence,
            id: 123,
            timestamp: now_seconds(),
            value: temp
        }.to_newtwork_bytes();
        udp_socket.send(&payload)?;
        packet_sequence += 1;

        println!("Sent telemetry data: {:?} - size: {}", payload, payload.len());
        thread::sleep(Duration::from_millis(1000));
    }
}