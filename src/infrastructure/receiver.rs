use std::{net::UdpSocket, sync::Arc, time::Duration};
use anyhow::{Context, Result};
use tokio::task::JoinHandle;

use crate::utils::settings::Settings;

pub struct Receiver {
    udp_socket: Arc<UdpSocket>,
    buffer_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    is_consuming: bool,
    worker_handle: Option<JoinHandle<()>>
}

impl Receiver {
    pub fn new(
        config: &Settings,
        buffer_sender: tokio::sync::mpsc::Sender<Vec<u8>>) -> Result<Self> {
        let socket = UdpSocket::bind(&config.udp_conf.receiver_address)
            .context("Could no load receiver address")?;
        socket.set_read_timeout(Some(Duration::from_secs(config.udp_conf.socket_read_timeout_seconds)))
            .context("Failed to apply timeout to socket")?;
        let udp_socket = Arc::new(socket);

        Ok(Self {
            udp_socket,
            buffer_sender,
            is_consuming: false,
            worker_handle: None
        })
    }

    pub fn consume(&mut self) {
        if self.is_consuming {
            println!("Receiver already consuming");
            return;
        }

        let socket = Arc::clone(&self.udp_socket);
        let sender = self.buffer_sender.clone();
        let handle = tokio::spawn(async move {
            println!("Polling data..");

            let mut udp_packet_buffer = vec![0u8; 22];
            loop {
                match socket.recv_from(&mut udp_packet_buffer) {
                    Ok((amount, src_address)) => {
                        println!("Received {} bytes from: {}", amount, src_address);

                        let packet_data = &udp_packet_buffer[..amount].to_vec();
                        match sender.send(packet_data.clone()).await {
                            Ok(_) => {
                                println!("Data written in buffer successfully");
                            },
                            Err(err) => {
                                eprintln!("Buffer write error: {}", err);
                                continue;
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!("Error receiving packets: {}", err);
                    }
                }
            }
        });

        self.is_consuming = true;
        self.worker_handle = Some(handle);
        println!("Receiver consume(): done");
    }
}