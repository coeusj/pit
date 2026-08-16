use std::{net::SocketAddr, sync::Arc, time::Duration};
use anyhow::{Context, Result};
use tokio::{net::UdpSocket, task::JoinHandle, time};

pub struct Receiver {
    udp_socket: Arc<UdpSocket>,
    buffer_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    is_consuming: bool,
    worker_handle: Option<JoinHandle<()>>
}

impl Receiver {
    pub async fn new(
        udp_socket_address: &String,
        buffer_sender: tokio::sync::mpsc::Sender<Vec<u8>>) -> Result<Self> {
        let socket = UdpSocket::bind(udp_socket_address)
            .await
            .context("Could not bind receiver address")?;

        Ok(Self {
            udp_socket: Arc::new(socket),
            buffer_sender,
            is_consuming: false,
            worker_handle: None
        })
    }

    pub fn consume(&mut self, udp_socket_read_timeout: Option<u64>) {
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
                let recv_future = socket.recv_from(&mut udp_packet_buffer);
                let recv_result: Result<(usize, SocketAddr)> = if let Some(secs) = udp_socket_read_timeout {
                    match time::timeout(Duration::from_secs(secs), recv_future).await {
                        Ok(inner) => inner.map_err(|e| e.into()),
                        Err(_) => {
                            // timeout elapsed
                            println!("UDP socket reader timed out after {}s", secs);
                            continue;
                        }
                    }
                } else {
                    // no timeout requested
                    match recv_future.await {
                        Ok((amt, addr)) => Ok((amt, addr)),
                        Err(e) => Err(e.into()),
                    }
                };

                match recv_result {
                    Ok((amount, src_address)) => {
                        println!("Received {} bytes from: {}", amount, src_address);

                        let packet_data = udp_packet_buffer[..amount].to_vec();
                        if let Err(err) = sender.send(packet_data).await {
                            eprintln!("Buffer write error: {}", err);
                            continue;
                        } else {
                            println!("Data written in buffer successfully");
                        }
                    }
                    Err(err) => {
                        eprintln!("Error receiving packets: {}", err);
                        continue;
                    }
                }
            }
        });

        self.is_consuming = true;
        self.worker_handle = Some(handle);
    }
}