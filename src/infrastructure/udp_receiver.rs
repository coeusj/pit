use std::{net::SocketAddr, sync::Arc, time::Duration};
use anyhow::{Context, Result};
use tokio::{net::UdpSocket, task::JoinHandle, time};
use tokio_util::sync::CancellationToken;

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

    pub fn consume(
        &mut self,
        udp_socket_read_timeout: Option<u64>,
        cancellation_token: CancellationToken) {
        if self.is_consuming {
            println!("Receiver already consuming");
            return;
        }

        let socket = Arc::clone(&self.udp_socket);
        let sender = self.buffer_sender.clone();
        let handle = tokio::spawn(async move {
            println!("Receiver started");

            let mut udp_packet_buffer = vec![0u8; 22];

            loop {
                let recv_res = tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        println!("Receiver task received cancellation signal. Exiting worker.");
                        break;
                    }
                    res = recv_packet_with_timeout(&socket, &mut udp_packet_buffer, udp_socket_read_timeout) => res
                };

                let (amount, src_address) = match recv_res {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        println!("UDP socket reader timed out");
                        continue;
                    },
                    Err(err) => {
                        eprintln!("Error receiving packet: {err}");
                        continue;
                    }
                };

                println!("Received {} bytes from: {}", amount, src_address);
                let packet_data = udp_packet_buffer[..amount].to_vec();

                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        println!("Cancellation received while writing to buffer. Exiting worker.");
                        break;
                    }
                    send_res = sender.send(packet_data) => {
                        if let Err(err) = send_res {
                            eprintln!("Buffer channel receiver dropped ({err}). Terminating task.");
                            break;
                        } else {
                            println!("Data written in buffer successfully");
                        }
                    }
                }
            }
        });

        self.is_consuming = true;
        self.worker_handle = Some(handle);
    }
}

async fn recv_packet_with_timeout(
    socket: &UdpSocket,
    buffer: &mut [u8],
    timeout_secs: Option<u64>
) -> Result<Option<(usize, SocketAddr)>, std::io::Error> {
    if let Some(secs) = timeout_secs {
        match time::timeout(Duration::from_secs(secs), socket.recv_from(buffer)).await {
            Ok(recv_res) => recv_res.map(Some),
            Err(_) => Ok(None) // Timeout elapsed
        }
    } else {
        socket.recv_from(buffer).await.map(Some)
    }
}