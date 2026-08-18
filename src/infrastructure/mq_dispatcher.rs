use anyhow::Result;
use lapin::{BasicProperties, Channel, Confirmation, Queue, options::{BasicPublishOptions, QueueDeclareOptions}, types::{FieldTable, ShortString}};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct Dispatcher {
    receiver: Option<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    amqp_channel: Channel,
    exchange_name: String,
    routing_key: String,
    is_consuming: bool,
    woker_handle: Option<JoinHandle<()>>
}

impl Dispatcher {
    pub fn new(
        receiver: Option<tokio::sync::mpsc::Receiver<Vec<u8>>>,
        amqp_channel: Channel,
        exchange_name: String,
        routing_key: String,
    ) -> Self {
        Self {
            receiver,
            amqp_channel,
            exchange_name,
            routing_key,
            is_consuming: false,
            woker_handle: None
        }
    }

    async fn init_queue(&self) -> Result<Queue> {
        Ok(self
            .amqp_channel
            .queue_declare(
                self.routing_key.clone().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?)
    }

    pub async fn consume(&mut self, cancellation_token: CancellationToken) -> Result<()> {
        if self.is_consuming {
            println!("Dispatcher already consuming");
            return Ok(());
        }

        self.init_queue().await?;
        self.is_consuming = true;

        let mut receiver = self.receiver
            .take()
            .expect("Buffer consumer already started or unitialized");

        let channel = self.amqp_channel.clone();
        let exchange = self.exchange_name.clone(); // Exchange (empty string = default exchange)
        let routing_key = self.routing_key.clone();

        let handle = tokio::spawn(async move {
            println!("Dispatcher started");

            let publish_opts = BasicPublishOptions::default();

            loop {
                let props = BasicProperties::default().with_delivery_mode(2);

                let payload = tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        println!("Dispatcher: received cancellation signal.");
                        break;
                    }
                    maybe_payload = receiver.recv() => {
                        match maybe_payload {
                            Some(p) => p,
                            None => {
                                println!("Dispatcher: receiver channel closed");
                                break;
                            }
                        }
                    }
                };

                let confirmation = match channel
                    .basic_publish(
                        ShortString::from(exchange.as_str()),
                        ShortString::from(routing_key.as_str()),
                        publish_opts,
                        &payload,
                        props)
                    .await
                {
                    Ok(t) => t,
                    Err(err ) => {
                        eprintln!("Failed to publish: {err}");
                        continue;
                    }
                };

                match confirmation.await {
                    Ok(Confirmation::Ack(_)) => {
                        println!("✅ Message acknowledged by RabbitMQ!");
                    }
                    Ok(Confirmation::Nack(_)) => {
                        eprintln!("❌ Message NACKed by RabbitMQ.");
                    }
                    Ok(Confirmation::NotRequested) => {
                        println!("✅ Message published (confirmations disabled).");
                    }
                    Err(err) => {
                        eprintln!("‼️ Broker confirmation error: {err}");
                    }
                }
            }

            println!("Dispatcher: receiver closed, exiting worker");
        });

        self.woker_handle = Some(handle);
        Ok(())
    }
}
