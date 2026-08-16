use crossbeam_channel::{Receiver, Sender, bounded};
use anyhow::Result;

pub struct RingBuffer<const BUFFER_SIZE: usize, const MAX_PACKET_SIZE: usize> {
    sender: Sender<[u8; MAX_PACKET_SIZE]>,
    receiver: Receiver<[u8; MAX_PACKET_SIZE]>,
    sequence_index: u64
}

impl<const BUFFER_SIZE: usize, const MAX_PACKET_SIZE: usize> RingBuffer<BUFFER_SIZE, MAX_PACKET_SIZE> {
    pub fn new() -> Self {
        let (sender, receiver) = bounded::<[u8; MAX_PACKET_SIZE]>(BUFFER_SIZE);

        Self {
            sender,
            receiver,
            sequence_index: 0
        }
    }

    pub fn read(&self) -> Result<[u8; MAX_PACKET_SIZE]> {
        match self.receiver.try_recv() {
            Ok(data) => Ok(data),
            Err(err) => Err(err.into())
        }
    }

    pub fn write(&self, data: [u8; MAX_PACKET_SIZE]) -> Result<()> {
        let packet_sequence = u64::from_be_bytes(data[0..8].try_into()?);
        if packet_sequence < self.sequence_index {
            println!("[warning] old data");
            return Ok(())
        }

        match self.sender.send(data) {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into())
        }
    }
}