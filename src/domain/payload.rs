#[derive(Debug)]
pub struct Payload {
    pub sequence_num: u64,
    pub id: u16,
    pub timestamp: u64,
    pub value: f32
}

impl Payload {
    pub fn new(data: [u8; 22]) -> Self {
        let mut cursor = 0;

        let sequence_num = u64::from_be_bytes(data[cursor..cursor+8].try_into().unwrap());
        cursor += 8;

        let id: u16 = u16::from_be_bytes(data[cursor..cursor+2].try_into().unwrap());
        cursor += 2;
        
        let timestamp = u64::from_be_bytes(data[cursor..cursor+8].try_into().unwrap());
        cursor += 8;

        let value = f32::from_be_bytes(data[cursor..cursor+4].try_into().unwrap());

        Self {
            id,
            sequence_num,
            timestamp,
            value
        }
    }

    pub fn to_newtwork_bytes(&self) -> [u8; 22] {
        let mut packet = [0u8; 22];
        let mut cursor = 0;

        packet[cursor..cursor+8].copy_from_slice(&self.sequence_num.to_be_bytes());
        cursor += 8;

        packet[cursor..cursor+2].copy_from_slice(&self.id.to_be_bytes());
        cursor += 2;

        packet[cursor..cursor+8].copy_from_slice(&self.timestamp.to_be_bytes());
        cursor += 8;

        packet[cursor..cursor+4].copy_from_slice(&self.value.to_be_bytes());

        return packet;
    }
}