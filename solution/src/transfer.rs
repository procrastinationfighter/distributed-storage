
#[repr(u8)]
enum MessageType {
    Write = 0x01,
    Read = 0x02,
    ReadProc = 0x03,
    Value = 0x04,
    WriteProc = 0x05,
    Ack = 0x06,
}

pub async fn deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), Error> {
    unimplemented!()
}

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    unimplemented!()
}