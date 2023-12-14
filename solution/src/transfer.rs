use crate::{RegisterCommand, transfer};
use std::io;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};

use crate::domain::*;

#[repr(u8)]
enum MessageType {
    Write = 0x01,
    Read = 0x02,
    ReadProc = 0x03,
    Value = 0x04,
    WriteProc = 0x05,
    Ack = 0x06,
}

impl TryFrom<u8> for MessageType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Write),
            0x02 => Ok(Self::Read),
            0x03 => Ok(Self::ReadProc),
            0x04 => Ok(Self::Value),
            0x05 => Ok(Self::WriteProc),
            0x06 => Ok(Self::Ack),
            _ => Err(unexpected_error_as_io("wrong message type")) 
        }
    }
}

/// Reads from data until the next magic number is found
/// and then returns remaining 4 bytes from the datagram's header.
async fn read_next_datagram_header(data: &mut (dyn AsyncRead + Send + Unpin)) -> Result<[u8;4], io::Error> {
    let mut i = 0;
    let mut buf = [0;1];

    while i < MAGIC_NUMBER.len() {
        let _ = data.read_exact(&mut buf).await?;
        if buf[0] == MAGIC_NUMBER[i] {
            i += 1;
        } else {
            i = 0;
        }
    }

    let mut buf = [0;4];
    let _ = data.read_exact(&mut buf).await?;

    Ok(buf)
}

fn unexpected_error_as_io(mess: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, mess)
}

pub async fn deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), io::Error> {
    let header_tail = read_next_datagram_header(data).await?;
    let Some (message_type) = header_tail.last() else {return Err(unexpected_error_as_io("message type can't be accessed"))};
    let message_type = match MessageType::try_from(*message_type) {
        Ok(m) => m,
        Err(e) => return Err(unexpected_error_as_io("wrong message type"))
    };

    match message_type {
        MessageType::Write => todo!(),
        MessageType::Read => todo!(),
        MessageType::ReadProc => todo!(),
        MessageType::Value => todo!(),
        MessageType::WriteProc => todo!(),
        MessageType::Ack => todo!(),
    }
}

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), io::Error> {
    unimplemented!()
}