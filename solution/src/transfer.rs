use crate::{RegisterCommand, transfer};
use std::io;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use uuid::Uuid;

use crate::domain::*;

const WRITE_CONTENT_SIZE: usize = 4096;
const HMAC_TAG_SIZE: usize = 256;
const HEADER_SIZE: usize = 8;
const CLIENT_REQUEST_NO_SIZE: usize = 8;
const SECTOR_ID_SIZE: usize = 8;
// Header, request number, sector id.
const READ_MESSAGE_SIZE: usize = HEADER_SIZE + CLIENT_REQUEST_NO_SIZE + SECTOR_ID_SIZE;
const WRITE_MESSAGE_SIZE: usize = READ_MESSAGE_SIZE + WRITE_CONTENT_SIZE;

const UUID_SIZE: usize = 16;
const TIMESTAMP_SIZE: usize = 8;
const WR_LINE_SIZE: usize = 8;
const SECTOR_DATA_SIZE: usize = 4096;
const SYSTEM_VAL_MESSAGE_SIZE: usize = TIMESTAMP_SIZE + WR_LINE_SIZE + SECTOR_DATA_SIZE;
const SYSTEM_BASIC_MESSAGE_SIZE: usize = HEADER_SIZE + UUID_SIZE + SECTOR_ID_SIZE;
const SYSTEM_MESSAGE_WITH_CONTENT_SIZE: usize = SYSTEM_BASIC_MESSAGE_SIZE + SYSTEM_VAL_MESSAGE_SIZE;

// COPIED FROM LAB06
// Create a type alias:
type HmacSha256 = Hmac<Sha256>;

fn calculate_hmac_tag(message: &str, secret_key: &[u8]) -> [u8; 32] {
    // Initialize a new MAC instance from the secret key:
    let mut mac = HmacSha256::new_from_slice(secret_key).unwrap();

    // Calculate MAC for the data (one can provide it in multiple portions):
    mac.update(message.as_bytes());

    // Finalize the computations of MAC and obtain the resulting tag:
    let tag = mac.finalize().into_bytes();

    tag.into()
}

fn verify_hmac_tag(tag: &[u8], message: &str, secret_key: &[u8]) -> bool {
    // Initialize a new MAC instance from the secret key:
    let mut mac = HmacSha256::new_from_slice(secret_key).unwrap();

    // Calculate MAC for the data (one can provide it in multiple portions):
    mac.update(message.as_bytes());

    // Verify the tag:
    mac.verify_slice(tag).is_ok()
}

async fn is_hmac_ok<I>(data: &mut (dyn AsyncRead + Send + Unpin), iter: I, key: &[u8]) -> Result<bool, io::Error> where I: IntoIterator<Item = u8> {
        // TODO: might be slow
        let mess = String::from_iter(iter.into_iter().map(|x| x as char));
        let mut tag = [0;HMAC_TAG_SIZE];
        data.read_exact(&mut tag).await?;

    Ok(verify_hmac_tag(&tag, &mess, key))
}

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

    let o = match message_type {
        MessageType::Write => parse_client_command(data, hmac_client_key, true, header_tail).await?,
        MessageType::Read => parse_client_command(data, hmac_client_key, false, header_tail).await?,
        MessageType::ReadProc | MessageType::Value |MessageType::WriteProc | MessageType::Ack => parse_system_command(data, hmac_system_key, header_tail).await?,
    };

    todo!()
}

async fn parse_client_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_client_key: &[u8; 32],
    is_write: bool,
    header_tail: [u8; 4]) -> Result<(RegisterCommand, bool), io::Error> {
        // Header is already read.
        let size = if is_write {WRITE_MESSAGE_SIZE} else {READ_MESSAGE_SIZE};
        let mut buf = vec![0;size];
        data.read_exact(&mut buf).await?;

        let request_identifier = u64::from_be_bytes(buf[0..CLIENT_REQUEST_NO_SIZE].try_into().unwrap());
        let sector_idx = u64::from_be_bytes(buf[CLIENT_REQUEST_NO_SIZE..(CLIENT_REQUEST_NO_SIZE+SECTOR_ID_SIZE)].try_into().unwrap());
        let content = if is_write {
            ClientRegisterCommandContent::Write {data: SectorVec(buf[(CLIENT_REQUEST_NO_SIZE+SECTOR_ID_SIZE)..].to_vec())}
        } else {
            ClientRegisterCommandContent::Read
        };

        let iter = MAGIC_NUMBER.into_iter().chain(header_tail.into_iter()).chain(buf.into_iter());

        Ok((RegisterCommand::Client(
            ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier,
                    sector_idx,
                },
                content,
            }
        ),
        is_hmac_ok(data, iter, hmac_client_key).await?,)
    )
}

async fn parse_system_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    header_tail: [u8; 4],
) -> Result<(RegisterCommand, bool), io::Error> {
    // Header is already read.
    // As for now, only requests are supported. 
    // TODO: add responses
    let with_message = is_system_command_with_message(header_tail[3]);
    let size = if with_message {SYSTEM_MESSAGE_WITH_CONTENT_SIZE} else {SYSTEM_BASIC_MESSAGE_SIZE};
    let mut buf = vec![0;size];
    data.read_exact(&mut buf).await?;
    
    let uuid = Uuid::from_u128(u128::from_be_bytes(buf[0..UUID_SIZE].try_into().unwrap()));
    let sector_idx = u64::from_be_bytes(buf[UUID_SIZE..(UUID_SIZE+SECTOR_ID_SIZE)].try_into().unwrap());
    let content = match MessageType::try_from(header_tail[3]).unwrap() {
        MessageType::ReadProc => {
            SystemRegisterCommandContent::ReadProc
        },
        MessageType::Value => {
            let timestamp = u64::from_be_bytes(buf[(UUID_SIZE+SECTOR_ID_SIZE)..(UUID_SIZE+SECTOR_ID_SIZE+TIMESTAMP_SIZE)].try_into().unwrap());
            let write_rank = buf[(UUID_SIZE+SECTOR_ID_SIZE+TIMESTAMP_SIZE+WR_LINE_SIZE - 1)];
            let sector_data = buf[UUID_SIZE+SECTOR_ID_SIZE+TIMESTAMP_SIZE+WR_LINE_SIZE..].to_vec();
            SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data: SectorVec(sector_data) }
        },
        MessageType::WriteProc => {
            let timestamp = u64::from_be_bytes(buf[(UUID_SIZE+SECTOR_ID_SIZE)..(UUID_SIZE+SECTOR_ID_SIZE+TIMESTAMP_SIZE)].try_into().unwrap());
            let write_rank = buf[(UUID_SIZE+SECTOR_ID_SIZE+TIMESTAMP_SIZE+WR_LINE_SIZE - 1)];
            let sector_data = buf[UUID_SIZE+SECTOR_ID_SIZE+TIMESTAMP_SIZE+WR_LINE_SIZE..].to_vec();
            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write: SectorVec(sector_data) }
        },
        MessageType::Ack => {
            SystemRegisterCommandContent::Ack
        },
        _ => return Err(unexpected_error_as_io("unexpected message type in system command")),
    };

    let iter = MAGIC_NUMBER.into_iter().chain(header_tail.into_iter()).chain(buf.into_iter());

    Ok((RegisterCommand::System(
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: header_tail[2],
                msg_ident: uuid,
                sector_idx,
            },
            content,
        }
    ),
    is_hmac_ok(data, iter, hmac_system_key).await?,)
)
}

fn is_system_command_with_message(code: u8) -> bool {
    match MessageType::try_from(code).unwrap() {
         MessageType::Value | MessageType::WriteProc => true,
         _ => false,
    }
}

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), io::Error> {
    unimplemented!()
}