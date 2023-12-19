use crate::RegisterCommand;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use crate::domain::*;

const WRITE_CONTENT_SIZE: usize = 4096;
const HMAC_TAG_SIZE: usize = 32;
const HEADER_SIZE: usize = 8;
const CLIENT_REQUEST_NO_SIZE: usize = 8;
const SECTOR_ID_SIZE: usize = 8;
// Header, request number, sector id.
const CLIENT_READ_MESSAGE_SIZE: usize = HEADER_SIZE + CLIENT_REQUEST_NO_SIZE + SECTOR_ID_SIZE;
const CLIENT_WRITE_MESSAGE_SIZE: usize = CLIENT_READ_MESSAGE_SIZE + WRITE_CONTENT_SIZE;

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

fn calculate_hmac_tag(message: &mut Vec<u8>, size: usize, secret_key: &[u8]) {
    // Initialize a new MAC instance from the secret key:
    let mut mac = HmacSha256::new_from_slice(secret_key).unwrap();

    // Calculate MAC for the data (one can provide it in multiple portions):
    mac.update(&message[0..size]);

    // Finalize the computations of MAC and obtain the resulting tag:
    let tag = mac.finalize().into_bytes();

    message.extend(tag);
}

fn verify_hmac_tag(tag: &[u8], message: &[u8], secret_key: &[u8]) -> bool {
    // Initialize a new MAC instance from the secret key:
    let mut mac = HmacSha256::new_from_slice(secret_key).unwrap();

    // Calculate MAC for the data (one can provide it in multiple portions):
    mac.update(message);

    // Verify the tag:
    mac.verify_slice(tag).is_ok()
}

async fn is_hmac_ok(
    data: &mut (dyn AsyncRead + Send + Unpin),
    message: &[u8],
    key: &[u8],
) -> Result<bool, io::Error> {
    // TODO: might be slow
    let mut tag = [0; HMAC_TAG_SIZE];
    data.read_exact(&mut tag).await?;

    Ok(verify_hmac_tag(&tag, message, key))
}

#[repr(u8)]
#[derive(Debug)]
enum MessageType {
    Read = 0x01,
    Write = 0x02,
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
            _ => Err(unexpected_error_as_io("wrong message type")),
        }
    }
}

/// Reads from data until the next magic number is found
/// and then returns remaining 4 bytes from the datagram's header.
async fn read_next_datagram_header(
    data: &mut (dyn AsyncRead + Send + Unpin),
) -> Result<[u8; 4], io::Error> {
    let mut curr_num = [0; MAGIC_NUMBER_LEN];
    let mut buf = [0; 1];

    let _ = data.read_exact(&mut curr_num).await?;

    loop {
        if curr_num == MAGIC_NUMBER {
            break;
        }

        let _ = data.read_exact(&mut buf).await?;
        curr_num.rotate_right(MAGIC_NUMBER_LEN - 1);
        curr_num[MAGIC_NUMBER_LEN - 1] = buf[0];
    }

    let mut buf = [0; HEADER_SIZE - MAGIC_NUMBER_LEN];
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
    let Some(message_type) = header_tail.last() else {
        return Err(unexpected_error_as_io("message type can't be accessed"));
    };
    let message_type = match MessageType::try_from(*message_type) {
        Ok(m) => m,
        Err(e) => {
            return Err(unexpected_error_as_io(&format!(
                "wrong message type: {}",
                e
            )))
        }
    };

    let o = match message_type {
        MessageType::Write => {
            parse_client_command(data, hmac_client_key, true, header_tail).await?
        }
        MessageType::Read => {
            parse_client_command(data, hmac_client_key, false, header_tail).await?
        }
        MessageType::ReadProc | MessageType::Value | MessageType::WriteProc | MessageType::Ack => {
            parse_system_command(data, hmac_system_key, header_tail).await?
        }
    };

    Ok(o)
}

async fn parse_client_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_client_key: &[u8; 32],
    is_write: bool,
    header_tail: [u8; 4],
) -> Result<(RegisterCommand, bool), io::Error> {
    // Header is already read.
    let size = if is_write {
        CLIENT_WRITE_MESSAGE_SIZE
    } else {
        CLIENT_READ_MESSAGE_SIZE
    } - HEADER_SIZE;
    let mut buf = vec![0; size];
    data.read_exact(&mut buf).await?;

    let request_identifier = u64::from_be_bytes(buf[0..CLIENT_REQUEST_NO_SIZE].try_into().unwrap());
    let sector_idx = u64::from_be_bytes(
        buf[CLIENT_REQUEST_NO_SIZE..(CLIENT_REQUEST_NO_SIZE + SECTOR_ID_SIZE)]
            .try_into()
            .unwrap(),
    );
    let content = if is_write {
        ClientRegisterCommandContent::Write {
            data: SectorVec(buf[(CLIENT_REQUEST_NO_SIZE + SECTOR_ID_SIZE)..].to_vec()),
        }
    } else {
        ClientRegisterCommandContent::Read
    };

    let iter: Vec<u8> = MAGIC_NUMBER
        .into_iter()
        .chain(header_tail.into_iter())
        .chain(buf.into_iter())
        .map(|x| x)
        .collect();

    Ok((
        RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier,
                sector_idx,
            },
            content,
        }),
        is_hmac_ok(data, &iter, hmac_client_key).await?,
    ))
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
    let size = if with_message {
        SYSTEM_MESSAGE_WITH_CONTENT_SIZE
    } else {
        SYSTEM_BASIC_MESSAGE_SIZE
    } - HEADER_SIZE;

    let mut buf = vec![0; size];
    let _ = data.read_exact(&mut buf).await?;

    let uuid = Uuid::from_u128(u128::from_be_bytes(buf[0..UUID_SIZE].try_into().unwrap()));
    let sector_idx = u64::from_be_bytes(
        buf[UUID_SIZE..(UUID_SIZE + SECTOR_ID_SIZE)]
            .try_into()
            .unwrap(),
    );

    let content = match MessageType::try_from(header_tail[3]).unwrap() {
        MessageType::ReadProc => SystemRegisterCommandContent::ReadProc,
        MessageType::Value => {
            let timestamp = u64::from_be_bytes(
                buf[(UUID_SIZE + SECTOR_ID_SIZE)..(UUID_SIZE + SECTOR_ID_SIZE + TIMESTAMP_SIZE)]
                    .try_into()
                    .unwrap(),
            );
            let write_rank = buf[UUID_SIZE + SECTOR_ID_SIZE + TIMESTAMP_SIZE + WR_LINE_SIZE - 1];
            let sector_data =
                buf[UUID_SIZE + SECTOR_ID_SIZE + TIMESTAMP_SIZE + WR_LINE_SIZE..].to_vec();
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data: SectorVec(sector_data),
            }
        }
        MessageType::WriteProc => {
            let timestamp = u64::from_be_bytes(
                buf[(UUID_SIZE + SECTOR_ID_SIZE)..(UUID_SIZE + SECTOR_ID_SIZE + TIMESTAMP_SIZE)]
                    .try_into()
                    .unwrap(),
            );
            let write_rank = buf[UUID_SIZE + SECTOR_ID_SIZE + TIMESTAMP_SIZE + WR_LINE_SIZE - 1];
            let sector_data =
                buf[UUID_SIZE + SECTOR_ID_SIZE + TIMESTAMP_SIZE + WR_LINE_SIZE..].to_vec();
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write: SectorVec(sector_data),
            }
        }
        MessageType::Ack => SystemRegisterCommandContent::Ack,
        _ => {
            return Err(unexpected_error_as_io(
                "unexpected message type in system command",
            ))
        }
    };

    let iter: Vec<u8> = MAGIC_NUMBER
        .into_iter()
        .chain(header_tail.into_iter())
        .chain(buf.into_iter())
        .map(|x| x)
        .collect();

    Ok((
        RegisterCommand::System(SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: header_tail[2],
                msg_ident: uuid,
                sector_idx,
            },
            content,
        }),
        is_hmac_ok(data, &iter, hmac_system_key).await?,
    ))
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
    let buf = match cmd {
        RegisterCommand::Client(c) => serialize_client_command(c, hmac_key).await,
        RegisterCommand::System(s) => serialize_system_command(s, hmac_key).await,
    }?;

    let mut written = 0;
    while written < buf.len() {
        written += writer.write(&buf[written..]).await?;
    }
    Ok(())
}

async fn serialize_client_command(
    cmd: &ClientRegisterCommand,
    hmac_key: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let size = match cmd.content {
        ClientRegisterCommandContent::Read => CLIENT_READ_MESSAGE_SIZE,
        ClientRegisterCommandContent::Write { .. } => CLIENT_WRITE_MESSAGE_SIZE,
    };

    let mut buf = Vec::with_capacity(size + HMAC_TAG_SIZE);
    serialize_main_header(&mut buf, None, client_content_to_msg_type(&cmd.content));
    serialize_client_header(&mut buf, &cmd.header);
    serialize_client_content(&mut buf, &cmd.content);

    calculate_hmac_tag(&mut buf, size, hmac_key);

    Ok(buf)
}

async fn serialize_system_command(
    cmd: &SystemRegisterCommand,
    hmac_key: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let size = match cmd.content {
        SystemRegisterCommandContent::ReadProc | SystemRegisterCommandContent::Ack => {
            SYSTEM_BASIC_MESSAGE_SIZE
        }
        SystemRegisterCommandContent::WriteProc { .. }
        | SystemRegisterCommandContent::Value { .. } => SYSTEM_MESSAGE_WITH_CONTENT_SIZE,
    };

    let mut buf = Vec::with_capacity(size + HMAC_TAG_SIZE);
    serialize_main_header(
        &mut buf,
        Some(cmd.header.process_identifier),
        system_content_to_msg_type(&cmd.content),
    );
    serialize_system_header(&mut buf, &cmd.header);
    serialize_system_content(&mut buf, &cmd.content);

    calculate_hmac_tag(&mut buf, size, hmac_key);

    Ok(buf)
}

fn serialize_main_header(buf: &mut Vec<u8>, process_rank: Option<u8>, msg_type: MessageType) {
    buf.extend_from_slice(&MAGIC_NUMBER);
    buf.push(0);
    buf.push(0);
    buf.push(process_rank.unwrap_or(0));
    buf.push(msg_type as u8);
}

fn serialize_system_header(buf: &mut Vec<u8>, header: &SystemCommandHeader) {
    buf.extend_from_slice(header.msg_ident.as_bytes());
    buf.extend_from_slice(&header.sector_idx.to_be_bytes());
}

fn serialize_system_content(buf: &mut Vec<u8>, content: &SystemRegisterCommandContent) {
    match content {
        SystemRegisterCommandContent::ReadProc | SystemRegisterCommandContent::Ack => (),
        SystemRegisterCommandContent::WriteProc {
            timestamp,
            write_rank,
            data_to_write,
        } => {
            serialize_system_content_detailed(buf, *timestamp, *write_rank, &data_to_write.0);
        }
        SystemRegisterCommandContent::Value {
            timestamp,
            write_rank,
            sector_data,
        } => {
            serialize_system_content_detailed(buf, *timestamp, *write_rank, &sector_data.0);
        }
    };
}

fn serialize_system_content_detailed(
    buf: &mut Vec<u8>,
    timestamp: u64,
    write_rank: u8,
    data: &[u8],
) {
    buf.extend_from_slice(&timestamp.to_be_bytes());
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, write_rank]);
    buf.extend_from_slice(data);
}

fn serialize_client_header(buf: &mut Vec<u8>, header: &ClientCommandHeader) {
    buf.extend_from_slice(&header.request_identifier.to_be_bytes());
    buf.extend_from_slice(&header.sector_idx.to_be_bytes());
}

fn serialize_client_content(buf: &mut Vec<u8>, content: &ClientRegisterCommandContent) {
    match content {
        ClientRegisterCommandContent::Read => (),
        ClientRegisterCommandContent::Write { data } => buf.extend_from_slice(&data.0),
    };
}

fn client_content_to_msg_type(content: &ClientRegisterCommandContent) -> MessageType {
    match content {
        ClientRegisterCommandContent::Read => MessageType::Read,
        ClientRegisterCommandContent::Write { .. } => MessageType::Write,
    }
}

fn system_content_to_msg_type(content: &SystemRegisterCommandContent) -> MessageType {
    match content {
        SystemRegisterCommandContent::ReadProc => MessageType::ReadProc,
        SystemRegisterCommandContent::Value { .. } => MessageType::Value,
        SystemRegisterCommandContent::WriteProc { .. } => MessageType::WriteProc,
        SystemRegisterCommandContent::Ack => MessageType::Ack,
    }
}
