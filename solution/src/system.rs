use crate::transfer::calculate_hmac_tag;
use crate::{
    build_atomic_register, build_sectors_manager, deserialize_register_command, domain::*,
    register_client_public::*, serialize_register_command, SectorsManager,
};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::*;

// Number of workers that handle atomic registers.
const WORKER_COUNT: u64 = 500;

const READ_RESPONSE_SIZE: usize = 8 + 8 + SECTOR_SIZE;
const WRITE_RESPONSE_SIZE: usize = 8 + 8;

const READ_RESPONSE_TYPE: u8 = 0x41;
const WRITE_RESPONSE_TYPE: u8 = 0x42;

enum SendType {
    Broadcast(Broadcast),
    Send(Send),
    FinishBroadcast(u64),
}

pub struct Client {
    broadcast_senders: HashMap<u8, UnboundedSender<SendType>>,
}

async fn sending_loop(
    mut receiver: UnboundedReceiver<SendType>,
    broadcast_interval: u64,
    addr: (String, u16),
    hmac_key: [u8; 64],
) {
    let mut timer = interval(Duration::from_millis(broadcast_interval));
    let mut retry_conn_timer = interval(Duration::from_millis(broadcast_interval / 2));
    let mut broadcast_messages: HashMap<u64, RegisterCommand> = HashMap::new();
    let mut stream = try_gaining_connection_with_peer(addr.clone()).await;
    // First tick finishes instantly
    timer.tick().await;
    retry_conn_timer.tick().await;

    loop {
        tokio::select! {
            _ = timer.tick(), if stream.is_some() => {
                let mut st = stream.unwrap();
                let mut res = Ok(());
                for m in broadcast_messages.values() {
                    res = serialize_register_command(m, &mut st, &hmac_key).await;
                }
                stream = match res {
                    Ok(_) => Some(st),
                    Err(_) => None,
                };
            }
            _ = retry_conn_timer.tick(), if stream.is_none() => {
                stream = try_gaining_connection_with_peer(addr.clone()).await;
            }
            m = receiver.recv(), if stream.is_some() => {
                match m {
                    Some(mess) => match mess {
                        SendType::Broadcast(b) => {
                            let cmd = RegisterCommand::System((*b.cmd).clone());
                            broadcast_messages.insert(b.cmd.header.sector_idx, cmd);
                        },
                        SendType::Send(s) => {
                            let cmd = RegisterCommand::System((*s.cmd).clone());
                            let mut st = stream.unwrap();
                            let res = serialize_register_command(&cmd, &mut st, &hmac_key).await;
                            stream = match res {
                                Ok(_) => Some(st),
                                Err(_) => None,
                            };
                        },
                        SendType::FinishBroadcast(target) => {
                            broadcast_messages.remove(&target);
                        },
                    },
                    None => break,
                }
            }
        }
    }
}

async fn try_gaining_connection_with_peer(addr: (String, u16)) -> Option<TcpStream> {
    TcpStream::connect(addr).await.ok()
}

async fn self_sending_loop(
    mut receiver: UnboundedReceiver<SendType>,
    broadcast_interval: u64,
    self_channel: UnboundedSender<(RegisterCommand, UnboundedSender<ClientResponse>)>,
) {
    let mut timer = interval(Duration::from_millis(broadcast_interval));
    let mut broadcast_messages: HashMap<u64, RegisterCommand> = HashMap::new();
    let (dummy_sender, _) = unbounded_channel();

    // First tick finishes instantly
    timer.tick().await;

    loop {
        tokio::select! {
            _ = timer.tick() => {
                for m in broadcast_messages.values() {
                    self_channel.send((m.clone(), dummy_sender.clone())).unwrap();
                }
            }
            m = receiver.recv() => {
                match m {
                    Some(mess) => match mess {
                        SendType::Broadcast(b) => {
                            let cmd = RegisterCommand::System((*b.cmd).clone());

                            // Send first broadcast instantly and next one when timer ticks.
                            self_channel.send((cmd.clone(), dummy_sender.clone())).unwrap();
                            broadcast_messages.insert(b.cmd.header.sector_idx, cmd);
                        },
                        SendType::Send(s) => {
                            let cmd = RegisterCommand::System((*s.cmd).clone());
                            self_channel.send((cmd.clone(), dummy_sender.clone())).unwrap();
                        },
                        SendType::FinishBroadcast(target) => {
                            broadcast_messages.remove(&target);
                        },
                    },
                    None => break,
                }
            }
        }
    }
}

impl Client {
    async fn new(
        config: Configuration,
        self_channel: UnboundedSender<(RegisterCommand, UnboundedSender<ClientResponse>)>,
    ) -> Client {
        let mut broadcast_senders = HashMap::with_capacity(config.public.tcp_locations.len());
        let mut handles = Vec::with_capacity(config.public.tcp_locations.len());

        for (i, addr) in config.public.tcp_locations.iter().enumerate() {
            let (sx, rx) = unbounded_channel();
            if i + 1 != config.public.self_rank.into() {
                handles.push(tokio::spawn(sending_loop(
                    rx,
                    500,
                    addr.clone(),
                    config.hmac_system_key,
                )));
            } else {
                handles.push(tokio::spawn(self_sending_loop(
                    rx,
                    500,
                    self_channel.clone(),
                )));
            }
            broadcast_senders.insert((i + 1) as u8, sx);
        }

        Client { broadcast_senders }
    }

    async fn end_broadcast(&self, sector_idx: u64) {
        for sender in self.broadcast_senders.values() {
            sender.send(SendType::FinishBroadcast(sector_idx)).unwrap();
        }
    }
}

#[async_trait::async_trait]
impl RegisterClient for Client {
    /// Sends a system message to a single process.
    async fn send(&self, msg: Send) {
        self.broadcast_senders
            .get(&msg.target)
            .unwrap()
            .send(SendType::Send(msg))
            .unwrap();
    }

    /// Broadcasts a system message to all processes in the system, including self.
    async fn broadcast(&self, msg: Broadcast) {
        for sender in self.broadcast_senders.values() {
            sender
                .send(SendType::Broadcast(Broadcast {
                    cmd: msg.cmd.clone(),
                }))
                .unwrap();
        }
    }
}

#[derive(Clone)]
struct TcpReceiver {
    sender: UnboundedSender<(RegisterCommand, UnboundedSender<ClientResponse>)>,
    client_key: [u8; 32],
    system_key: [u8; 64],
    sector_count: u64,
}

impl TcpReceiver {
    async fn run_receiver(
        config: &Configuration,
        sender: UnboundedSender<(RegisterCommand, UnboundedSender<ClientResponse>)>,
    ) {
        let addr = &config.public.tcp_locations[(config.public.self_rank - 1) as usize];

        let rcvr = TcpReceiver {
            sender,
            client_key: config.hmac_client_key,
            system_key: config.hmac_system_key,
            sector_count: config.public.n_sectors,
        };

        tokio::spawn(client_receiver_loop(rcvr, addr.clone()));
    }
}

async fn client_receiver_loop(tcp_receiver: TcpReceiver, addr: (String, u16)) {
    let listener = TcpListener::bind(addr).await.unwrap();

    while let Ok((stream, _)) = listener.accept().await {
        let (rs, ws) = stream.into_split();
        let (sender_sx, sender_rx) = unbounded_channel();

        tokio::spawn(one_client_receiver_loop(
            rs,
            tcp_receiver.clone(),
            sender_sx,
        ));
        tokio::spawn(client_sender_loop(ws, tcp_receiver.client_key, sender_rx));
    }
}

async fn one_client_receiver_loop(
    mut rs: OwnedReadHalf,
    tcp_receiver: TcpReceiver,
    sender_sx: UnboundedSender<ClientResponse>,
) {
    while let Ok((cmd, hmac_ok)) =
        deserialize_register_command(&mut rs, &tcp_receiver.system_key, &tcp_receiver.client_key)
            .await
    {
        if let RegisterCommand::Client(c) = cmd {
            let mut response = ClientResponse {
                response: None,
                status_code: StatusCode::Ok,
                sector_idx: c.header.sector_idx,
                request_number: c.header.request_identifier,
            };
            if !hmac_ok {
                response.status_code = StatusCode::AuthFailure;
                sender_sx.send(response).unwrap();
                continue;
            } else if response.sector_idx >= tcp_receiver.sector_count {
                response.status_code = StatusCode::InvalidSectorIndex;
                sender_sx.send(response).unwrap();
                continue;
            }
            tcp_receiver
                .sender
                .send((RegisterCommand::Client(c), sender_sx.clone()))
                .unwrap();
        } else if !hmac_ok {
            log::warn!("hmac could not be confirmed for a message");
        } else {
            tcp_receiver.sender.send((cmd, sender_sx.clone())).unwrap();
        }
    }
}

async fn client_sender_loop(
    mut ws: OwnedWriteHalf,
    client_key: [u8; 32],
    mut receiver: UnboundedReceiver<ClientResponse>,
) {
    while let Some(response) = receiver.recv().await {
        let buf = serialize_client_response(response, &mut ws, &client_key).await;

        if let Err(e) = buf {
            log::debug!("connection issue: {}", e.to_string());
        }
    }
}

async fn serialize_client_response(
    res: ClientResponse,
    writer: &mut (dyn AsyncWrite + std::marker::Send + Unpin),
    hmac_key: &[u8],
) -> io::Result<()> {
    let mut buf = if res.is_with_content() {
        Vec::with_capacity(READ_RESPONSE_SIZE)
    } else {
        Vec::with_capacity(WRITE_RESPONSE_SIZE)
    };

    // Push magic number
    buf.extend(MAGIC_NUMBER.iter());
    // Push padding
    buf.push(0);
    buf.push(0);
    // Push status code
    buf.push(res.status_code as u8);
    // Push message type
    buf.push(res.msg_type());

    // Request number
    buf.extend(res.request_number.to_be_bytes().iter());

    if res.is_with_content() {
        buf.append(&mut res.response.unwrap().0);
    }

    let size = buf.len();
    calculate_hmac_tag(&mut buf, size, hmac_key);

    let mut written = 0;
    while written < buf.len() {
        written += writer.write(&buf[written..]).await.unwrap();
    }

    Ok(())
}

pub async fn create_system(config: Configuration) {
    let self_ident = config.public.self_rank;
    let processes_count = config.public.tcp_locations.len() as u8;
    // Architecture:
    // Tcp handler receives and parses message and sends it to sx.
    // This function (see loop below) creates workers. When sx receives something, adequate worker gets the message.
    // Each worker has two channels: for client and system messages. A worker works only on one sector at a time.
    // After finishing, workers send responses to TcpHandler.
    let (sx, mut rx) = unbounded_channel();
    TcpReceiver::run_receiver(&config, sx.clone()).await;
    let manager = build_sectors_manager(config.public.storage_dir.clone()).await;
    let client = Arc::new(Client::new(config, sx.clone()).await);

    let mut worker_handles = Vec::with_capacity(WORKER_COUNT as usize);
    let mut worker_senders = Vec::with_capacity(WORKER_COUNT as usize);

    for _ in 0..WORKER_COUNT {
        let (wsx_client, wrx_client) = unbounded_channel();
        let (wsx_system, wrx_system) = unbounded_channel();
        worker_handles.push(tokio::spawn(worker_loop(
            client.clone(),
            manager.clone(),
            wrx_client,
            wrx_system,
            self_ident,
            processes_count,
        )));
        worker_senders.push((wsx_client, wsx_system))
    }

    while let Some((mess, sender)) = rx.recv().await {
        match mess {
            RegisterCommand::Client(c) => {
                let id = get_worker_id(c.header.sector_idx);
                worker_senders[id].0.send((c, sender)).unwrap();
            }
            RegisterCommand::System(s) => {
                let id = get_worker_id(s.header.sector_idx);
                worker_senders[id].1.send(s).unwrap();
            }
        }
    }
}

fn get_worker_id(sector_idx: u64) -> usize {
    (sector_idx % WORKER_COUNT) as usize
}

struct ClientResponse {
    response: Option<SectorVec>,
    status_code: StatusCode,
    sector_idx: u64,
    request_number: u64,
}

impl ClientResponse {
    fn is_with_content(&self) -> bool {
        self.response.is_some() && self.status_code == StatusCode::Ok
    }

    fn msg_type(&self) -> u8 {
        if self.response.is_some() {
            READ_RESPONSE_TYPE
        } else {
            WRITE_RESPONSE_TYPE
        }
    }
}

async fn worker_loop(
    client: Arc<Client>,
    manager: Arc<dyn SectorsManager>,
    mut client_receiver: UnboundedReceiver<(
        ClientRegisterCommand,
        UnboundedSender<ClientResponse>,
    )>,
    mut system_receiver: UnboundedReceiver<SystemRegisterCommand>,
    self_ident: u8,
    processes_count: u8,
) {
    while let Some((client_message, response_sender)) = client_receiver.recv().await {
        let curr_sector = client_message.header.sector_idx;

        let mut register = build_atomic_register(
            self_ident,
            curr_sector,
            client.clone(),
            manager.clone(),
            processes_count,
        )
        .await;

        let rs = response_sender.clone();
        let client2 = client.clone();

        let callback: ClientCallback = Box::new(move |op_success| {
            Box::pin(async move {
                let response = ClientResponse {
                    response: if let OperationReturn::Read(ReadReturn { read_data }) =
                        op_success.op_return
                    {
                        Some(read_data)
                    } else {
                        None
                    },
                    status_code: StatusCode::Ok,
                    sector_idx: curr_sector,
                    request_number: op_success.request_identifier,
                };
                rs.send(response).unwrap();
                client2.end_broadcast(curr_sector).await;
            })
        });

        register.client_command(client_message, callback).await;

        while let Some(system_message) = system_receiver.recv().await {
            register.system_command(system_message).await;
        }
    }
}
