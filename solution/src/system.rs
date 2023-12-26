use crate::atomic_register::Register;
use crate::transfer::calculate_hmac_tag;
use crate::{
    build_atomic_register, build_sectors_manager, deserialize_register_command, domain::*,
    register_client_public::*, serialize_register_command, AtomicRegister, SectorsManager,
};
use core::time;
use std::cell::Cell;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::ops::DerefMut;
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tokio::time::*;
use uuid::Uuid;

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
    self_rank: u8,
    self_channel: UnboundedSender<(RegisterCommand, SocketAddr)>,
    broadcast_task_handles: Vec<JoinHandle<()>>,
    broadcast_senders: HashMap<u8, UnboundedSender<SendType>>,

    client_key: [u8; 32],
    system_key: [u8; 64],
}

async fn sending_loop(
    mut receiver: UnboundedReceiver<SendType>,
    broadcast_interval: u64,
    addr: SocketAddr,
    hmac_key: [u8; 64],
) {
    let mut timer = interval(Duration::from_millis(broadcast_interval));
    let mut broadcast_messages: HashMap<u64, RegisterCommand> = HashMap::new();
    let mut stream = TcpSocket::new_v4().unwrap().connect(addr).await.unwrap();
    // First tick finishes instantly
    timer.tick().await;

    loop {
        tokio::select! {
            _ = timer.tick() => {
                for m in broadcast_messages.values() {
                    serialize_register_command(m, &mut stream, &hmac_key).await.unwrap();
                }
            }
            m = receiver.recv() => {
                match m {
                    Some(mess) => match mess {
                        SendType::Broadcast(b) => {
                            let cmd = RegisterCommand::System((*b.cmd).clone());

                            // Send first broadcast instantly and next one when timer ticks.
                            serialize_register_command(&cmd, &mut stream, &hmac_key).await.unwrap();
                            broadcast_messages.insert(b.cmd.header.sector_idx, cmd);
                        },
                        SendType::Send(s) => {
                            let cmd = RegisterCommand::System((*s.cmd).clone());
                            serialize_register_command(&cmd, &mut stream, &hmac_key).await.unwrap();
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

async fn self_sending_loop(
    mut receiver: UnboundedReceiver<SendType>,
    broadcast_interval: u64,
    self_channel: UnboundedSender<(RegisterCommand, SocketAddr)>,
    hmac_key: [u8; 64],
) {
    let mut timer = interval(Duration::from_millis(broadcast_interval));
    let mut broadcast_messages: HashMap<u64, RegisterCommand> = HashMap::new();
    let dummy_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0));

    // First tick finishes instantly
    timer.tick().await;

    loop {
        tokio::select! {
            _ = timer.tick() => {
                for m in broadcast_messages.values() {
                    self_channel.send((m.clone(), dummy_addr.clone())).unwrap();
                }
            }
            m = receiver.recv() => {
                match m {
                    Some(mess) => match mess {
                        SendType::Broadcast(b) => {
                            let cmd = RegisterCommand::System((*b.cmd).clone());

                            // Send first broadcast instantly and next one when timer ticks.
                            self_channel.send((cmd.clone(), dummy_addr.clone())).unwrap();
                            broadcast_messages.insert(b.cmd.header.sector_idx, cmd);
                        },
                        SendType::Send(s) => {
                            let cmd = RegisterCommand::System((*s.cmd).clone());
                            self_channel.send((cmd.clone(), dummy_addr.clone())).unwrap();
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
    pub async fn new(
        config: Configuration,
        self_channel: UnboundedSender<(RegisterCommand, SocketAddr)>,
    ) -> Client {
        let mut broadcast_senders = HashMap::with_capacity(config.public.tcp_locations.len());
        let mut handles = Vec::with_capacity(config.public.tcp_locations.len());

        for (i, (s, port)) in config.public.tcp_locations.iter().enumerate() {
            let (sx, rx) = unbounded_channel();
            if i + 1 == config.public.self_rank.into() {
                let addr = SocketAddr::new(IpAddr::from_str(s).unwrap(), *port);

                handles.push(tokio::spawn(sending_loop(
                    rx,
                    500,
                    addr,
                    config.hmac_system_key.clone(),
                )));
            } else {
                let addr = SocketAddr::new(IpAddr::from_str(s).unwrap(), *port);

                handles.push(tokio::spawn(self_sending_loop(
                    rx,
                    500,
                    self_channel.clone(),
                    config.hmac_system_key.clone(),
                )));
            }
            broadcast_senders.insert((i + 1) as u8, sx);
        }

        Client {
            self_rank: config.public.self_rank,
            self_channel,
            broadcast_task_handles: handles,
            broadcast_senders,

            client_key: config.hmac_client_key,
            system_key: config.hmac_system_key,
        }
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

// Receives messages over tcp and responds to clients
struct TcpHandler {
    receiver_handle: JoinHandle<()>,
    sender_handle: JoinHandle<()>,
}

struct TcpReceiver {
    sender: UnboundedSender<(RegisterCommand, SocketAddr)>,
    error_sender: UnboundedSender<(ClientResponse, SocketAddr)>,
    clients: Arc<Mutex<HashMap<Uuid, String>>>,
    client_key: [u8; 32],
    system_key: [u8; 64],
    sector_count: u64,
}

struct TcpSender {
    receiver: UnboundedReceiver<(ClientResponse, SocketAddr)>,
    clients: Arc<Mutex<HashMap<Uuid, String>>>,
    socket: TcpSocket,
}

async fn client_receiver_loop(mut tcp_receiver: TcpReceiver, addr: SocketAddr) {
    let socket = TcpSocket::new_v4().unwrap();
    let dummy_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0));
    socket.bind(addr).unwrap();

    let listener = socket
        .listen((CLIENT_CONNECTION_LIMIT + NODES_LIMIT) as u32)
        .unwrap();

    while let Ok((mut stream, addr)) = listener.accept().await {
        // TODO: rewrite so that it does not read only one connection
        while let Ok((cmd, hmac_ok)) = deserialize_register_command(
            &mut stream,
            &tcp_receiver.system_key,
            &tcp_receiver.client_key,
        )
        .await
        {
            if !hmac_ok {
                tcp_receiver
                    .error_sender
                    .send((ClientResponse::InvalicHmac, addr))
                    .unwrap();
            } else if let RegisterCommand::Client(c) = cmd {
                if c.header.sector_idx >= tcp_receiver.sector_count {
                    tcp_receiver
                        .error_sender
                        .send((ClientResponse::WrongSector, addr))
                        .unwrap();
                } else {
                    tcp_receiver
                        .sender
                        .send((RegisterCommand::Client(c), addr))
                        .unwrap();
                }
            } else {
                tcp_receiver.sender.send((cmd, dummy_addr)).unwrap();
            }
        }
    }
}

async fn client_sender_loop(tcp_sender: TcpSender) {
    // TODO this should actually be a "connection" loop
    while let Some((mess, addr)) = tcp_sender.receiver.recv().await {
        serialize_client_response(mess, writer, &tcp_sender.client_key).await;
    }
}

async fn serialize_client_response(
    mut res: ClientResponse,
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

    calculate_hmac_tag(&mut buf, buf.len(), hmac_key);

    writer.write_all_buf(&mut buf)?;

    Ok(())
}

impl TcpHandler {
    async fn new(
        config: &Configuration,
        sender: UnboundedSender<(RegisterCommand, SocketAddr)>,
        receiver: UnboundedReceiver<(ClientResponse, SocketAddr)>,
        error_sender: UnboundedSender<(ClientResponse, SocketAddr)>,
    ) -> TcpHandler {
        let (s, port) = &config.public.tcp_locations[(config.public.self_rank - 1) as usize];
        let addr = SocketAddr::new(IpAddr::from_str(&s).unwrap(), *port);
        let clients = Arc::new(Mutex::new(HashMap::new()));

        // TODO: this architecture is wrong
        // correct approach: create a loop for every connection
        // if connection is severed, connect again when sending response
        let rcvr = TcpReceiver {
            sender,
            error_sender,
            clients: clients.clone(),
            client_key: config.hmac_client_key,
            system_key: config.hmac_system_key,
            sector_count: config.public.n_sectors,
        };

        let socket = TcpSocket::new_v4().unwrap();
        socket.bind(addr).unwrap();

        let sndr = TcpSender {
            receiver,
            clients,
            socket,
        };

        let rh = tokio::spawn(client_receiver_loop(rcvr, addr));
        let sh = tokio::spawn(client_sender_loop(sndr));

        TcpHandler {
            receiver_handle: rh,
            sender_handle: sh,
        }
    }
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
    let (c_sx, c_rx) = unbounded_channel();
    let tcp_handler = TcpHandler::new(&config, sx.clone(), c_rx, c_sx.clone()).await;
    let manager = build_sectors_manager(config.public.storage_dir.clone()).await;
    let client = Arc::new(Client::new(config, sx.clone()).await);

    let mut worker_handles = Vec::with_capacity(WORKER_COUNT as usize);
    let mut worker_senders = Vec::with_capacity(WORKER_COUNT as usize);

    for i in 0..WORKER_COUNT {
        let (wsx_client, wrx_client) = unbounded_channel();
        let (wsx_system, wrx_system) = unbounded_channel();
        worker_handles.push(tokio::spawn(worker_loop(
            client.clone(),
            manager.clone(),
            wrx_client,
            wrx_system,
            self_ident,
            processes_count,
            c_sx.clone(),
        )));
        worker_senders.push((wsx_client, wsx_system))
    }

    while let Some((mess, addr)) = rx.recv().await {
        match mess {
            RegisterCommand::Client(c) => {
                let id = get_worker_id(c.header.sector_idx);
                worker_senders[id].0.send((c, addr)).unwrap();
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
    mut client_receiver: UnboundedReceiver<(ClientRegisterCommand, SocketAddr)>,
    mut system_receiver: UnboundedReceiver<SystemRegisterCommand>,
    self_ident: u8,
    processes_count: u8,
    mut respond_sender: UnboundedSender<(ClientResponse, SocketAddr)>,
) {
    while let Some((client_message, addr)) = client_receiver.recv().await {
        let curr_sector = client_message.header.sector_idx;

        let mut register = build_atomic_register(
            self_ident,
            curr_sector,
            client.clone(),
            manager.clone(),
            processes_count,
        )
        .await;

        let mut rs = respond_sender.clone();
        let mut a2 = addr.clone();
        let (this_sx, mut this_rx) = unbounded_channel();

        let callback: ClientCallback = Box::new(move |op_success| {
            Box::pin(async move {
                rs.send((ClientResponse::Success(op_success), a2)).unwrap();
                this_sx.send(()).unwrap();
            })
        });

        register.client_command(client_message, callback).await;

        loop {
            tokio::select! {
                Some(system_message) = system_receiver.recv() => {
                    register.system_command(system_message).await;
                }
                Some(()) = this_rx.recv() => {
                    client.end_broadcast(curr_sector).await;
                    break;
                }
            }
        }
        while let Some(system_message) = system_receiver.recv().await {
            register.system_command(system_message);
        }
    }
}
