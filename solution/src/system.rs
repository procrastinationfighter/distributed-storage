use core::time;
use std::cell::Cell;
use std::collections::HashMap;
use std::ops::DerefMut;

use crate::atomic_register::Register;
use crate::{
    build_atomic_register, build_sectors_manager, domain::*, register_client_public::*,
    serialize_register_command, AtomicRegister, SectorsManager,
};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tokio::time::*;

// Number of workers that handle atomic registers.
const WORKER_COUNT: u64 = 500;

enum SendType {
    Broadcast(Broadcast),
    Send(Send),
    FinishBroadcast(u64),
}

pub struct Client {
    self_rank: u8,
    self_channel: UnboundedSender<RegisterCommand>,
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
    self_channel: UnboundedSender<RegisterCommand>,
    hmac_key: [u8; 64],
) {
    let mut timer = interval(Duration::from_millis(broadcast_interval));
    let mut broadcast_messages: HashMap<u64, RegisterCommand> = HashMap::new();
    // First tick finishes instantly
    timer.tick().await;

    loop {
        tokio::select! {
            _ = timer.tick() => {
                for m in broadcast_messages.values() {
                    self_channel.send(m.clone()).unwrap();
                }
            }
            m = receiver.recv() => {
                match m {
                    Some(mess) => match mess {
                        SendType::Broadcast(b) => {
                            let cmd = RegisterCommand::System((*b.cmd).clone());

                            // Send first broadcast instantly and next one when timer ticks.
                            self_channel.send(cmd.clone()).unwrap();
                            broadcast_messages.insert(b.cmd.header.sector_idx, cmd);
                        },
                        SendType::Send(s) => {
                            let cmd = RegisterCommand::System((*s.cmd).clone());
                            self_channel.send(cmd.clone()).unwrap();
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
        self_channel: UnboundedSender<RegisterCommand>,
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
struct TcpHandler {}

impl TcpHandler {
    async fn new(config: &Configuration, sender: UnboundedSender<RegisterCommand>, receiver: UnboundedReceiver<ClientResponse>) -> TcpHandler {
        todo!("receiver")
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
    let tcp_handler = TcpHandler::new(&config, sx.clone(), c_rx).await;
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

    while let Some(mess) = rx.recv().await {
        match mess {
            RegisterCommand::Client(c) => {
                let id = get_worker_id(c.header.sector_idx);
                worker_senders[id].0.send(c).unwrap();
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

enum ClientResponse {
    Success(OperationSuccess),
    InvalicHmac,
    WrongSector,
}

async fn worker_loop(
    client: Arc<Client>,
    manager: Arc<dyn SectorsManager>,
    mut client_receiver: UnboundedReceiver<ClientRegisterCommand>,
    mut system_receiver: UnboundedReceiver<SystemRegisterCommand>,
    self_ident: u8,
    processes_count: u8,
    mut respond_sender: UnboundedSender<ClientResponse>,
) {
    while let Some(client_message) = client_receiver.recv().await {
        let curr_sector = client_message.header.sector_idx;

        let mut register = build_atomic_register(
            self_ident,
            curr_sector,
            client.clone(),
            manager.clone(),
            processes_count,
        ).await;

        let mut rs = respond_sender.clone();
        let (mut this_sx, mut this_rx) = unbounded_channel();

        let callback: ClientCallback = Box::new(|op_success| {
            Box::pin(async move {
                rs.send(ClientResponse::Success(op_success)).unwrap();
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
            // register.system_command(cmd)
        }
    }
}
