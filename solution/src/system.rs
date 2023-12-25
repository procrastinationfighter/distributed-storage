use core::time;
use std::cell::Cell;
use std::collections::HashMap;
use std::ops::DerefMut;

use crate::atomic_register::Register;
use crate::{domain::*, register_client_public::*, serialize_register_command};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tokio::time::*;

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
        self.broadcast_senders.get(&msg.target).unwrap().send(SendType::Send(msg)).unwrap();
    }

    /// Broadcasts a system message to all processes in the system, including self.
    async fn broadcast(&self, msg: Broadcast) {
        for sender in self.broadcast_senders.values() {
            sender.send(SendType::Broadcast(Broadcast { cmd: msg.cmd.clone() })).unwrap();
        }
    }
}
