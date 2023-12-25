use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use uuid::Uuid;

use crate::domain::*;
use crate::OperationSuccess;
use crate::{
    AtomicRegister, RegisterClient, SectorIdx, SectorVec, SectorsManager, SystemRegisterCommand,
};

pub struct Register {
    timestamp: u64,
    write_rank: u8,
    val: SectorVec,
    self_ident: u8,

    op_id: Uuid,
    client_op_id: u64,
    reading: bool,
    writing: bool,
    write_phase: bool,
    write_val: SectorVec,
    read_val: SectorVec,
    read_list: HashMap<u8, (u64, u8, SectorVec)>,
    ack_list: HashSet<u8>,
    success_callback: ClientCallback,

    client: Arc<dyn RegisterClient>,
    manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    sector_idx: SectorIdx,
}

fn dummy_callback(_: OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(core::future::ready(()))
}

impl Register {
    pub async fn new(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Register {
        log::debug!(
            "creating a new register, self ident: {}, sector_idx: {}",
            self_ident,
            sector_idx
        );
        let (timestamp, write_rank) = sectors_manager.read_metadata(sector_idx).await;
        let val = sectors_manager.read_data(sector_idx).await;

        Register {
            timestamp,
            write_rank,
            val,
            self_ident,

            op_id: Uuid::new_v4(),
            client_op_id: 0,
            reading: false,
            writing: false,
            write_phase: false,
            write_val: SectorVec(vec![]),
            read_val: SectorVec(vec![]),
            read_list: HashMap::new(),
            ack_list: HashSet::new(),
            success_callback: Box::new(dummy_callback),

            client: register_client,
            manager: sectors_manager,
            processes_count,
            sector_idx,
        }
    }

    async fn send(&self, cmd: SystemRegisterCommand, target: u8) {
        log::debug!("{} sends {:?} to {}", self.self_ident, cmd, target);
        self.client
            .send(crate::Send {
                cmd: Arc::new(cmd),
                target,
            })
            .await
    }

    async fn broadcast(&self, cmd: SystemRegisterCommand) {
        log::debug!("{} broadcasts {:?}", self.self_ident, cmd);
        self.client
            .broadcast(crate::Broadcast { cmd: Arc::new(cmd) })
            .await
    }

    async fn store(&mut self, sector: (SectorVec, u64, u8)) {
        log::debug!("{} stores {:?}", self.self_ident, sector);
        self.manager.write(self.sector_idx, &sector).await;

        self.timestamp = sector.1;
        self.write_rank = sector.2;
        self.val = sector.0;
    }

    async fn read_proc(&mut self, header: SystemCommandHeader) {
        self.send(
            SystemRegisterCommand {
                header: SystemCommandHeader {
                    process_identifier: self.self_ident,
                    msg_ident: header.msg_ident,
                    sector_idx: self.sector_idx,
                },
                content: SystemRegisterCommandContent::Value {
                    timestamp: self.timestamp,
                    write_rank: self.write_rank,
                    sector_data: self.val.clone(),
                },
            },
            header.process_identifier,
        )
        .await
    }

    async fn value(&mut self, from: u8, timestamp: u64, write_rank: u8, data: SectorVec) {
        if self.write_phase {
            log::debug!(
                "atomic register ({}, {}) received a value message when in write phase",
                self.sector_idx,
                self.self_ident
            );
            return;
        }

        self.read_list.insert(from, (timestamp, write_rank, data));

        if self.read_list.len() > (self.processes_count / 2).into()
            && (self.reading || self.writing)
        {
            log::debug!(
                "{} enters write phase for operation {}, sector: {}",
                self.self_ident,
                self.op_id,
                self.sector_idx
            );

            let mut v = SectorVec(vec![]);
            std::mem::swap(&mut v, &mut self.val);

            self.read_list
                .insert(self.self_ident, (self.timestamp, self.write_rank, v));

            let (mut maxts, mut rr) = (0, 0);

            // highest(*)
            let mut m = HashMap::new();
            std::mem::swap(&mut m, &mut self.read_list);
            for (t, w, v) in m.into_values() {
                if (t, w) > (maxts, rr) {
                    maxts = t;
                    rr = w;
                    self.read_val = v;
                }
            }

            self.ack_list = HashSet::new();
            self.write_phase = true;

            if self.reading {
                self.broadcast(SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.self_ident,
                        msg_ident: self.op_id,
                        sector_idx: self.sector_idx,
                    },
                    content: SystemRegisterCommandContent::WriteProc {
                        timestamp: maxts,
                        write_rank: rr,
                        data_to_write: self.read_val.clone(),
                    },
                })
                .await;
            } else {
                let sector = (self.write_val.clone(), maxts + 1, self.self_ident);
                self.store(sector).await;

                let mut v = SectorVec(vec![]);
                std::mem::swap(&mut v, &mut self.write_val);

                self.broadcast(SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.self_ident,
                        msg_ident: self.op_id,
                        sector_idx: self.sector_idx,
                    },
                    content: SystemRegisterCommandContent::WriteProc {
                        timestamp: self.timestamp,
                        write_rank: self.write_rank,
                        data_to_write: v,
                    },
                })
                .await;
            }
        }
    }

    async fn write_proc(
        &mut self,
        header: SystemCommandHeader,
        timestamp: u64,
        write_rank: u8,
        data: SectorVec,
    ) {
        if (timestamp, write_rank) > (self.timestamp, self.write_rank) {
            log::debug!(
                "{} stores new data in write_proc, sector: {}",
                self.self_ident,
                self.sector_idx
            );
            self.store((data, timestamp, write_rank)).await;
        }

        self.send(
            SystemRegisterCommand {
                header: SystemCommandHeader {
                    process_identifier: self.self_ident,
                    msg_ident: header.msg_ident,
                    sector_idx: self.sector_idx,
                },
                content: SystemRegisterCommandContent::Ack,
            },
            header.process_identifier,
        )
        .await
    }

    async fn ack(&mut self, from: u8) {
        if !self.write_phase {
            log::debug!(
                "atomic register ({}, {}) received an ack message when not in write phase",
                self.sector_idx,
                self.self_ident
            );
            return;
        }

        self.ack_list.insert(from);

        if self.ack_list.len() > (self.processes_count / 2).into() && (self.reading || self.writing)
        {
            log::debug!(
                "{} enters finishes the transaction and runs callback, sector: {}",
                self.self_ident,
                self.sector_idx
            );

            self.ack_list = HashSet::new();
            self.write_phase = false;

            let callback = std::mem::replace(&mut self.success_callback, Box::new(dummy_callback));

            if self.reading {
                log::debug!(
                    "{} finishes a read, sector: {}",
                    self.self_ident,
                    self.sector_idx
                );
                let mut v = SectorVec(vec![]);
                std::mem::swap(&mut v, &mut self.read_val);
                self.reading = false;
                callback(OperationSuccess {
                    request_identifier: self.client_op_id,
                    op_return: OperationReturn::Read(ReadReturn { read_data: v }),
                })
                .await;
            } else {
                log::debug!(
                    "{} finishes a write, sector: {}",
                    self.self_ident,
                    self.sector_idx
                );
                self.writing = false;
                callback(OperationSuccess {
                    request_identifier: self.client_op_id,
                    op_return: OperationReturn::Write,
                })
                .await;
            }
        }
    }
}

#[async_trait::async_trait]
impl AtomicRegister for Register {
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: Box<
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
        >,
    ) {
        if cmd.header.sector_idx != self.sector_idx {
            log::warn!("atomic register for sector {}, write rank {} received client request for sector {}", self.sector_idx, self.write_rank, cmd.header.sector_idx);
            return;
        }
        log::debug!("{} received a client command: {:?}", self.self_ident, cmd);

        self.op_id = Uuid::new_v4();
        self.client_op_id = cmd.header.request_identifier;
        self.read_list = HashMap::new();
        self.ack_list = HashSet::new();
        self.success_callback = success_callback;

        match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.reading = true;
            }
            ClientRegisterCommandContent::Write { data } => {
                self.writing = true;
                self.write_val = data;
            }
        };

        self.broadcast(SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: self.op_id,
                sector_idx: self.sector_idx,
            },
            content: SystemRegisterCommandContent::ReadProc,
        })
        .await;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        if cmd.header.sector_idx != self.sector_idx {
            log::warn!("atomic register for sector {}, write rank {} received server request for sector {}", self.sector_idx, self.write_rank, cmd.header.sector_idx);
            return;
        }

        log::debug!("{} received a system command: {:?}", self.self_ident, cmd);

        match cmd.content {
            SystemRegisterCommandContent::ReadProc => self.read_proc(cmd.header).await,
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => {
                if self.op_id == cmd.header.msg_ident {
                    self.value(
                        cmd.header.process_identifier,
                        timestamp,
                        write_rank,
                        sector_data,
                    )
                    .await;
                }
            }
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => {
                self.write_proc(cmd.header, timestamp, write_rank, data_to_write)
                    .await;
            }
            SystemRegisterCommandContent::Ack => {
                if self.op_id == cmd.header.msg_ident {
                    self.ack(cmd.header.process_identifier).await;
                }
            }
        }
    }
}
