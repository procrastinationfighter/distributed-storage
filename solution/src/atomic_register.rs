use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use uuid::Uuid;

use crate::domain::*;
use crate::OperationSuccess;
use crate::{
    atomic_register_public, AtomicRegister, RegisterClient, SectorIdx, SectorVec, SectorsManager,
    SystemRegisterCommand,
};

pub struct Register {
    timestamp: u64,
    write_rank: u8,
    val: SectorVec,
    self_ident: u8,

    op_id: Uuid,
    reading: bool,
    writing: bool,
    write_val: SectorVec,
    read_val: SectorVec,
    read_list: BTreeMap<(u64, u8), SectorVec>,
    ack_list: HashSet<u8>,
    success_callback:
        Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,

    client: Arc<dyn RegisterClient>,
    manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    sector_idx: SectorIdx,
}

fn dummy_callback(_: OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(core::future::ready(()))
}

impl Register {
    async fn new(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Register {
        let (timestamp, write_rank) = sectors_manager.read_metadata(sector_idx).await;
        let val = sectors_manager.read_data(sector_idx).await;

        Register {
            timestamp,
            write_rank,
            val,
            self_ident,

            op_id: Uuid::new_v4(),
            reading: false,
            writing: false,
            write_val: SectorVec(vec![]),
            read_val: SectorVec(vec![]),
            read_list: BTreeMap::new(),
            ack_list: HashSet::new(),
            success_callback: Box::new(dummy_callback),

            client: register_client,
            manager: sectors_manager,
            processes_count,
            sector_idx,
        }
    }

    async fn send(&self, cmd: SystemRegisterCommand, target: u8) {
        self.client
            .send(crate::Send {
                cmd: Arc::new(cmd),
                target,
            })
            .await
    }

    async fn broadcast(&self, cmd: SystemRegisterCommand) {
        self.client
            .broadcast(crate::Broadcast { cmd: Arc::new(cmd) })
            .await
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
        self.op_id = Uuid::new_v4();
        self.read_list = BTreeMap::new();
        self.ack_list = HashSet::new();
        self.success_callback = success_callback;

        let mut message = SystemRegisterCommand { 
            header: SystemCommandHeader { process_identifier: self.self_ident, msg_ident: self.op_id, sector_idx: self.sector_idx }, 
            content: SystemRegisterCommandContent::ReadProc, 
        };

        match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.reading = true;
            },
            ClientRegisterCommandContent::Write { data } => {
                self.writing = true;
            },
        };

        self.broadcast(message).await;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        todo!("atomicregister::systemcommand")
    }
}
