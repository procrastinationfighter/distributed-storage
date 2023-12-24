use crate::atomic_register::Register;
use crate::register_client_public::*;

#[async_trait::async_trait]
impl RegisterClient for Register {
    /// Sends a system message to a single process.
    async fn send(&self, msg: Send) {
        todo!("register_client::send")
    }

    /// Broadcasts a system message to all processes in the system, including self.
    async fn broadcast(&self, msg: Broadcast) {
        todo!("register_client::broadcast")
    }
}
