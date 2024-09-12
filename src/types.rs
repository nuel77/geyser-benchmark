use serde::Deserialize;
use solana_sdk::commitment_config::CommitmentLevel;
use yellowstone_grpc_proto::geyser::SubscribeUpdateSlot;

#[derive(Debug, Clone, Deserialize)]
pub struct SlotInfo {
    pub slot: u64,
    pub parent: u64,
    pub commitment: CommitmentLevel,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WsResponse {
    pub result: SlotInfo,
}

pub trait Identity {
    fn identity(&self) -> String;
}

impl Identity for SlotInfo {
    fn identity(&self) -> String {
        let commitment_id = match self.commitment {
            CommitmentLevel::Processed => 0,
            CommitmentLevel::Confirmed => 1,
            CommitmentLevel::Finalized => 2,
        };
        format!("{}-{}", self.slot, commitment_id)
    }
}

impl Identity for SubscribeUpdateSlot {
    fn identity(&self) -> String {
        format!("{}-{}", self.slot, self.status)
    }
}
