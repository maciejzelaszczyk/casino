use std::pin::Pin;
use std::{collections::HashMap, marker::PhantomData};
use std::marker::Send;
use std::sync::{Arc, Mutex};
use sp_api::{TransactionFor, ProvideRuntimeApi};
use sc_consensus::block_import::BlockImport;
use sp_consensus::Error as ConsensusError;
use sc_consensus::{BlockCheckParams, BlockImportParams, ImportResult};
use sc_network::PeerId;
use sc_network_gossip::{GossipEngine, Network as NetworkT, ValidatorContext, ValidationResult};
use sp_runtime::traits::{Block as BlockT, Hash, Header, NumberFor};
use sp_timestamp::{InherentDataProvider, Timestamp};
use sc_utils::mpsc::{TracingUnboundedSender, TracingUnboundedReceiver};
use futures::{Future, FutureExt, StreamExt};
use futures::task::{Context, Poll};
use std::str;
use sp_blockchain::well_known_cache_keys::Id;

const CASINO_PROTOCOL: &str = "casino/1";
const CASINO_TOPIC: &[u8] = "casino".as_bytes();

const BLOCK_NUMBER_THRESHOLD: usize = 10;
const PREDICTION_WINDOW: usize = 5;

pub struct CasinoBlockImport<Block: BlockT, Client, InnerBlockImport> {
    inner_block_import: InnerBlockImport,
    timestamp_sender: TracingUnboundedSender<(Timestamp, NumberFor<Block>)>,
    _phantom_marker: PhantomData<Client>
}

impl<Block: BlockT, Client, InnerBlockImport> CasinoBlockImport<Block, Client, InnerBlockImport> {
    pub fn new(inner_block_import: InnerBlockImport, timestamp_sender: TracingUnboundedSender<(Timestamp, NumberFor<Block>)>) -> Self {
        Self { inner_block_import, timestamp_sender, _phantom_marker: Default::default() }
    }
}

impl<Block: BlockT, Client, InnerBlockImport: Clone> Clone for CasinoBlockImport<Block, Client, InnerBlockImport>
{
    fn clone(&self) -> Self {
        Self {
            inner_block_import: self.inner_block_import.clone(),
            timestamp_sender: self.timestamp_sender.clone(),
            _phantom_marker: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<Block, Client, InnerBlockImport> BlockImport<Block> for CasinoBlockImport<Block, Client, InnerBlockImport>
    where
        Block: BlockT,
        Client: ProvideRuntimeApi<Block> + Send,
        InnerBlockImport: BlockImport<Block, Error = ConsensusError, Transaction = TransactionFor<Client, Block>> + Send + Sync,
        TransactionFor<Client, Block>: 'static
{
    type Error = ConsensusError;
    type Transaction = TransactionFor<Client, Block>; 

    async fn check_block(&mut self, block: BlockCheckParams<Block>) -> Result<ImportResult, Self::Error> {
        self.inner_block_import.check_block(block).await
    }

    async fn import_block(&mut self, block: BlockImportParams<Block, Self::Transaction>, cache: HashMap<Id, Vec<u8>>) -> Result<ImportResult, Self::Error> {
        let block_number = *block.header.number();
        let timestamp = InherentDataProvider::from_system_time().timestamp();
        println!("Timestamp imported {}, block number: {}", u64_timestamp_to_string(*timestamp), block_number);
        self.timestamp_sender.unbounded_send((timestamp, block_number)).expect("Failed to send current timestamp and block number.");
        self.inner_block_import.import_block(block, cache).await.map_err(Into::into)
    }
}
pub struct CassinoGossipEngine<Block: BlockT> {
    gossip_engine: Arc<Mutex<GossipEngine<Block>>>,
    timestamp_receiver: TracingUnboundedReceiver<(Timestamp, NumberFor<Block>)>,
    block_number_threshold: usize,
    prediction_window: usize,
    first_timestamps: HashMap<NumberFor<Block>, u64>
}

impl<Block: BlockT> CassinoGossipEngine<Block> {
    pub fn new<Network: NetworkT<Block> + Clone + Send + 'static>(
        network: Network,
        timestamp_receiver: TracingUnboundedReceiver<(Timestamp, NumberFor<Block>)>,
        block_number_threshold: usize,
        prediction_window: usize
    ) -> Self {
        let validator = Arc::new(Validator {});
        let gossip_engine = Arc::new(Mutex::new(GossipEngine::new(
            network.clone(),
            CASINO_PROTOCOL,
            validator.clone(),
            None
        ))); 
        Self {
            gossip_engine,
            timestamp_receiver,
            block_number_threshold,
            prediction_window,
            first_timestamps: HashMap::new()
        }
    }
    fn gossip(&mut self, received_timestamp: (Timestamp, NumberFor<Block>)) {
        let (timestamp, block_number) = received_timestamp;
        let timestamp: u64 = timestamp.into();
        println!("Current timestamp: {} | Block number: {}", u64_timestamp_to_string(timestamp), block_number);
        let block_number_threshold = NumberFor::<Block>::from(self.block_number_threshold as u32);
        self.first_timestamps.entry(block_number).or_insert(timestamp);
        if block_number.eq(&block_number_threshold) {
            let mut timestamps = self.first_timestamps.values().cloned().collect::<Vec<_>>();
            timestamps.sort();
            let diffs = timestamps.windows(2).map(|w| w[1] - w[0]);
            let mean_diff: u64 = diffs.sum::<u64>() / ((timestamps.len() - 1) as u64);
            let predicted_timesamp = timestamp + self.prediction_window as u64 * mean_diff;
            println!("Gossiping | Timestamp prediction: {}", u64_timestamp_to_string(predicted_timesamp));
            self.gossip_engine.lock().unwrap().gossip_message(topic_hash::<Block>(CASINO_TOPIC), Vec::from(predicted_timesamp.to_be_bytes()), false);
        }
    }

    fn check_send_timestamp_gossip(&mut self, timestamp_and_block_number: TypeSentFromBlockImporter<Block>) {
        let (timestamp, block_number) = timestamp_and_block_number;
        log::info!("🎰 Received timestamp {} and block number {} from block importer.", u64_timestamp_to_string(*timestamp), &block_number);
        let block_number_threshold = sp_runtime::traits::NumberFor::<Block>::from(self.gossip_block_number_threshold as u32);
        if block_number.eq(&block_number_threshold) {
            log::debug!("🎰 Saving previous block timestamp {} UTC", u64_timestamp_to_string(*timestamp));
            self.previous_block_timestamp = *timestamp;
        } else if self.previous_block_timestamp != 0 {
            let diff = *timestamp - self.previous_block_timestamp;
            let predicted_timestamp = self.previous_block_timestamp + self.gossip_next_block_number * diff;
            log::info!("🎰 Timestamp of block {}th to arrive: {} UTC", self.gossip_block_number_threshold + self.gossip_next_block_number, u64_timestamp_to_string(predicted_timestamp));
            log::debug!("🎰 Diff between previous block: {}", u64_timestamp_to_string(diff));
            self.gossip_engine.lock().unwrap().gossip_message(
                casino_topic::<Block>(),
                Vec::from(predicted_timestamp.to_be_bytes()),
                false);
            self.previous_block_timestamp = 0;
        }
    }
}

fn u64_timestamp_to_string(timestamp: u64) -> String {
    let seconds = timestamp / 1000;
    let nanoseconds: u32 = (seconds % 1000) as u32 * 1_000_000;
    chrono::NaiveDateTime::from_timestamp(seconds as i64, nanoseconds).to_string()
}

pub struct CasinoError(String);

impl<Block: BlockT> Unpin for CassinoGossipEngine<Block> {}

impl<Block: BlockT> Future for CassinoGossipEngine<Block> {
    type Output = Result<(), CasinoError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(_) = self.gossip_engine.lock().unwrap().poll_unpin(cx) {
            return Poll::Ready(Err(CasinoError(String::from("Gossip engine future finished."))));
        }
        loop {
            match self.timestamp_receiver.poll_next_unpin(cx) {
                Poll::Ready(Some(timestamp)) => self.gossip(timestamp),
                Poll::Ready(None) => return Poll::Ready(Err(CasinoError(String::from("Gossip validator report stream closed.")))),
                Poll::Pending => break
            }
        }
        let mut messages_for_topic = self.gossip_engine.lock().unwrap().messages_for(topic_hash::<Block>(CASINO_TOPIC));
        loop {
            match messages_for_topic.poll_next_unpin(cx) {
                Poll::Ready(Some(notification)) => {
                    let a: [u8; 8] = notification.message.clone().try_into().unwrap();
                    let timestamp = u64::from_be_bytes(a);
                    println!("Collected timestamp: {}", u64_timestamp_to_string(timestamp));
                },
                Poll::Ready(None) => return Poll::Ready(Err(CasinoError(String::from("Gossip engine stream closed.")))),
                Poll::Pending => break
            }
        }
        Poll::Pending
    }

}

#[derive(Clone)]
pub struct Validator;

impl<Block: BlockT> sc_network_gossip::Validator<Block> for Validator {
    fn validate(&self, _: &mut dyn ValidatorContext<Block>, _: &PeerId, _: &[u8]) -> ValidationResult<Block::Hash> {
        ValidationResult::ProcessAndDiscard(topic_hash::<Block>(CASINO_TOPIC))
    }
    fn message_expired<'a>(&'a self) -> Box<dyn FnMut(Block::Hash, &[u8]) -> bool + 'a> {
        Box::new(move |_topic, _data| true)
    }
}

fn topic_hash<Block: BlockT>(topic: &[u8]) -> Block::Hash {
    <<Block::Header as Header>::Hashing as Hash>::hash(topic)
}

pub fn run_casino_gossiper<Block: BlockT, Network: NetworkT<Block> + Clone + Send + 'static>(rx: TracingUnboundedReceiver<(Timestamp, NumberFor<Block>)>, network: Network) -> sp_blockchain::Result<impl Future<Output = ()> + Send> {
    let casino_gossip_engine = CassinoGossipEngine::new(
        network,
        rx,
        BLOCK_NUMBER_THRESHOLD,
        PREDICTION_WINDOW
    );
    let casino_gossip_engine = casino_gossip_engine.map(|res| match res {
        Ok(()) => log::error!("Casino gossiper future has concluded naturally, this should be unreachable."),
        Err(e) => log::error!("Casino gossiper error: {:?}", e.0)
    });
    Ok(casino_gossip_engine)
}
