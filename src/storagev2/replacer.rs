use std::collections::{hash_map::Entry, HashMap};

use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct LRUKNode {
    i: usize,
    history: Vec<u64>,
    pin: u64,
}

impl LRUKNode {
    pub fn new(i: usize, ts: u64) -> Self {
        Self {
            i,
            history: vec![ts],
            pin: 0,
        }
    }

    pub fn get_k_distance(&self, k: usize) -> Option<u64> {
        let len = self.history.len();
        if len < k {
            return None;
        }

        let latest = self.history.last().unwrap();
        let kth = len - k;

        Some(latest - self.history[kth])
    }
}

#[derive(Default, Debug)]
struct LRUKReplacer {
    nodes: HashMap<usize, LRUKNode>,
    current_ts: u64,
    k: usize,
}

impl LRUKReplacer {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            ..Default::default()
        }
    }

    pub fn evict(&mut self) -> Option<usize> {
        let mut max: (usize, u64) = (0, 0);
        let mut single_access: Vec<&LRUKNode> = Vec::new();
        for (id, node) in &self.nodes {
            if node.pin != 0 {
                continue;
            }

            match node.get_k_distance(self.k) {
                Some(d) if d > max.1 => max = (*id, d),
                None => single_access.push(node),
                _ => {}
            };
        }

        if max.1 != 0 {
            return Some(max.0);
        }

        if single_access.is_empty() {
            return None;
        }

        // If multiple frames have less than k recorded accesses, choose the one with the
        // earliest timestamp to evict
        let mut earliest: (usize, u64) = (0, u64::MAX);
        for node in &single_access {
            match node.history.last() {
                Some(ts) if *ts < earliest.1 => earliest = (node.i, *ts),
                None => todo!(),
                _ => {}
            }
        }

        Some(earliest.0)
    }

    pub fn record_access(&mut self, i: usize) {
        match self.nodes.entry(i) {
            Entry::Occupied(mut node) => {
                node.get_mut().history.push(self.current_ts);
                self.current_ts += 1;
            }
            Entry::Vacant(entry) => {
                entry.insert(LRUKNode::new(i, self.current_ts));
                self.current_ts += 1;
            }
        }
    }

    pub fn pin(&mut self, i: usize) {
        if let Some(node) = self.nodes.get_mut(&i) {
            node.pin += 1;
        }
    }

    pub fn unpin(&mut self, i: usize) {
        if let Some(node) = self.nodes.get_mut(&i) {
            node.pin -= 1;
        }
    }

    pub fn remove(&mut self, i: usize) {
        match self.nodes.entry(i) {
            Entry::Occupied(node) => {
                assert!(node.get().pin == 0);
                node.remove();
            }
            Entry::Vacant(_) => {
                eprintln!(
                    "warn: attempt to remove frame that has not been registered in the replacer: \
                    {i}"
                );
            }
        }
    }
}

pub enum LRUKMessage {
    Evict {
        reply: oneshot::Sender<Option<usize>>,
    },
    RecordAccess(usize),
    Pin(usize),
    Unpin(usize),
    Remove(usize),
}

pub struct LRUKActor {
    inner: LRUKReplacer,
    rx: mpsc::Receiver<LRUKMessage>,
}

impl LRUKActor {
    pub fn new(k: usize, rx: mpsc::Receiver<LRUKMessage>) -> Self {
        let inner = LRUKReplacer::new(k);

        Self { inner, rx }
    }

    pub async fn run(&mut self) {
        while let Some(m) = self.rx.recv().await {
            match m {
                LRUKMessage::Evict { reply } => {
                    let ret = self.inner.evict();

                    if reply.send(ret).is_err() {
                        eprintln!("replacer channel error: could not reply to evict message");
                    }
                }
                LRUKMessage::RecordAccess(i) => self.inner.record_access(i),
                LRUKMessage::Pin(i) => self.inner.pin(i),
                LRUKMessage::Unpin(i) => self.inner.unpin(i),
                LRUKMessage::Remove(i) => self.inner.remove(i),
            }
        }
    }
}

#[derive(Clone)]
pub struct LRUKHandle {
    tx: mpsc::Sender<LRUKMessage>,
}

impl LRUKHandle {
    pub fn new(k: usize) -> Self {
        let (tx, rx) = mpsc::channel(256);

        let mut replacer = LRUKActor::new(k, rx);
        let _jh = tokio::spawn(async move { replacer.run().await });

        Self { tx }
    }

    pub async fn evict(&self) -> Option<usize> {
        let (tx, rx) = oneshot::channel();

        if let Err(e) = self.tx.send(LRUKMessage::Evict { reply: tx }).await {
            eprintln!("replacer channel error: {e}");
        }

        rx.await.expect("replacer has been killed")
    }

    pub async fn record_access(&self, i: usize) {
        if let Err(e) = self.tx.send(LRUKMessage::RecordAccess(i)).await {
            eprintln!("replacer channel error: {e}");
        }
    }

    pub async fn pin(&self, i: usize) {
        if let Err(e) = self.tx.send(LRUKMessage::Pin(i)).await {
            eprintln!("replacer channel error: {e}");
        }
    }

    pub async fn unpin(&self, i: usize) {
        if let Err(e) = self.tx.send(LRUKMessage::Unpin(i)).await {
            eprintln!("replacer channel error: {e}");
        }
    }

    pub fn blocking_unpin(&self, i: usize) {
        if let Err(e) = self.tx.blocking_send(LRUKMessage::Unpin(i)) {
            eprintln!("replacer channel error: {e}");
        }
    }

    pub async fn remove(&self, i: usize) {
        if let Err(e) = self.tx.send(LRUKMessage::Remove(i)).await {
            eprintln!("replacer channel error: {e}");
        }
    }
}
