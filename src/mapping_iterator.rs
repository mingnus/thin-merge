use anyhow::Result;
use std::sync::Arc;
use thinp::io_engine::Block;
use thinp::io_engine::IoEngine;
use thinp::pdata::btree::*;
use thinp::pdata::unpack::Unpack;
use thinp::thin::block_time::*;

//------------------------------------------

pub struct MappingIterator {
    engine: Arc<dyn IoEngine + Send + Sync>,
    leaves: Vec<u64>,
    batch_size: usize,
    cached_leaves: Vec<Block>,
    node: Node<BlockTime>,
    nr_entries: usize, // nr_entries in the current visiting node
    pos: [usize; 2],   // leaf index and entry index in leaf
}

impl MappingIterator {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, leaves: Vec<u64>) -> Result<Self> {
        let batch_size = engine.get_batch_size();
        let len = std::cmp::min(batch_size, leaves.len());
        let cached_leaves = Self::read_blocks(&engine, &leaves[..len])?;
        let node =
            unpack_node::<BlockTime>(&[], cached_leaves[0].get_data(), true, leaves.len() > 1)?;
        let nr_entries = Self::get_nr_entries(&node);

        let pos = [0, 0];

        Ok(Self {
            engine,
            leaves,
            batch_size,
            cached_leaves,
            node,
            nr_entries,
            pos,
        })
    }

    fn read_blocks(
        engine: &Arc<dyn IoEngine + Send + Sync>,
        blocks: &[u64],
    ) -> std::io::Result<Vec<Block>> {
        engine.read_many(blocks)?.into_iter().collect()
    }

    pub fn get(&self) -> Option<(u64, &BlockTime)> {
        if self.pos[0] < self.leaves.len() {
            match &self.node {
                Node::Internal { .. } => {
                    panic!("not a leaf");
                }
                Node::Leaf { keys, values, .. } => {
                    if keys.is_empty() {
                        None
                    } else {
                        Some((keys[self.pos[1]], &values[self.pos[1]]))
                    }
                }
            }
        } else {
            None
        }
    }

    fn get_nr_entries<V: Unpack>(node: &Node<V>) -> usize {
        match node {
            Node::Internal { header, .. } => header.nr_entries as usize,
            Node::Leaf { header, .. } => header.nr_entries as usize,
        }
    }

    fn inc_pos(&mut self) -> bool {
        if self.pos[0] < self.leaves.len() {
            self.pos[1] += 1;
            self.pos[1] >= self.nr_entries
        } else {
            false
        }
    }

    fn next_node(&mut self) -> Result<()> {
        self.pos[0] += 1;
        self.pos[1] = 0;

        if self.pos[0] == self.leaves.len() {
            return Ok(()); // reach the end
        }

        let idx = self.pos[0] % self.batch_size;

        // FIXME: reuse the code in the constructor
        if idx == 0 {
            let endpos = std::cmp::min(self.pos[0] + self.batch_size, self.leaves.len());
            self.cached_leaves =
                Self::read_blocks(&self.engine, &self.leaves[self.pos[0]..endpos])?;
        }

        self.node = unpack_node::<BlockTime>(&[], self.cached_leaves[idx].get_data(), true, true)?;
        self.nr_entries = Self::get_nr_entries(&self.node);

        Ok(())
    }

    pub fn step(&mut self) -> Result<()> {
        if self.inc_pos() {
            self.next_node()?;
        }
        Ok(())
    }
}

//------------------------------------------
