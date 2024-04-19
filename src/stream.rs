use anyhow::Result;
use std::sync::Arc;
use thinp::io_engine::IoEngine;
use thinp::thin::block_time::*;

use crate::mapping_iterator::MappingIterator;

//------------------------------------------

pub struct MappingStream {
    iter: MappingIterator,
    current: Option<(u64, BlockTime)>,
}

impl MappingStream {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, leaves: Vec<u64>) -> Result<Self> {
        let iter = MappingIterator::new(engine, leaves)?;
        let current = iter.get().map(|(k, v)| (k, *v));
        Ok(Self { iter, current })
    }

    pub fn more_mappings(&self) -> bool {
        self.current.is_some()
    }

    pub fn get_mapping(&self) -> Option<&(u64, BlockTime)> {
        self.current.as_ref()
    }

    pub fn consume(&mut self) -> Result<Option<(u64, BlockTime)>> {
        match self.get_mapping() {
            Some(&m) => {
                let r = Ok(Some(m));
                self.iter.step()?;
                self.current = self.iter.get().map(|(k, &v)| (k, v));
                r
            }
            None => Ok(None),
        }
    }

    pub fn step(&mut self) -> Result<()> {
        if self.more_mappings() {
            self.iter.step()?;
            self.current = self.iter.get().map(|(k, &v)| (k, v));
        }
        Ok(())
    }
}

//------------------------------------------
