use anyhow::{anyhow, Result};
use std::cmp::Ordering;
use std::sync::Arc;
use thinp::io_engine::IoEngine;
use thinp::thin::block_time::*;

use crate::mapping_iterator::MappingIterator;

//------------------------------------------

pub struct MappingStream {
    iter: MappingIterator,
    current: Option<(u64, BlockTime, u64)>,
}

impl MappingStream {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, leaves: Vec<u64>) -> Result<Self> {
        let mut iter = MappingIterator::new(engine, leaves)?;
        let current = iter.next_range()?;
        Ok(Self { iter, current })
    }

    pub fn more_mappings(&self) -> bool {
        self.current.is_some()
    }

    pub fn get_mapping(&self) -> Option<&(u64, BlockTime, u64)> {
        self.current.as_ref()
    }

    pub fn consume(&mut self, delta: u64) -> Result<Option<(u64, BlockTime, u64)>> {
        match &mut self.current {
            Some((key, bt, len)) => match delta.cmp(len) {
                Ordering::Greater => Err(anyhow!("delta too lone")),
                Ordering::Equal => {
                    let ret = self.current;
                    self.current = self.iter.next_range()?;
                    Ok(ret)
                }
                Ordering::Less => {
                    let ret = Some((*key, *bt, delta));
                    *key += delta;
                    bt.block += delta;
                    *len -= delta;
                    Ok(ret)
                }
            },
            None => Ok(None),
        }
    }

    // consume without returning
    pub fn skip(&mut self, delta: u64) -> Result<()> {
        if let Some((key, bt, len)) = &mut self.current {
            match delta.cmp(len) {
                Ordering::Greater => return Err(anyhow!("delta too lone")),
                Ordering::Equal => {
                    self.current = self.iter.next_range()?;
                }
                Ordering::Less => {
                    *key += delta;
                    bt.block += delta;
                    *len -= delta;
                }
            }
        }

        Ok(())
    }

    pub fn consume_all(&mut self) -> Result<Option<(u64, BlockTime, u64)>> {
        if self.current.is_some() {
            let ret = self.current;
            self.current = self.iter.next_range()?;
            Ok(ret)
        } else {
            Ok(None)
        }
    }

    // consume_all without returning
    pub fn skip_all(&mut self) -> Result<()> {
        if self.current.is_some() {
            self.current = self.iter.next_range()?;
        }

        Ok(())
    }
}

//------------------------------------------
