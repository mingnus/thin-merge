use anyhow::{anyhow, Result};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::thread;
use thinp::commands::engine::*;
use thinp::io_engine::IoEngine;
use thinp::pdata::btree::{self, *};
use thinp::pdata::btree_error::KeyRange;
use thinp::pdata::btree_leaf_walker::{LeafVisitor, LeafWalker};
use thinp::pdata::btree_walker::btree_to_map;
use thinp::pdata::space_map::common::SMRoot;
use thinp::pdata::space_map::metadata::core_metadata_sm;
use thinp::pdata::space_map::RestrictedSpaceMap;
use thinp::pdata::unpack::unpack;
use thinp::report::Report;
use thinp::thin::block_time::*;
use thinp::thin::device_detail::DeviceDetail;
use thinp::thin::dump::RunBuilder;
use thinp::thin::ir::{self, MetadataVisitor};
use thinp::thin::metadata_repair::is_superblock_consistent;
use thinp::thin::restore::Restorer;
use thinp::thin::superblock::*;
use thinp::write_batcher::WriteBatcher;

use crate::mapping_iterator::MappingIterator;
use crate::stream::MappingStream;

//------------------------------------------

const QUEUE_DEPTH: usize = 4;
const BUFFER_LEN: usize = 1024;
const WRITE_BATCH_SIZE: usize = 32;

struct CollectLeaves {
    leaves: Vec<u64>,
}

impl CollectLeaves {
    fn new() -> CollectLeaves {
        CollectLeaves { leaves: Vec::new() }
    }
}

impl LeafVisitor<BlockTime> for CollectLeaves {
    fn visit(&mut self, _kr: &KeyRange, b: u64) -> btree::Result<()> {
        self.leaves.push(b);
        Ok(())
    }

    fn visit_again(&mut self, b: u64) -> btree::Result<()> {
        self.leaves.push(b);
        Ok(())
    }

    fn end_walk(&mut self) -> btree::Result<()> {
        Ok(())
    }
}

fn collect_leaves(
    engine: Arc<dyn IoEngine + Send + Sync>,
    roots: &[u64],
) -> Result<BTreeMap<u64, Vec<u64>>> {
    let mut map: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let mut sm = RestrictedSpaceMap::new(engine.get_nr_blocks());

    for r in roots {
        let mut w = LeafWalker::new(engine.clone(), &mut sm, false);
        let mut v = CollectLeaves::new();
        let mut path = vec![0];
        w.walk::<CollectLeaves, BlockTime>(&mut path, &mut v, *r)?;

        map.insert(*r, v.leaves);
    }

    Ok(map)
}

struct MergeIterator {
    base_stream: MappingStream,
    snap_stream: MappingStream,
}

impl MergeIterator {
    fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        base_root: u64,
        snap_root: u64,
    ) -> Result<Self> {
        let mut leaves = collect_leaves(engine.clone(), &[base_root, snap_root])?;
        let base_stream = MappingStream::new(engine.clone(), leaves.remove(&base_root).unwrap())?;
        let snap_stream = MappingStream::new(engine, leaves.remove(&snap_root).unwrap())?;

        Ok(Self {
            base_stream,
            snap_stream,
        })
    }

    fn next(&mut self) -> Result<Option<(u64, BlockTime)>> {
        match (
            self.base_stream.more_mappings(),
            self.snap_stream.more_mappings(),
        ) {
            (true, true) => {
                let base_map = self.base_stream.get_mapping().unwrap();
                let snap_map = self.snap_stream.get_mapping().unwrap();

                match base_map.0.cmp(&snap_map.0) {
                    Ordering::Less => self.base_stream.consume(),
                    Ordering::Equal => {
                        self.base_stream.step()?;
                        self.snap_stream.consume()
                    }
                    Ordering::Greater => self.snap_stream.consume(),
                }
            }
            (true, false) => self.base_stream.consume(),
            (false, true) => self.snap_stream.consume(),
            (false, false) => Ok(None),
        }
    }
}

//------------------------------------------

fn update_device_details(
    engine: Arc<dyn IoEngine + Send + Sync>,
    mapped_blocks: u64,
) -> Result<()> {
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let b = engine.read(sb.details_root)?;
    let mut details_leaf = unpack_node::<DeviceDetail>(&[], b.get_data(), false, true)?;

    if let Node::Leaf { ref mut values, .. } = details_leaf {
        values[0].mapped_blocks = mapped_blocks;
    } else {
        return Err(anyhow!("unexpected node type"));
    }

    let mut cursor = std::io::Cursor::new(b.get_data());
    pack_node(&details_leaf, &mut cursor)?;
    thinp::checksum::write_checksum(b.get_data(), thinp::checksum::BT::NODE)?;
    engine.write(&b)?;

    Ok(())
}

fn merge(
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
    sb: &Superblock,
    origin_id: u64,
    snap_id: u64,
    rebase: bool,
) -> Result<()> {
    let sm = core_metadata_sm(engine_out.get_nr_blocks(), 2);
    let mut w = WriteBatcher::new(engine_out.clone(), sm.clone(), WRITE_BATCH_SIZE);
    let mut restorer = Restorer::new(&mut w, report);

    let roots = btree_to_map::<u64>(&mut vec![], engine_in.clone(), false, sb.mapping_root)?;
    let details =
        btree_to_map::<DeviceDetail>(&mut vec![], engine_in.clone(), false, sb.details_root)?;

    let origin_dev = *details
        .get(&origin_id)
        .ok_or_else(|| anyhow!("Unable to find the details for the origin"))?;
    let origin_root = *roots
        .get(&origin_id)
        .ok_or_else(|| anyhow!("Unable to find mapping tree for the origin"))?;
    let snap_dev = *details
        .get(&snap_id)
        .ok_or_else(|| anyhow!("Unable to find the details for the snapshot"))?;
    let snap_root = *roots
        .get(&snap_id)
        .ok_or_else(|| anyhow!("Unable to find mapping tree for the snapshot"))?;

    let mut iter = MergeIterator::new(engine_in.clone(), origin_root, snap_root)?;

    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let out_sb = ir::Superblock {
        uuid: "".to_string(),
        time: sb.time,
        transaction: sb.transaction_id,
        flags: None,
        version: Some(sb.version),
        data_block_size: sb.data_block_size,
        nr_data_blocks: data_root.nr_blocks,
        metadata_snap: None,
    };

    let out_dev = if rebase {
        ir::Device {
            dev_id: snap_id as u32,
            mapped_blocks: snap_dev.mapped_blocks,
            transaction: snap_dev.transaction_id,
            creation_time: snap_dev.creation_time,
            snap_time: snap_dev.snapshotted_time,
        }
    } else {
        ir::Device {
            dev_id: origin_id as u32,
            mapped_blocks: origin_dev.mapped_blocks,
            transaction: origin_dev.transaction_id,
            creation_time: origin_dev.creation_time,
            snap_time: origin_dev.snapshotted_time,
        }
    };

    let (tx, rx) = mpsc::sync_channel::<Vec<ir::Map>>(QUEUE_DEPTH);

    let merger = thread::spawn(move || -> Result<()> {
        let mut builder = RunBuilder::new();
        let mut runs = Vec::with_capacity(BUFFER_LEN);

        while let Some((k, v)) = iter.next()? {
            if let Some(run) = builder.next(k, v.block, v.time) {
                runs.push(run);
                if runs.len() == BUFFER_LEN {
                    tx.send(runs)?;
                    runs = Vec::with_capacity(BUFFER_LEN);
                }
            }
        }

        if let Some(run) = builder.complete() {
            runs.push(run);
        }

        if !runs.is_empty() {
            tx.send(runs)?;
        }

        drop(tx);
        Ok(())
    });

    restorer.superblock_b(&out_sb)?;
    restorer.device_b(&out_dev)?;

    let mut mapped_blocks = 0;
    while let Ok(runs) = rx.recv() {
        for run in &runs {
            restorer.map(run)?;
            mapped_blocks += run.len;
        }
    }

    merger
        .join()
        .expect("unexpected error")
        .expect("metadata contains error");

    restorer.device_e()?;
    restorer.superblock_e()?;
    restorer.eof()?;

    update_device_details(engine_out, mapped_blocks)?;

    Ok(())
}

fn dump_single_device(
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
    sb: &Superblock,
    dev_id: u64,
) -> Result<()> {
    let sm = core_metadata_sm(engine_out.get_nr_blocks(), 2);
    let mut w = WriteBatcher::new(engine_out, sm.clone(), WRITE_BATCH_SIZE);
    let mut restorer = Restorer::new(&mut w, report);

    let roots = btree_to_map::<u64>(&mut vec![], engine_in.clone(), false, sb.mapping_root)?;
    let details =
        btree_to_map::<DeviceDetail>(&mut vec![], engine_in.clone(), false, sb.details_root)?;

    let root = *roots
        .get(&dev_id)
        .ok_or_else(|| anyhow!("Unable to find mapping tree for the origin"))?;
    let details = *details
        .get(&dev_id)
        .ok_or_else(|| anyhow!("Unable to find the details for the origin"))?;

    let mut leaves = collect_leaves(engine_in.clone(), &[root])?;
    let mut iter = MappingIterator::new(engine_in, leaves.remove(&root).unwrap())?;

    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let out_sb = ir::Superblock {
        uuid: "".to_string(),
        time: sb.time,
        transaction: sb.transaction_id,
        flags: None,
        version: Some(sb.version),
        data_block_size: sb.data_block_size,
        nr_data_blocks: data_root.nr_blocks,
        metadata_snap: None,
    };

    let out_dev = ir::Device {
        dev_id: dev_id as u32,
        mapped_blocks: details.mapped_blocks,
        transaction: details.transaction_id,
        creation_time: details.creation_time,
        snap_time: details.snapshotted_time,
    };

    let (tx, rx) = mpsc::sync_channel::<Vec<ir::Map>>(QUEUE_DEPTH);

    let dumper = thread::spawn(move || -> Result<()> {
        let mut builder = RunBuilder::new();
        let mut runs = Vec::with_capacity(BUFFER_LEN);

        while let Some((k, v)) = iter.get() {
            if let Some(run) = builder.next(k, v.block, v.time) {
                runs.push(run);
                if runs.len() == BUFFER_LEN {
                    tx.send(runs)?;
                    runs = Vec::with_capacity(BUFFER_LEN);
                }
            }
            iter.step()?;
        }

        if let Some(run) = builder.complete() {
            runs.push(run);
        }

        if !runs.is_empty() {
            tx.send(runs)?;
        }

        drop(tx);
        Ok(())
    });

    restorer.superblock_b(&out_sb)?;
    restorer.device_b(&out_dev)?;

    while let Ok(runs) = rx.recv() {
        for run in &runs {
            restorer.map(run)?;
        }
    }

    dumper
        .join()
        .expect("unexpected error")
        .expect("metadata contains error");

    restorer.device_e()?;
    restorer.superblock_e()?;
    restorer.eof()?;

    Ok(())
}

//------------------------------------------

pub struct ThinMergeOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub engine_opts: EngineOptions,
    pub report: Arc<Report>,
    pub origin: u64,
    pub snapshot: Option<u64>,
    pub rebase: bool,
}

struct Context {
    report: Arc<Report>,
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &ThinMergeOptions) -> Result<Context> {
    let engine_in = EngineBuilder::new(opts.input, &opts.engine_opts)
        .exclusive(!opts.engine_opts.use_metadata_snap)
        .build()?;

    let mut out_opts = opts.engine_opts.clone();
    out_opts.engine_type = EngineType::Sync; // sync write temporarily
    let engine_out = EngineBuilder::new(opts.output, &out_opts)
        .write(true)
        .build()?;

    Ok(Context {
        report: opts.report.clone(),
        engine_in,
        engine_out,
    })
}

pub fn merge_thins(opts: ThinMergeOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    let sb = if opts.engine_opts.use_metadata_snap {
        let actual_sb = read_superblock(ctx.engine_in.as_ref(), SUPERBLOCK_LOCATION)?;
        if actual_sb.metadata_snap == 0 {
            return Err(anyhow!("no current metadata snap"));
        }
        let mut sb_snap = read_superblock(ctx.engine_in.as_ref(), actual_sb.metadata_snap)?;
        // patch the metadata snapshot to carry the data space map size information
        sb_snap
            .data_sm_root
            .copy_from_slice(&actual_sb.data_sm_root);
        sb_snap
    } else {
        read_superblock(ctx.engine_in.as_ref(), SUPERBLOCK_LOCATION)?
    };

    // ensure the metadata is consistent
    is_superblock_consistent(sb.clone(), ctx.engine_in.clone(), false)?;

    if let Some(snapshot) = opts.snapshot {
        merge(
            ctx.engine_in,
            ctx.engine_out,
            ctx.report,
            &sb,
            opts.origin,
            snapshot,
            opts.rebase,
        )
    } else {
        dump_single_device(ctx.engine_in, ctx.engine_out, ctx.report, &sb, opts.origin)
    }
}

//------------------------------------------
