use anyhow::{anyhow, Result};
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
use thinp::pdata::space_map::NoopSpaceMap;
use thinp::pdata::unpack::unpack;
use thinp::report::Report;
use thinp::thin::block_time::*;
use thinp::thin::device_detail::DeviceDetail;
use thinp::thin::ir::{self, MetadataVisitor};
use thinp::thin::metadata_repair::is_superblock_consistent;
use thinp::thin::restore::Restorer;
use thinp::thin::superblock::*;
use thinp::write_batcher::WriteBatcher;

use crate::mapping_iterator::MappingIterator;
use crate::stream::*;

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

fn collect_leaves(engine: Arc<dyn IoEngine + Send + Sync>, root: u64) -> Result<Vec<u64>> {
    // Using NoopSpaceMap is sufficient as the ref counts are irrelevant in this case.
    // Also, The LeafWalker ignores the ref counts in space map and walks visited nodes anyway.
    let mut sm = NoopSpaceMap::new(engine.get_nr_blocks());

    let mut w = LeafWalker::new(engine.clone(), &mut sm, false);
    let mut v = CollectLeaves::new();
    let mut path = vec![0];
    w.walk::<CollectLeaves, BlockTime>(&mut path, &mut v, root)?;

    Ok(v.leaves)
}

//------------------------------------------

struct RangeMergeIterator {
    base_stream: MappingStream,
    snap_stream: MappingStream,
}

impl RangeMergeIterator {
    fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        base_root: u64,
        snap_root: u64,
    ) -> Result<Self> {
        let base_leaves = collect_leaves(engine.clone(), base_root)?;
        let snap_leaves = collect_leaves(engine.clone(), snap_root)?;
        let base_stream = MappingStream::new(engine.clone(), base_leaves)?;
        let snap_stream = MappingStream::new(engine, snap_leaves)?;

        Ok(Self {
            base_stream,
            snap_stream,
        })
    }

    fn ends_before_started(left: &(u64, BlockTime, u64), right: &(u64, BlockTime, u64)) -> bool {
        left.0 + left.2 <= right.0
    }

    fn overlays_tail(base: &(u64, BlockTime, u64), overlay: &(u64, BlockTime, u64)) -> bool {
        base.0 < overlay.0
    }

    fn overlays_head(base: &(u64, BlockTime, u64), overlay: &(u64, BlockTime, u64)) -> bool {
        overlay.0 + overlay.2 < base.0 + base.2
    }

    fn overlays_all(base: &(u64, BlockTime, u64), overlay: &(u64, BlockTime, u64)) -> bool {
        base.0 + base.2 <= overlay.0 + overlay.2
    }

    fn next(&mut self) -> Result<Option<(u64, BlockTime, u64)>> {
        while self.base_stream.more_mappings() && self.snap_stream.more_mappings() {
            let mut base_map = self.base_stream.get_mapping().unwrap();
            let snap_map = self.snap_stream.get_mapping().unwrap();

            if Self::ends_before_started(snap_map, base_map) {
                return self.snap_stream.consume_all();
            } else if Self::ends_before_started(base_map, snap_map) {
                return self.base_stream.consume_all();
            } else if Self::overlays_tail(base_map, snap_map) {
                let delta = snap_map.0 - base_map.0;
                return self.base_stream.consume(delta);
            } else if Self::overlays_head(base_map, snap_map) {
                let intersected = snap_map.0 + snap_map.2 - base_map.0;
                self.base_stream.skip(intersected)?;
                return self.snap_stream.consume(snap_map.2);
            } else {
                while Self::overlays_all(base_map, snap_map) {
                    self.base_stream.skip_all()?;
                    if !self.base_stream.more_mappings() {
                        break;
                    }
                    base_map = self.base_stream.get_mapping().unwrap();
                }
            }
        }

        if self.base_stream.more_mappings() {
            return self.base_stream.consume_all();
        }

        if self.snap_stream.more_mappings() {
            return self.snap_stream.consume_all();
        }

        Ok(None)
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
    out_sb: &ir::Superblock,
    out_dev: &ir::Device,
    origin_root: u64,
    snap_root: u64,
) -> Result<()> {
    let sm = core_metadata_sm(engine_out.get_nr_blocks(), 2);
    let mut w = WriteBatcher::new(engine_out.clone(), sm.clone(), WRITE_BATCH_SIZE);
    let mut restorer = Restorer::new(&mut w, report);

    let mut iter = RangeMergeIterator::new(engine_in.clone(), origin_root, snap_root)?;

    let (tx, rx) = mpsc::sync_channel::<Vec<ir::Map>>(QUEUE_DEPTH);

    let merger = thread::spawn(move || -> Result<()> {
        let mut runs = Vec::with_capacity(BUFFER_LEN);

        while let Some((k, v, l)) = iter.next()? {
            runs.push(ir::Map {
                thin_begin: k,
                data_begin: v.block,
                time: v.time,
                len: l,
            });
            if runs.len() == BUFFER_LEN {
                tx.send(runs)?;
                runs = Vec::with_capacity(BUFFER_LEN);
            }
        }

        if !runs.is_empty() {
            tx.send(runs)?;
        }

        drop(tx);
        Ok(())
    });

    restorer.superblock_b(out_sb)?;
    restorer.device_b(out_dev)?;

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
    out_sb: &ir::Superblock,
    out_dev: &ir::Device,
    root: u64,
) -> Result<()> {
    let sm = core_metadata_sm(engine_out.get_nr_blocks(), 2);
    let mut w = WriteBatcher::new(engine_out, sm.clone(), WRITE_BATCH_SIZE);
    let mut restorer = Restorer::new(&mut w, report);

    let leaves = collect_leaves(engine_in.clone(), root)?;
    let mut iter = MappingIterator::new(engine_in, leaves)?;

    let (tx, rx) = mpsc::sync_channel::<Vec<ir::Map>>(QUEUE_DEPTH);

    let dumper = thread::spawn(move || -> Result<()> {
        let mut runs = Vec::with_capacity(BUFFER_LEN);

        while let Some((k, v, l)) = iter.next_range()? {
            runs.push(ir::Map {
                thin_begin: k,
                data_begin: v.block,
                time: v.time,
                len: l,
            });
            if runs.len() == BUFFER_LEN {
                tx.send(runs)?;
                runs = Vec::with_capacity(BUFFER_LEN);
            }
        }

        if !runs.is_empty() {
            tx.send(runs)?;
        }

        drop(tx);
        Ok(())
    });

    restorer.superblock_b(out_sb)?;
    restorer.device_b(out_dev)?;

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

fn read_patched_superblock_snap(engine: &dyn IoEngine) -> Result<Superblock> {
    // here we don't use read_superblock_snap() as we need both the main superblock and the
    // metadata snapshot.
    let actual_sb = read_superblock(engine, SUPERBLOCK_LOCATION)?;
    if actual_sb.metadata_snap == 0 {
        return Err(anyhow!("no current metadata snap"));
    }
    let mut sb_snap = read_superblock(engine, actual_sb.metadata_snap)?;

    // patch the metadata snapshot to carry the data space map size information
    sb_snap
        .data_sm_root
        .copy_from_slice(&actual_sb.data_sm_root);

    Ok(sb_snap)
}

fn get_device_root_and_details(
    dev_id: u64,
    roots: &BTreeMap<u64, u64>,
    details: &BTreeMap<u64, DeviceDetail>,
) -> Result<(u64, DeviceDetail)> {
    let root = *roots
        .get(&dev_id)
        .ok_or_else(|| anyhow!("Unable to find mapping tree for the device {}", dev_id))?;
    let details = *details
        .get(&dev_id)
        .ok_or_else(|| anyhow!("Unable to find the details for the device {}", dev_id))?;
    Ok((root, details))
}

fn build_output_superblock(sb: &Superblock) -> Result<ir::Superblock> {
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    Ok(ir::Superblock {
        uuid: "".to_string(),
        time: sb.time,
        transaction: sb.transaction_id,
        flags: None,
        version: Some(sb.version),
        data_block_size: sb.data_block_size,
        nr_data_blocks: data_root.nr_blocks,
        metadata_snap: None,
    })
}

fn build_output_device(dev_id: u64, details: &DeviceDetail) -> ir::Device {
    ir::Device {
        dev_id: dev_id as u32,
        mapped_blocks: details.mapped_blocks,
        transaction: details.transaction_id,
        creation_time: details.creation_time,
        snap_time: details.snapshotted_time,
    }
}

fn merge_thins_(
    ctx: Context,
    sb: &Superblock,
    origin_id: u64,
    snap_id: Option<u64>,
    rebase: bool,
) -> Result<()> {
    let out_sb = build_output_superblock(sb)?;

    let roots = btree_to_map::<u64>(&mut vec![], ctx.engine_in.clone(), false, sb.mapping_root)?;
    let details =
        btree_to_map::<DeviceDetail>(&mut vec![], ctx.engine_in.clone(), false, sb.details_root)?;

    let (origin_root, origin_details) = get_device_root_and_details(origin_id, &roots, &details)?;

    if let Some(snap_id) = snap_id {
        let (snap_root, snap_details) = get_device_root_and_details(snap_id, &roots, &details)?;

        let out_dev = if rebase {
            build_output_device(snap_id, &snap_details)
        } else {
            build_output_device(origin_id, &origin_details)
        };

        if origin_root == snap_root {
            // fallback to dump a single device
            dump_single_device(
                ctx.engine_in,
                ctx.engine_out,
                ctx.report,
                &out_sb,
                &out_dev,
                origin_root,
            )
        } else {
            merge(
                ctx.engine_in,
                ctx.engine_out,
                ctx.report,
                &out_sb,
                &out_dev,
                origin_root,
                snap_root,
            )
        }
    } else {
        let out_dev = build_output_device(origin_id, &origin_details);

        dump_single_device(
            ctx.engine_in,
            ctx.engine_out,
            ctx.report,
            &out_sb,
            &out_dev,
            origin_root,
        )
    }
}

pub fn merge_thins(opts: ThinMergeOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    let sb = if opts.engine_opts.use_metadata_snap {
        read_patched_superblock_snap(ctx.engine_in.as_ref())?
    } else {
        read_superblock(ctx.engine_in.as_ref(), SUPERBLOCK_LOCATION)?
    };

    // ensure the metadata is consistent
    is_superblock_consistent(sb.clone(), ctx.engine_in.clone(), false)?;

    merge_thins_(ctx, &sb, opts.origin, opts.snapshot, opts.rebase)
}

//------------------------------------------
