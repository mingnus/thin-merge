use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;
use std::vec::Vec;
use thinp::thin::ir::{self, MetadataVisitor, Visit};
use thinp::thin::xml;

//-----------------------------------------

// Analogy to thinp::thin::ir::Superblock
#[derive(Clone, Debug, PartialEq)]
struct ThinSuperblock {
    uuid: String,
    time: u32,
    transaction: u64,
    flags: Option<u32>,
    version: Option<u32>,
    data_block_size: u32,
    nr_data_blocks: u64,
    metadata_snap: Option<u64>,
}

impl ThinSuperblock {
    fn new_from(sb: &ir::Superblock) -> Self {
        Self {
            uuid: sb.uuid.clone(),
            time: sb.time,
            transaction: sb.transaction,
            flags: sb.flags,
            version: sb.version,
            data_block_size: sb.data_block_size,
            nr_data_blocks: sb.nr_data_blocks,
            metadata_snap: sb.metadata_snap,
        }
    }
}

//-----------------------------------------

// Analogy to thinp::thin::ir::Device, with extra trait implementations
#[derive(Clone, Debug, PartialEq)]
struct ThinDevice {
    pub dev_id: u32,
    pub mapped_blocks: u64,
    pub transaction: u64,
    pub creation_time: u32,
    pub snap_time: u32,
}

impl ThinDevice {
    fn new_from(d: &ir::Device) -> Self {
        Self {
            dev_id: d.dev_id,
            mapped_blocks: d.mapped_blocks,
            transaction: d.transaction,
            creation_time: d.creation_time,
            snap_time: d.snap_time,
        }
    }
}

//-----------------------------------------

// Analogy to thinp::thin::ir::Map, with extra trait implementations
#[derive(Clone, Debug, Default, PartialEq)]
struct ThinMap {
    thin_begin: u64,
    data_begin: u64,
    time: u32,
    len: u64,
}

impl ThinMap {
    fn new_from(m: &ir::Map) -> Self {
        Self {
            thin_begin: m.thin_begin,
            data_begin: m.data_begin,
            time: m.time,
            len: m.len,
        }
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn end(&self) -> u64 {
        self.thin_begin + self.len
    }

    fn merge(&mut self, rhs: &ThinMap) -> bool {
        if rhs.thin_begin == self.thin_begin + self.len
            && rhs.data_begin == self.data_begin + self.len
            && rhs.time == self.time
        {
            self.len += rhs.len;
            true
        } else {
            false
        }
    }

    fn split(&self, key: u64) -> (Self, Self) {
        if key <= self.thin_begin {
            return (Self::default(), self.clone());
        } else if key >= self.thin_begin + self.len {
            return (self.clone(), Self::default());
        }

        let lhs = Self {
            thin_begin: self.thin_begin,
            data_begin: self.data_begin,
            time: self.time,
            len: key - self.thin_begin,
        };
        let rhs = Self {
            thin_begin: key,
            data_begin: self.data_begin + lhs.len,
            time: self.time,
            len: self.len - lhs.len,
        };

        (lhs, rhs)
    }
}

trait RangeUtils {
    fn ends_before_started(&self, rhs: &ThinMap) -> bool;
    fn intersects_tail(&self, rhs: &ThinMap) -> bool;
    fn intersects_head(&self, rhs: &ThinMap) -> bool;
}

impl RangeUtils for ThinMap {
    fn ends_before_started(&self, rhs: &ThinMap) -> bool {
        self.thin_begin + self.len <= rhs.thin_begin
    }

    fn intersects_tail(&self, rhs: &ThinMap) -> bool {
        self.thin_begin < rhs.thin_begin
    }

    fn intersects_head(&self, rhs: &ThinMap) -> bool {
        self.thin_begin + self.len < rhs.thin_begin + rhs.len
    }
}

//-----------------------------------------

struct ThinMetadata {
    sb: Option<ThinSuperblock>,
    devices: BTreeMap<u32, ThinDevice>,
    mappings: BTreeMap<u32, Vec<ThinMap>>,
    current_dev: Option<ThinDevice>,
    current_mappings: Vec<ThinMap>,
}

impl ThinMetadata {
    fn new() -> Self {
        Self {
            sb: None,
            devices: BTreeMap::new(),
            mappings: BTreeMap::new(),
            current_dev: None,
            current_mappings: Vec::new(),
        }
    }

    fn new_from(
        sb: ThinSuperblock,
        devices: BTreeMap<u32, ThinDevice>,
        mappings: BTreeMap<u32, Vec<ThinMap>>,
    ) -> Self {
        Self {
            sb: Some(sb),
            devices,
            mappings,
            current_dev: None,
            current_mappings: Vec::new(),
        }
    }
}

impl MetadataVisitor for ThinMetadata {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit> {
        self.sb = Some(ThinSuperblock::new_from(sb));
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, _name: &str) -> Result<Visit> {
        Err(anyhow!("not supported"))
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        Err(anyhow!("not supported"))
    }

    fn device_b(&mut self, d: &ir::Device) -> Result<Visit> {
        self.current_dev = Some(ThinDevice::new_from(d));
        Ok(Visit::Continue)
    }

    fn device_e(&mut self) -> Result<Visit> {
        if let Some(dev) = &self.current_dev {
            let mut mappings = Vec::new();
            std::mem::swap(&mut self.current_mappings, &mut mappings);
            self.devices.insert(dev.dev_id, dev.clone());
            self.mappings.insert(dev.dev_id, mappings);
            Ok(Visit::Continue)
        } else {
            Err(anyhow!("device not found"))
        }
    }

    fn map(&mut self, m: &ir::Map) -> Result<Visit> {
        if self.current_dev.is_some() {
            push_compact(&mut self.current_mappings, &ThinMap::new_from(m));
            Ok(Visit::Continue)
        } else {
            Err(anyhow!("device not found"))
        }
    }

    fn ref_shared(&mut self, _name: &str) -> Result<Visit> {
        Err(anyhow!("not supported"))
    }

    fn eof(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }
}

// Sometimes the mappings from the input source might not be well
// compressed, such as those in the generated xml or from the merger.
// The function helps collect adjacented mappings packed so that they
// could be handled more efficiently.
fn push_compact(dest: &mut Vec<ThinMap>, src: &ThinMap) {
    if let Some(last) = dest.last_mut() {
        if !last.merge(src) {
            dest.push(src.clone());
        }
    } else {
        dest.push(src.clone());
    }
}

//-----------------------------------------

fn parse_xml(path: &Path) -> Result<ThinMetadata> {
    let input = OpenOptions::new().read(true).open(path)?;
    let mut thin_meta = ThinMetadata::new();
    xml::read(input, &mut thin_meta)?;
    Ok(thin_meta)
}

fn merge_mappings(
    origin_mappings: &[ThinMap],
    snap_mappings: &[ThinMap],
) -> Result<(Vec<ThinMap>, u64)> {
    let mut origin_iter = origin_mappings.iter();
    let mut snap_iter = snap_mappings.iter();

    let mut origin_m = origin_iter.next().cloned().unwrap_or_default();
    let mut snap_m = snap_iter.next().cloned().unwrap_or_default();
    let mut merged = Vec::new();
    let mut mapped_blocks = 0;

    while !origin_m.is_empty() && !snap_m.is_empty() {
        if snap_m.ends_before_started(&origin_m) {
            mapped_blocks += snap_m.len;
            push_compact(&mut merged, &snap_m);
            snap_m = snap_iter.next().cloned().unwrap_or_default();
        } else if origin_m.ends_before_started(&snap_m) {
            mapped_blocks += origin_m.len;
            push_compact(&mut merged, &origin_m);
            origin_m = origin_iter.next().cloned().unwrap_or_default();
        } else if origin_m.intersects_tail(&snap_m) {
            let (front, back) = origin_m.split(snap_m.thin_begin);
            mapped_blocks += front.len;
            push_compact(&mut merged, &front);
            origin_m = back;
        } else if snap_m.intersects_head(&origin_m) {
            let (_, back) = origin_m.split(snap_m.end());
            origin_m = back;
            mapped_blocks += snap_m.len;
            push_compact(&mut merged, &snap_m);
            snap_m = snap_iter.next().cloned().unwrap_or_default();
        } else {
            // skip to the next non-fully overlapped range
            while !origin_m.is_empty() && origin_m.end() <= snap_m.end() {
                origin_m = origin_iter.next().cloned().unwrap_or_default();
            }
        }
    }

    while !origin_m.is_empty() {
        mapped_blocks += origin_m.len;
        push_compact(&mut merged, &origin_m);
        origin_m = origin_iter.next().cloned().unwrap_or_default();
    }

    while !snap_m.is_empty() {
        mapped_blocks += snap_m.len;
        push_compact(&mut merged, &snap_m);
        snap_m = snap_iter.next().cloned().unwrap_or_default();
    }

    Ok((merged, mapped_blocks))
}

fn merge_thins(
    source: &ThinMetadata,
    origin: u32,
    snapshot: u32,
    rebase: bool,
) -> Result<ThinMetadata> {
    let origin_mappings = source.mappings.get(&origin).unwrap();
    let snap_mappings = source.mappings.get(&snapshot).unwrap();
    let (merged_mappings, mapped_blocks) = merge_mappings(origin_mappings, snap_mappings)?;

    let mut dev = if rebase {
        source.devices.get(&snapshot).unwrap()
    } else {
        source.devices.get(&origin).unwrap()
    }
    .clone();

    dev.mapped_blocks = mapped_blocks;

    Ok(ThinMetadata::new_from(
        source.sb.clone().unwrap(),
        BTreeMap::from_iter([(dev.dev_id, dev.clone())]),
        BTreeMap::from_iter([(dev.dev_id, merged_mappings)]),
    ))
}

pub fn verify_merge_results(
    xml_before: &Path,
    xml_after: &Path,
    origin: u32,
    snapshot: u32,
    rebase: bool,
) -> Result<()> {
    let meta_before = parse_xml(xml_before)?;
    let meta_after = parse_xml(xml_after)?;

    let merged = merge_thins(&meta_before, origin, snapshot, rebase)?;

    // TODO: log mismatch mappings
    if !merged.sb.eq(&meta_after.sb) {
        return Err(anyhow!("unexpected merged superblock"));
    }
    if !merged.devices.iter().eq(&meta_after.devices) {
        return Err(anyhow!("unexpected merged devices"));
    }
    if !merged.mappings.eq(&meta_after.mappings) {
        return Err(anyhow!("unexpected merged mappings"));
    }

    Ok(())
}

//-----------------------------------------
