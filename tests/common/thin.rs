use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use thinp::io_engine::*;
use thinp::pdata::btree_walker::btree_to_map;
use thinp::thin::device_detail::DeviceDetail;

//-----------------------------------------------

pub fn get_superblock(md: &Path) -> Result<thinp::thin::superblock::Superblock> {
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    read_superblock(&engine, SUPERBLOCK_LOCATION)
}

pub fn get_needs_check(md: &Path) -> Result<bool> {
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    Ok(sb.flags.needs_check)
}

pub fn get_metadata_usage(md: &Path) -> Result<(u64, u64)> {
    use thinp::pdata::space_map::common::SMRoot;
    use thinp::pdata::unpack::unpack;
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    let root = unpack::<SMRoot>(&sb.metadata_sm_root)?;
    Ok((root.nr_blocks, root.nr_allocated))
}

pub fn get_data_usage(md: &Path) -> Result<(u64, u64)> {
    use thinp::pdata::space_map::common::SMRoot;
    use thinp::pdata::unpack::unpack;
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    let root = unpack::<SMRoot>(&sb.data_sm_root)?;
    Ok((root.nr_blocks, root.nr_allocated))
}

// FIXME: duplicates of thin::check::get_thins_from_superblock()
pub fn get_thins(md: &Path) -> Result<BTreeMap<u64, (u64, DeviceDetail)>> {
    use thinp::thin::superblock::*;

    let engine: Arc<dyn IoEngine + Send + Sync> = Arc::new(SyncIoEngine::new(md, false)?);
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let devs =
        btree_to_map::<DeviceDetail>(&mut Vec::new(), engine.clone(), false, sb.details_root)?;

    let roots = btree_to_map::<u64>(&mut Vec::new(), engine, false, sb.mapping_root)?;

    let thins = roots
        .into_iter()
        .zip(devs.into_values())
        .map(|((id, root), details)| (id, (root, details)))
        .collect();
    Ok(thins)
}
//-----------------------------------------------
