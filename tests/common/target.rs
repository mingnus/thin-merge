use std::ffi::OsString;

use crate::common::process::*;

//------------------------------------------

pub fn system_cmd<S, I>(cmd: S, args: I) -> Command
where
    S: Into<OsString>,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let all_args = args.into_iter().map(Into::<OsString>::into).collect();
    Command::new(Into::<OsString>::into(cmd), all_args)
}

pub fn thin_merge_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    const RUST_PATH: &str = env!("CARGO_BIN_EXE_thin_merge");
    system_cmd(RUST_PATH, args)
}

pub fn thin_check_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    system_cmd("thin_check", args)
}

pub fn thin_dump_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    system_cmd("thin_dump", args)
}

pub fn thin_repair_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    system_cmd("thin_repair", args)
}

pub fn thin_restore_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    system_cmd("thin_restore", args)
}

pub fn thin_metadata_unpack_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    system_cmd("thin_metadata_unpack", args)
}

//------------------------------------------

pub mod msg {
    pub const FILE_NOT_FOUND: &str = "Couldn't find input file";
    pub const MISSING_INPUT_ARG: &str = "the following required arguments were not provided"; // TODO: be specific
    pub const MISSING_OUTPUT_ARG: &str = "the following required arguments were not provided"; // TODO: be specific
    pub const BAD_SUPERBLOCK: &str = "bad checksum in superblock";

    pub fn bad_option_hint(option: &str) -> String {
        format!("unexpected argument '{}' found", option)
    }
}

//------------------------------------------
