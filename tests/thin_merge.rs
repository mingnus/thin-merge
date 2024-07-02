use anyhow::Result;

mod common;

use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::output_option::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const USAGE: &str = "Merge an external snapshot with its origin into one device

Usage: thin_merge [OPTIONS] --origin <DEV_ID> --input <FILE> --output <FILE>

Options:
  -h, --help               Print help
  -i, --input <FILE>       Specify the input metadata
  -m, --metadata-snap      Use metadata snapshot
  -o, --output <FILE>      Specify the output metadata
      --origin <DEV_ID>    The numeric identifier for the external origin
      --rebase             Choose rebase instead of merge
      --snapshot <DEV_ID>  The numeric identifier for the external snapshot
  -V, --version            Print version";

//------------------------------------------

struct ThinMerge;

impl<'a> Program<'a> for ThinMerge {
    fn name() -> &'a str {
        "thin_merge"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_merge_cmd(args)
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::IoOptions
    }

    fn required_args() -> &'a [&'a str] {
        &["--origin", "12", "--snapshot", "34"]
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

// make a tests metadata consists of two thins with ids match that of the required_args.
// TODO: parameterize metadata creation
fn mk_metadata(td: &mut TestDir) -> Result<std::path::PathBuf> {
    let md = mk_zeroed_md(td)?;

    let xml = td.mk_path("meta.xml");
    let before = b"<superblock uuid=\"\" time=\"2\" transaction=\"3\" version=\"2\" data_block_size=\"128\" nr_data_blocks=\"16384\">
  <device dev_id=\"12\" mapped_blocks=\"0\" transaction=\"0\" creation_time=\"0\" snap_time=\"1\">
  </device>
  <device dev_id=\"34\" mapped_blocks=\"0\" transaction=\"0\" creation_time=\"0\" snap_time=\"1\">
  </device>
</superblock>";
    write_file(&xml, before)?;
    run_ok(thin_restore_cmd(args!["-i", &xml, "-o", &md]))?;

    Ok(md)
}

impl<'a> InputProgram<'a> for ThinMerge {
    fn mk_valid_input(td: &mut TestDir) -> Result<std::path::PathBuf> {
        mk_metadata(td)
    }

    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }

    fn missing_input_arg() -> &'a str {
        msg::MISSING_INPUT_ARG
    }

    fn corrupted_input() -> &'a str {
        msg::BAD_SUPERBLOCK
    }
}

impl<'a> OutputProgram<'a> for ThinMerge {
    fn missing_output_arg() -> &'a str {
        msg::MISSING_OUTPUT_ARG
    }
}

impl<'a> MetadataReader<'a> for ThinMerge {}

impl<'a> MetadataWriter<'a> for ThinMerge {
    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }
}

//-----------------------------------------

test_accepts_help!(ThinMerge);
test_accepts_version!(ThinMerge);
test_rejects_bad_option!(ThinMerge);

test_input_file_not_found!(ThinMerge);
test_input_cannot_be_a_directory!(ThinMerge);
test_corrupted_input_data!(ThinMerge);
test_tiny_input_file!(ThinMerge);
test_help_message_for_tiny_input_file!(ThinMerge);

test_readonly_input_file!(ThinMerge);
test_missing_output_option!(ThinMerge);

//-----------------------------------------
