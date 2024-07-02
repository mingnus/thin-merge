use anyhow::Result;

mod common;
mod tools;

use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::output_option::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin_xml_generator::*;
use tools::verifier::*;

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
        &["--origin", "10", "--snapshot", "20"]
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

fn mk_default_xml(path: &std::path::Path) -> Result<()> {
    let content = b"<superblock uuid=\"\" time=\"2\" transaction=\"3\" version=\"2\" data_block_size=\"128\" nr_data_blocks=\"16384\">
  <device dev_id=\"10\" mapped_blocks=\"0\" transaction=\"0\" creation_time=\"0\" snap_time=\"1\">
  </device>
  <device dev_id=\"20\" mapped_blocks=\"0\" transaction=\"0\" creation_time=\"0\" snap_time=\"1\">
  </device>
  <device dev_id=\"30\" mapped_blocks=\"24\" transaction=\"0\" creation_time=\"0\" snap_time=\"1\">
    <range_mapping origin_begin=\"274\" data_begin=\"8440\" length=\"17\" time=\"0\"/>
    <range_mapping origin_begin=\"485\" data_begin=\"15480\" length=\"7\" time=\"0\"/>
  </device>
</superblock>";
    write_file(path, content)
}

// make a tests metadata consists of two thins with ids match that of the required_args.
// TODO: parameterize metadata creation
fn mk_metadata(td: &mut TestDir) -> Result<std::path::PathBuf> {
    let md = mk_zeroed_md(td)?;
    let xml = td.mk_path("meta.xml");
    mk_default_xml(&xml)?;
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

#[test]
fn merge_origin_only() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml_before = td.mk_path("before.xml");
    let xml_after = td.mk_path("after.xml");
    let meta_before = mk_zeroed_md(&mut td)?;
    let meta_after = mk_zeroed_md(&mut td)?;

    let mut s = FragmentedS::new(1, 65536);
    write_xml(&xml_before, &mut s)?;
    run_ok(thin_restore_cmd(args![
        "-i",
        &xml_before,
        "-o",
        &meta_before
    ]))?;
    run_ok(thin_check_cmd(args![&meta_before]))?;

    // the generated thin ids start by 0
    run_ok(thin_merge_cmd(args![
        "-i",
        &meta_before,
        "-o",
        &meta_after,
        "--origin",
        "0"
    ]))?;
    run_ok(thin_check_cmd(args![&meta_after]))?;

    run_ok(thin_dump_cmd(args![&meta_after, "-o", &xml_after]))?;
    assert_eq!(md5(&xml_before)?, md5(&xml_after)?);

    Ok(())
}

// Test merging two thins without shared mappings
#[test]
fn merge_two_thins() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml_before = td.mk_path("before.xml");
    let xml_after = td.mk_path("after.xml");
    let meta_before = mk_zeroed_md(&mut td)?;
    let meta_after = mk_zeroed_md(&mut td)?;

    let mut s = FragmentedS::new(2, 65536);
    write_xml(&xml_before, &mut s)?;
    run_ok(thin_restore_cmd(args![
        "-i",
        &xml_before,
        "-o",
        &meta_before
    ]))?;
    run_ok(thin_check_cmd(args![&meta_before]))?;

    // the generated thin ids start by 0
    run_ok(thin_merge_cmd(args![
        "-i",
        &meta_before,
        "-o",
        &meta_after,
        "--origin",
        "0",
        "--snapshot",
        "1"
    ]))?;
    run_ok(thin_check_cmd(args![&meta_after]))?;

    run_ok(thin_dump_cmd(args![&meta_after, "-o", &xml_after]))?;
    assert!(verify_merge_results(&xml_before, &xml_after, 0, 1, false).is_ok());

    Ok(())
}

// Test merging two snapshots sharing some common blocks.
// This is not the typical use case; it is just to ensure that shared mappings
// are handled properly.
#[test]
fn merge_local_snapshots() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml_before = td.mk_path("before.xml");
    let xml_after = td.mk_path("after.xml");
    let meta_before = mk_zeroed_md(&mut td)?;
    let meta_after = mk_zeroed_md(&mut td)?;

    let mut s = SnapS::new(65536, 2, 20);
    write_xml(&xml_before, &mut s)?;
    run_ok(thin_restore_cmd(args![
        "-i",
        &xml_before,
        "-o",
        &meta_before
    ]))?;
    run_ok(thin_check_cmd(args![&meta_before]))?;

    // the generated thin ids start by 0
    run_ok(thin_merge_cmd(args![
        "-i",
        &meta_before,
        "-o",
        &meta_after,
        "--origin",
        "0",
        "--snapshot",
        "1"
    ]))?;
    run_ok(thin_check_cmd(args![&meta_after]))?;

    run_ok(thin_dump_cmd(args![&meta_after, "-o", &xml_after]))?;
    assert!(verify_merge_results(&xml_before, &xml_after, 0, 1, false).is_ok());

    Ok(())
}

// The scenario where the external snapshot is read-only
#[test]
fn merge_with_empty_snapshot() -> Result<()> {
    let mut td = TestDir::new()?;
    let meta_before = mk_metadata(&mut td)?;
    let meta_after = mk_zeroed_md(&mut td)?;
    let xml_expected = td.mk_path("expected.xml");
    let xml_after = td.mk_path("after.xml");

    run_ok(thin_check_cmd(args![&meta_before]))?;
    run_ok(thin_merge_cmd(args![
        "-i",
        &meta_before,
        "-o",
        &meta_after,
        "--origin",
        "30",
        "--snapshot",
        "20"
    ]))?;
    run_ok(thin_check_cmd(args![&meta_after]))?;

    run_ok(thin_dump_cmd(args![
        &meta_before,
        "--dev-id",
        "30",
        "-o",
        &xml_expected
    ]))?;
    run_ok(thin_dump_cmd(args![&meta_after, "-o", &xml_after]))?;
    assert_eq!(md5(&xml_expected)?, md5(&xml_after)?);

    Ok(())
}

// Corner case test, not a typical use case.
#[test]
fn merge_with_empty_origin() -> Result<()> {
    let mut td = TestDir::new()?;
    let meta_before = mk_metadata(&mut td)?;
    let meta_after = mk_zeroed_md(&mut td)?;
    let xml_expected = td.mk_path("expected.xml");
    let xml_after = td.mk_path("after.xml");

    run_ok(thin_check_cmd(args![&meta_before]))?;
    run_ok(thin_merge_cmd(args![
        "-i",
        &meta_before,
        "-o",
        &meta_after,
        "--origin",
        "20",
        "--snapshot",
        "30"
    ]))?;
    run_ok(thin_check_cmd(args![&meta_after]))?;

    run_ok(thin_dump_cmd(args![
        &meta_before,
        "--dev-id",
        "30",
        "-o",
        &xml_expected
    ]))?;
    run_ok(system_cmd(
        "sed",
        args!["-i", "s/dev_id=\"30\"/dev_id=\"20\"/g", &xml_expected],
    ))?;
    run_ok(thin_dump_cmd(args![&meta_after, "-o", &xml_after]))?;
    assert_eq!(md5(&xml_expected)?, md5(&xml_after)?);

    Ok(())
}

#[test]
fn out_of_metadata_space() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml_before = td.mk_path("before.xml");
    let meta_before = mk_zeroed_md(&mut td)?;
    let meta_after = td.mk_path("meta.bin");
    thinp::file_utils::create_sized_file(&meta_after, 1_048_576)?; // 1MB

    let mut s = FragmentedS::new(2, 131072);
    write_xml(&xml_before, &mut s)?;
    run_ok(thin_restore_cmd(args![
        "-i",
        &xml_before,
        "-o",
        &meta_before
    ]))?;
    run_ok(thin_check_cmd(args![&meta_before]))?;

    // the generated thin ids start by 0
    run_fail(thin_merge_cmd(args![
        "-i",
        &meta_before,
        "-o",
        &meta_after,
        "--origin",
        "0",
        "--snapshot",
        "1"
    ]))?;

    Ok(())
}

//-----------------------------------------
