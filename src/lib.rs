#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

pub mod mapping_iterator;
pub mod merge;
pub mod stream;

pub mod thinp;
use thinp::checksum;
use thinp::commands;
use thinp::file_utils;
use thinp::io_engine;
use thinp::ioctl;
use thinp::math;
use thinp::pack;
use thinp::pdata;
use thinp::report;
use thinp::run_iter;
use thinp::thin;
use thinp::write_batcher;
use thinp::xml;
