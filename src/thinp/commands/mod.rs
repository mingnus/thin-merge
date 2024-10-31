pub mod engine;
pub mod utils;

pub trait Command<'a> {
    fn name(&self) -> &'a str;
    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode;
}
