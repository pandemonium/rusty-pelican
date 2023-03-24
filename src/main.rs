mod resp;
mod commands;
mod core;
mod datatype;
mod generic;
mod connections;
mod server;
mod ttl;

use std::io;
use std::path;

fn main() -> Result<(), io::Error> {
    let mut state = core::Data::empty();
    state.restore_from_disk(path::Path::new("data.data"))?;
    let run_loop = core::RunLoop::make(state, "127.0.0.1:8080")?;
    run_loop.execute()
}