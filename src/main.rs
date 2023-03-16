mod resp;
mod commands;
mod core;

use std::io;
use std::path;


fn main() -> Result<(), io::Error> {
    let mut state = core::PersistentState::make();
    state.restore_from_disk(path::Path::new("data.data"))?;
    let mut run_loop = core::server::RunLoop::make(state, "127.0.0.1:8080")?;
    run_loop.execute()
}