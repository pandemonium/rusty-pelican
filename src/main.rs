mod resp;
mod commands;
mod core;

use std::io::Error;

use crate::core::*;
use crate::core::server::*;


fn main() -> Result<(), Error> {
    let state = PersistentState::make();
    let mut run_loop = RunLoop::make(state, "127.0.0.1:8080")?;
    run_loop.execute()
}