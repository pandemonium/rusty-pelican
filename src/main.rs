use std::io;

use rusty_pelican::core::*;


fn main() -> io::Result<()> {
    let data = tx_log::LoggedTransactions::new(
        domain::ttl::Lifetimes::new(Datasets::default())
    )?;

    println!("Starting ...");
    let mut state = StateContext::new(data);
    state.restore_from_disk()?;

    println!("Running.");
    let run_loop = RunLoop::new(state, "127.0.0.1:8080")?;
    run_loop.execute()
}