mod resp;
mod commands;
mod core;
mod datatype;
mod generic;
mod connections;
mod server;
mod ttl;
mod tx_log;
mod snapshots;

use std::io;

fn main() -> Result<(), io::Error> {
    let state = tx_log::LoggedTransactions::new(
        ttl::Lifetimes::new(core::Dataset::empty())
    )?;

    println!("Starting ...");
    let mut domain = core::DomainContext::new(state)?;
    domain.restore()?;

    println!("Running.");
    let run_loop = core::RunLoop::new(domain, "127.0.0.1:8080")?;
    run_loop.execute()
}