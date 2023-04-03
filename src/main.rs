mod commands;
mod core;
mod generic;
mod connections;
mod server;
mod globs;

use std::io;

use crate::core::domain::ttl;

fn main() -> Result<(), io::Error> {
    let state = core::tx_log::LoggedTransactions::new(
        ttl::Lifetimes::new(core::Dataset::empty())
    )?;

    println!("Starting ...");
    let mut domain = core::DomainContext::new(state);
    domain.restore()?;

    println!("Running.");
    let run_loop = core::RunLoop::new(domain, "127.0.0.1:8080")?;
    run_loop.execute()
}