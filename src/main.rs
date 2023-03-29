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
    let domain = core::DomainContext::new(state)?;
    domain.apply_transaction_log()?;

    println!("main: make run-loop");
    let run_loop = core::RunLoop::make(domain, "127.0.0.1:8080")?;
    run_loop.execute()
}