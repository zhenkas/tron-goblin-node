//! Temporary producer/observer entry point. Built explicitly with devnet-fast.
#[cfg(not(feature = "devnet-fast"))]
fn main() {
    eprintln!("build with --features devnet-fast");
    std::process::exit(2);
}

#[cfg(feature = "devnet-fast")]
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 3 {
        return Err("usage: devnet-node init|start CONFIG [GENESIS]".into());
    }
    let config = tron_node::NodeConfig::from_file(std::path::Path::new(&args[2]))?;
    tron_node::devnet::validate(&config)?;
    if args[1] == "init" {
        return tron_node::devnet::initialize(
            &config,
            std::path::Path::new(args.get(3).ok_or("GENESIS required")?),
        );
    }
    if args[1] != "start"
        || (!config.data_dir.join("devnet.json").is_file()
            && !(config.assist.enable
                && config.assist.upstream_peer.is_some()
                && config.assist.bootstrap_genesis.is_some()))
    {
        return Err("initialize a fresh private test ledger before start".into());
    }
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let shutdown = tron_node::ShutdownSignal::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM");
        tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
        signal.shutdown();
    });
    tron_node::run(config, shutdown).await?;
    Ok(())
}
