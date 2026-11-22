//! Persistent isolated private network. No deployment or service management.
#[cfg(not(feature = "private-network"))]
fn main() {
    eprintln!("build with --features private-network");
    std::process::exit(2);
}
#[cfg(feature = "private-network")]
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let result = execute().await;
    if let Err(error) = &result {
        tron_node::critical_notifications::terminal_failure(error);
    }
    result
}
#[cfg(feature = "private-network")]
async fn execute() -> Result<(), Box<dyn std::error::Error>> {
    use tron_node::private_network::Profile;
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: private-node describe MANIFEST | init CONFIG | start CONFIG".into());
    }
    if args[1] == "describe" {
        let p = Profile::load(std::path::Path::new(&args[2]))?;
        println!(
            "{}",
            serde_json::json!({"genesis":hex::encode(p.genesis()?.1.as_bytes()),"profile_hash":hex::encode(p.hash()?)})
        );
        return Ok(());
    }
    let config = tron_node::NodeConfig::from_file(std::path::Path::new(&args[2]))?;
    tron_node::private_network::validate_config(&config)?;
    if args[1] == "init" {
        return tron_node::private_network::initialize(&config);
    }
    if args[1] != "start" {
        return Err("expected describe, init or start".into());
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
