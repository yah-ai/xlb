use clap::{Parser, Subcommand};

mod config;
mod fetch_cmd;
mod gui;
mod inspect;
mod serve;
mod socket;

#[derive(Parser)]
#[command(name = "xlb", about = "xlb — serve a node, inspect state, fetch blobs, view live TUI")]
struct Cli {
    /// Path to the Unix domain socket used by the running node.
    #[arg(short, long, global = true, default_value_t = default_socket())]
    socket: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a long-lived xlb node daemon (reads xlb-node.json).
    Serve {
        /// Path to the node config file.
        #[arg(short, long, default_value = "xlb-node.json")]
        config: String,
    },
    /// Print current node identity, classes, and governor state (one-shot).
    Inspect,
    /// Fetch a single blob by class name and BLAKE3 hash (one-shot).
    Fetch {
        /// Asset class name (must be registered in the running node).
        class: String,
        /// BLAKE3 hash of the blob to fetch (hex).
        hash: String,
        /// Save fetched bytes to this file path (optional).
        #[arg(short, long)]
        out: Option<String>,
    },
    /// Open the live TUI dashboard (connects to a running node).
    Gui,
}

fn default_socket() -> String {
    std::env::var("XDG_RUNTIME_DIR")
        .map(|d| format!("{d}/xlb-node.sock"))
        .unwrap_or_else(|_| "/tmp/xlb-node.sock".to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("warn".parse().unwrap()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.cmd {
        Cmd::Serve { config } => serve::run(&config, &cli.socket).await,
        Cmd::Inspect => inspect::run(&cli.socket).await,
        Cmd::Fetch { class, hash, out } => {
            fetch_cmd::run(&class, &hash, out.as_deref(), &cli.socket).await
        }
        Cmd::Gui => gui::run(&cli.socket).await,
    }
}
