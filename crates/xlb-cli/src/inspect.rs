use anyhow::Result;

use crate::socket::{client::Client, protocol::Response};

pub async fn run(socket_path: &str) -> Result<()> {
    let mut client = Client::connect(socket_path).await?;
    let resp = client.roundtrip(&crate::socket::protocol::Command::Inspect).await?;

    match resp {
        Response::NodeInfo(info) => {
            println!("node_id  : {}", info.node_id);
            println!("uptime   : {}s", info.uptime_secs);
            println!("classes  : {}", info.classes.len());
            for cls in &info.classes {
                let cdn = cls.cdn_fallback.as_deref().unwrap_or("none");
                let seeds = if cls.permanent_seeds.is_empty() {
                    "none".to_string()
                } else {
                    cls.permanent_seeds.join(", ")
                };
                println!("  {:<24} [{}]", cls.name, cls.role);
                println!("    cdn     : {cdn}");
                println!("    seeds   : {seeds}");
            }
        }
        Response::Error { message } => {
            eprintln!("error: {message}");
            std::process::exit(1);
        }
        _ => {
            eprintln!("unexpected response");
            std::process::exit(1);
        }
    }

    Ok(())
}
