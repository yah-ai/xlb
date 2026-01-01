use anyhow::Result;

use crate::socket::{
    client::Client,
    protocol::{Command, Response},
};

pub async fn run(
    class: &str,
    hash: &str,
    out: Option<&str>,
    socket_path: &str,
) -> Result<()> {
    let mut client = Client::connect(socket_path).await?;
    let cmd = Command::Fetch {
        class: class.to_string(),
        hash: hash.to_string(),
        out: out.map(str::to_string),
    };
    let resp = client.roundtrip(&cmd).await?;

    match resp {
        Response::FetchResult(r) => {
            println!(
                "fetched {} bytes via {} in {}ms",
                r.bytes, r.tier, r.elapsed_ms
            );
            if let Some(path) = &r.saved_to {
                println!("saved to: {path}");
            }
        }
        Response::Error { message } => {
            eprintln!("fetch failed: {message}");
            std::process::exit(1);
        }
        _ => {
            eprintln!("unexpected response");
            std::process::exit(1);
        }
    }

    Ok(())
}
