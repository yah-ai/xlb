use anyhow::{Context, Result};
use tokio::net::UnixStream;

use super::{protocol::{Command, Response}, read_frame, write_frame};

/// A short-lived request-response connection to the xlb node control socket.
pub struct Client {
    stream: UnixStream,
}

impl Client {
    pub async fn connect(socket_path: &str) -> Result<Self> {
        let stream = UnixStream::connect(socket_path)
            .await
            .with_context(|| {
                format!(
                    "cannot connect to xlb node at {socket_path}\n\
                     hint: start it with `xlb serve --config xlb-node.json`"
                )
            })?;
        Ok(Self { stream })
    }

    pub async fn send(&mut self, cmd: &Command) -> Result<()> {
        let (_, ref mut w) = self.stream.split();
        write_frame(w, cmd).await
    }

    pub async fn recv(&mut self) -> Result<Response> {
        let (ref mut r, _) = self.stream.split();
        read_frame(r).await
    }

    /// Send a command and receive exactly one response.
    pub async fn roundtrip(&mut self, cmd: &Command) -> Result<Response> {
        self.send(cmd).await?;
        self.recv().await
    }

    /// Send SubscribeEvents and iterate over the incoming event stream.
    /// Calls `f` for each event; stops when `f` returns `false` or the
    /// connection closes.
    pub async fn subscribe_events<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(Response) -> bool,
    {
        self.send(&Command::SubscribeEvents).await?;
        loop {
            match self.recv().await {
                Ok(resp) => {
                    if !f(resp) {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        Ok(())
    }
}
