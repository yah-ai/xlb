mod app;
mod ui;

use std::time::Duration;

use anyhow::Result;
use crossterm::event::{Event, KeyCode, KeyEventKind};
use tokio::sync::mpsc;

use crate::socket::{
    client::Client,
    protocol::{Command, Response},
};

use self::app::{App, Update};

pub async fn run(socket_path: &str) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Update>();

    // Task 1: initial inspect + periodic class-stats refresh.
    let socket_owned = socket_path.to_string();
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        poll_loop(socket_owned, tx_clone).await;
    });

    // Task 2: event subscription.
    let socket_owned2 = socket_path.to_string();
    let tx_clone2 = tx.clone();
    tokio::spawn(async move {
        subscribe_loop(socket_owned2, tx_clone2).await;
    });

    // Ratatui terminal setup.
    let mut terminal = ratatui::init();
    terminal.clear()?;

    let mut app = App::new();
    let tick = Duration::from_millis(250);

    loop {
        // Drain all pending updates before drawing.
        while let Ok(upd) = rx.try_recv() {
            app.apply(upd);
        }

        terminal.draw(|f| ui::render(f, &app))?;

        if crossterm::event::poll(tick)? {
            if let Event::Key(key) = crossterm::event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Up | KeyCode::Char('k') => app.scroll_up(),
                    KeyCode::Down | KeyCode::Char('j') => app.scroll_down(),
                    _ => {}
                }
            }
        }
    }

    ratatui::restore();
    Ok(())
}

async fn poll_loop(socket_path: String, tx: mpsc::UnboundedSender<Update>) {
    loop {
        match Client::connect(&socket_path).await {
            Ok(mut client) => {
                // Initial inspect.
                match client.roundtrip(&Command::Inspect).await {
                    Ok(Response::NodeInfo(info)) => {
                        let class_names: Vec<String> =
                            info.classes.iter().map(|c| c.name.clone()).collect();
                        let _ = tx.send(Update::NodeInfo(info));

                        // Fetch stats for each class.
                        for name in &class_names {
                            if let Ok(mut c) = Client::connect(&socket_path).await {
                                if let Ok(Response::ClassStats(stats)) =
                                    c.roundtrip(&Command::ClassStats {
                                        class: name.clone(),
                                    })
                                    .await
                                {
                                    let _ = tx.send(Update::ClassStats(stats));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Update::Status(format!("inspect failed: {e}")));
                    }
                    _ => {}
                }
            }
            Err(e) => {
                let _ = tx.send(Update::Status(format!("disconnected: {e}")));
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn subscribe_loop(socket_path: String, tx: mpsc::UnboundedSender<Update>) {
    loop {
        match Client::connect(&socket_path).await {
            Ok(mut client) => {
                let _ = client
                    .subscribe_events(|resp| {
                        if let Response::Event(ev) = resp {
                            tx.send(Update::Event(ev)).is_ok()
                        } else {
                            true
                        }
                    })
                    .await;
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
