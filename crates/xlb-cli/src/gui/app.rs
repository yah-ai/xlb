use std::collections::VecDeque;

use crate::socket::protocol::{ClassStats, FetchRecord, NodeEvent, NodeInfo};

/// Maximum recent-fetch entries kept in the event tail.
const MAX_RECENT: usize = 20;

pub struct App {
    pub node_info: Option<NodeInfo>,
    pub class_stats: Vec<ClassStats>,
    pub event_tail: VecDeque<EventLine>,
    pub selected: usize,
    pub status: String,
}

/// One line in the event tail at the bottom of the TUI.
pub struct EventLine {
    pub timestamp_secs: u64,
    pub text: String,
    pub ok: bool,
}

/// Messages pumped from background tasks into the TUI's event loop.
pub enum Update {
    NodeInfo(NodeInfo),
    ClassStats(ClassStats),
    Event(NodeEvent),
    Status(String),
}

impl App {
    pub fn new() -> Self {
        Self {
            node_info: None,
            class_stats: vec![],
            event_tail: VecDeque::with_capacity(MAX_RECENT),
            selected: 0,
            status: "connecting…".into(),
        }
    }

    pub fn apply(&mut self, update: Update) {
        match update {
            Update::NodeInfo(info) => {
                self.status = format!(
                    "connected · {} class(es) · uptime {}s",
                    info.classes.len(),
                    info.uptime_secs
                );
                self.node_info = Some(info);
            }

            Update::ClassStats(stats) => {
                if let Some(existing) =
                    self.class_stats.iter_mut().find(|s| s.name == stats.name)
                {
                    *existing = stats;
                } else {
                    self.class_stats.push(stats);
                }
            }

            Update::Event(ev) => {
                let (text, ok, ts) = match &ev {
                    NodeEvent::FetchCompleted { class, hash, bytes, tier, elapsed_ms } => {
                        let hash_short = &hash[..hash.len().min(8)];
                        (
                            format!(
                                "{class:<20} {hash_short}… {bytes:>10} {tier:<8} ok ({elapsed_ms}ms)"
                            ),
                            true,
                            epoch_secs(),
                        )
                    }
                    NodeEvent::FetchFailed { class, hash, reason } => {
                        let hash_short = &hash[..hash.len().min(8)];
                        (
                            format!("{class:<20} {hash_short}… FAILED: {reason}"),
                            false,
                            epoch_secs(),
                        )
                    }
                    NodeEvent::FetchStarted { class, hash } => {
                        let hash_short = &hash[..hash.len().min(8)];
                        (format!("{class:<20} {hash_short}… fetching…"), true, epoch_secs())
                    }
                    NodeEvent::PeerJoined { class, node_id } => {
                        let id_short = &node_id[..node_id.len().min(8)];
                        (format!("{class:<20} peer joined  {id_short}…"), true, epoch_secs())
                    }
                    NodeEvent::PeerLeft { class, node_id } => {
                        let id_short = &node_id[..node_id.len().min(8)];
                        (format!("{class:<20} peer left    {id_short}…"), true, epoch_secs())
                    }
                    NodeEvent::GovernorChanged { class, is_passive } => (
                        format!("{class:<20} governor → {}", if *is_passive { "passive" } else { "active" }),
                        *is_passive,
                        epoch_secs(),
                    ),
                };

                self.push_event(EventLine { timestamp_secs: ts, text, ok });

                // Also update recent_fetches in class_stats
                if let NodeEvent::FetchCompleted { class, hash, bytes, tier, elapsed_ms } = ev {
                    let record = FetchRecord {
                        timestamp_secs: epoch_secs(),
                        class: class.clone(),
                        hash_short: hash[..hash.len().min(8)].to_string(),
                        bytes,
                        tier,
                        ok: true,
                        note: None,
                    };
                    if let Some(s) = self.class_stats.iter_mut().find(|s| s.name == class) {
                        s.recent_fetches.push(record);
                        if s.recent_fetches.len() > 10 {
                            s.recent_fetches.remove(0);
                        }
                    }
                    let _ = elapsed_ms;
                }
            }

            Update::Status(msg) => {
                self.status = msg;
            }
        }
    }

    fn push_event(&mut self, line: EventLine) {
        if self.event_tail.len() >= MAX_RECENT {
            self.event_tail.pop_front();
        }
        self.event_tail.push_back(line);
    }

    pub fn scroll_up(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
        }
    }

    pub fn scroll_down(&mut self) {
        let max = self.class_stats.len().saturating_sub(1);
        if self.selected < max {
            self.selected += 1;
        }
    }
}

fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
