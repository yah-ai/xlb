use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Row, Table, TableState},
    Frame,
};

use super::app::App;

pub fn render(f: &mut Frame, app: &App) {
    let area = f.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),  // title bar
            Constraint::Min(5),     // class table
            Constraint::Length(1),  // governor line
            Constraint::Min(4),     // recent fetches / events
            Constraint::Length(1),  // help line
        ])
        .split(area);

    render_title(f, app, chunks[0]);
    render_classes(f, app, chunks[1]);
    render_governor(f, app, chunks[2]);
    render_events(f, app, chunks[3]);
    render_help(f, chunks[4]);
}

fn render_title(f: &mut Frame, app: &App, area: Rect) {
    let node_id = app
        .node_info
        .as_ref()
        .map(|n| n.node_id[..n.node_id.len().min(12)].to_string())
        .unwrap_or_else(|| "…".to_string());

    let uptime = app
        .node_info
        .as_ref()
        .map(|n| format_uptime(n.uptime_secs))
        .unwrap_or_else(|| "—".to_string());

    let title = format!(" xlb-gui · node:{node_id}… · uptime {uptime} ");
    f.render_widget(
        Paragraph::new(title)
            .style(Style::default().bg(Color::DarkGray).fg(Color::White)),
        area,
    );
}

fn render_classes(f: &mut Frame, app: &App, area: Rect) {
    let header = Row::new(["Class", "Role", "Peers", "Cache", "Budget", "Up", "Down"])
        .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = app
        .class_stats
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let style = if i == app.selected {
                Style::default().bg(Color::Blue).fg(Color::White)
            } else {
                Style::default()
            };

            let cache_str = format_bytes(s.cache_bytes);
            let budget_str = format_bytes(s.cache_budget_bytes);
            let up_str = format_kbps(s.upload_kbps);
            let down_str = format_kbps(s.download_kbps);
            let peers = s.peer_count.to_string();
            let role_sym = match s.role.as_str() {
                "seed" | "permanent" => "[seed]",
                "passive" => "[pass]",
                _ => "[part]",
            };

            Row::new([
                s.name.clone(),
                role_sym.to_string(),
                peers,
                cache_str,
                budget_str,
                up_str,
                down_str,
            ])
            .style(style)
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(7),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(Block::default().title(" Classes ").borders(Borders::ALL));

    let mut state = TableState::default().with_selected(Some(app.selected));
    f.render_stateful_widget(table, area, &mut state);
}

fn render_governor(f: &mut Frame, app: &App, area: Rect) {
    let gov_text = if let Some(stats) = app.class_stats.get(app.selected) {
        let power = if stats.governor.on_battery { "battery" } else { "AC power" };
        let metered = if stats.governor.metered { "metered" } else { "unmetered" };
        let passive = if stats.governor.is_passive { " [PASSIVE]" } else { "" };
        format!(" Governor ({}) : {power} · {metered}{passive}", stats.name)
    } else {
        " Governor: no class selected".to_string()
    };

    f.render_widget(
        Paragraph::new(gov_text)
            .style(Style::default().fg(Color::Yellow)),
        area,
    );
}

fn render_events(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Left: recent fetches for selected class
    let fetch_lines: Vec<Line> = if let Some(stats) = app.class_stats.get(app.selected) {
        stats
            .recent_fetches
            .iter()
            .rev()
            .take(8)
            .map(|r| {
                let color = if r.ok { Color::Green } else { Color::Red };
                let status = if r.ok { "ok" } else { "fail" };
                Line::from(vec![
                    Span::styled(
                        format!("{:<8}", &r.hash_short),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::raw(format!(
                        " {:>10}  {:<8} {}",
                        format_bytes(r.bytes),
                        &r.tier,
                        status
                    )),
                ])
                .style(Style::default().fg(color))
            })
            .collect()
    } else {
        vec![]
    };

    let fetch_title = if let Some(s) = app.class_stats.get(app.selected) {
        format!(" Recent fetches ({}) ", s.name)
    } else {
        " Recent fetches ".to_string()
    };

    f.render_widget(
        Paragraph::new(fetch_lines)
            .block(Block::default().title(fetch_title).borders(Borders::ALL)),
        chunks[0],
    );

    // Right: live event tail
    let event_lines: Vec<Line> = app
        .event_tail
        .iter()
        .rev()
        .take(10)
        .map(|ev| {
            let color = if ev.ok { Color::White } else { Color::Red };
            let ts = format_time(ev.timestamp_secs);
            Line::from(vec![
                Span::styled(format!("{ts} "), Style::default().fg(Color::DarkGray)),
                Span::styled(ev.text.clone(), Style::default().fg(color)),
            ])
        })
        .collect();

    f.render_widget(
        Paragraph::new(event_lines)
            .block(Block::default().title(" Events ").borders(Borders::ALL)),
        chunks[1],
    );
}

fn render_help(f: &mut Frame, area: Rect) {
    f.render_widget(
        Paragraph::new(" q quit  ↑↓ select class  r refresh")
            .style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "—".to_string();
    }
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    const KB: u64 = 1024;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.0} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.0} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn format_kbps(kbps: f64) -> String {
    if kbps == 0.0 {
        return "—".to_string();
    }
    if kbps >= 1024.0 {
        format!("{:.1} MB/s", kbps / 1024.0)
    } else {
        format!("{:.0} KB/s", kbps)
    }
}

fn format_uptime(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

fn format_time(epoch_secs: u64) -> String {
    let total_secs = epoch_secs % 86400;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}
