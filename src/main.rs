use chrono::Utc;
use clap::Parser;
use crossterm::{
    cursor::MoveTo,
    event::{self, Event, KeyCode},
    execute,
    terminal::{Clear, ClearType, disable_raw_mode, enable_raw_mode},
};
use dashmap::DashSet;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders, List, Paragraph},
};
use reqwest::Client;
use rusqlite::{Connection, params};
use scraper::{Html, Selector};
use std::{
    collections::VecDeque,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, mpsc};
use url::Url;
use uuid::Uuid;
use warp::Filter;

/* ================= CLI ================= */

#[derive(Parser, Clone)]
struct Args {
    #[arg(long)]
    url: String,

    #[arg(long, default_value = "./archive")]
    output: String,

    #[arg(long, default_value_t = 0)]
    workers: usize,

    #[arg(long, default_value_t = 24)]
    max_workers: usize,

    #[arg(long, default_value = "both")] // folder | warc | both
    mode: String,

    #[arg(long, default_value_t = 3030)]
    port: u16,
}

/* ================= STATE ================= */

#[derive(Clone)]
struct UiState {
    queue: usize,
    visited: usize,
    active: usize,
    logs: Vec<String>,
}

/* ================= JOB ================= */

#[derive(Clone)]
struct Job {
    url: Url,
}

/* ================= URL → PATH ================= */

fn url_to_path(base: &str, url: &Url) -> PathBuf {
    let mut p = PathBuf::from(base);
    let path = url.path().trim_start_matches('/');

    if path.is_empty() {
        p.push("index.html");
    } else if url.path().ends_with('/') {
        p.push(path);
        p.push("index.html");
    } else {
        p.push(path);
        if !path.contains('.') {
            p.push("index.html");
        }
    }

    p
}

/* ================= WARC ================= */

struct WarcWriter {
    inner: BufWriter<File>,
}

impl WarcWriter {
    fn new(file: File) -> Self {
        Self {
            inner: BufWriter::new(file),
        }
    }

    fn write(&mut self, url: &str, body: &[u8]) {
        let now = Utc::now().to_rfc3339();
        let id = Uuid::new_v4();

        let header = format!(
            "WARC/1.0\r
WARC-Type: response\r
WARC-Date: {}\r
WARC-Record-ID: <urn:uuid:{}>\r
WARC-Target-URI: {}\r
Content-Length: {}\r
\r
HTTP/1.1 200 OK\r
\r
",
            now,
            id,
            url,
            body.len()
        );

        let _ = self.inner.write_all(header.as_bytes());
        let _ = self.inner.write_all(body);
        let _ = self.inner.write_all(b"\r\n\r\n");
    }
}

/* ================= DB ================= */

fn init_db() -> Connection {
    let conn = Connection::open("index.db").unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS captures (
            id TEXT PRIMARY KEY,
            url TEXT,
            ts TEXT
        )",
        [],
    )
    .unwrap();
    conn
}

/* ================= LINKS ================= */

fn extract_links(base: &Url, html: &str) -> Vec<Url> {
    let doc = Html::parse_document(html);
    let sel = Selector::parse("a[href]").unwrap();

    doc.select(&sel)
        .filter_map(|e| e.value().attr("href"))
        .filter_map(|h| base.join(h).ok())
        .collect()
}

/* ================= WORKER ================= */

async fn worker(
    rx: Arc<Mutex<mpsc::Receiver<Job>>>,
    tx: mpsc::Sender<(Url, Vec<u8>)>,
    client: Client,
    visited: Arc<DashSet<String>>,
    domain: String,
) {
    loop {
        let job = {
            let mut rx = rx.lock().await;
            rx.recv().await
        };

        let Some(job) = job else { break };

        if job.url.domain() != Some(domain.as_str()) {
            continue;
        }

        let key = job.url.to_string();
        if !visited.insert(key) {
            continue;
        }

        let resp = match client.get(job.url.clone()).send().await {
            Ok(r) => r,
            Err(_) => continue,
        };

        let bytes = match resp.bytes().await {
            Ok(b) => b.to_vec(),
            Err(_) => continue,
        };

        let _ = tx.send((job.url, bytes)).await;
    }
}

/* ================= WORKER COUNT ================= */

fn resolve_workers(user: usize, max: usize) -> usize {
    if user > 0 {
        user
    } else {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        (cores * 4).min(max).max(4)
    }
}

/* ================= TUI ================= */

async fn run_tui(state: Arc<Mutex<UiState>>) {
    enable_raw_mode().unwrap();

    // CLEAR SCREEN ON START AND FORCE CURSOR TO TOP LEFT

    execute!(std::io::stdout(), Clear(ClearType::All));

    execute!(std::io::stdout(), MoveTo(0, 0));
    let mut stdout = std::io::stdout();
    let backend = CrosstermBackend::new(&mut stdout);
    let mut terminal = Terminal::new(backend).unwrap();

    loop {
        let s = state.lock().await.clone();

        terminal
            .draw(|f| {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(5),
                        Constraint::Length(10),
                        Constraint::Min(5),
                    ])
                    .split(f.area());

                let status = Paragraph::new(format!(
                    "Queue: {}\nVisited: {}\nActive: {}",
                    s.queue, s.visited, s.active
                ))
                .block(Block::default().title("Archive").borders(Borders::ALL));

                let logs = List::new(s.logs.clone())
                    .block(Block::default().title("Logs").borders(Borders::ALL));

                let help =
                    Paragraph::new("Q to quit").block(Block::default().borders(Borders::ALL));

                f.render_widget(status, chunks[0]);
                f.render_widget(logs, chunks[1]);
                f.render_widget(help, chunks[2]);
            })
            .unwrap();

        if event::poll(Duration::from_millis(100)).unwrap() {
            if let Event::Key(k) = event::read().unwrap() {
                if k.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    disable_raw_mode().unwrap();
}

/* ================= MAIN ================= */

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let start = Url::parse(&args.url).unwrap();
    let domain = start.domain().unwrap().to_string();
    let workers = resolve_workers(args.workers, args.max_workers);

    let client = Client::new();

    let (tx_job, rx_job) = mpsc::channel::<Job>(5000);
    let (tx_out, mut rx_out) = mpsc::channel::<(Url, Vec<u8>)>(5000);

    let rx_job = Arc::new(Mutex::new(rx_job));
    let visited = Arc::new(DashSet::new());

    let state = Arc::new(Mutex::new(UiState {
        queue: 0,
        visited: 0,
        active: 0,
        logs: vec![],
    }));

    let mut warc = if args.mode == "warc" || args.mode == "both" {
        Some(WarcWriter::new(File::create("archive.warc").unwrap()))
    } else {
        None
    };

    let db = Arc::new(Mutex::new(init_db()));

    tx_job.send(Job { url: start }).await.unwrap();

    for _ in 0..workers {
        let rx = rx_job.clone();
        let tx = tx_out.clone();
        let client = client.clone();
        let visited = visited.clone();
        let domain = domain.clone();

        tokio::spawn(async move {
            worker(rx, tx, client, visited, domain).await;
        });
    }

    let output = args.output.clone();
    let tx_job2 = tx_job.clone();
    let state_clone = state.clone();

    tokio::spawn(async move {
        while let Some((url, bytes)) = rx_out.recv().await {
            {
                let mut s = state_clone.lock().await;
                s.visited += 1;
            }

            let url_str = url.to_string();

            // folder export
            if args.mode == "folder" || args.mode == "both" {
                let path = url_to_path(&output, &url);
                if let Some(p) = path.parent() {
                    let _ = fs::create_dir_all(p);
                }
                let _ = fs::write(path, &bytes);
            }

            // WARC
            if let Some(w) = warc.as_mut() {
                w.write(&url_str, &bytes);
            }

            // DB
            let db = db.lock().await;
            let _ = db.execute(
                "INSERT INTO captures (id, url, ts) VALUES (?1, ?2, ?3)",
                params![
                    Uuid::new_v4().to_string(),
                    url_str.clone(),
                    Utc::now().to_rfc3339()
                ],
            );

            // crawl more
            if let Ok(html) = String::from_utf8(bytes.clone()) {
                for link in extract_links(&url, &html) {
                    let _ = tx_job2.send(Job { url: link }).await;
                }
            }

            let mut s = state_clone.lock().await;
            s.logs.push(format!("Fetched {}", url_str));
            if s.logs.len() > 8 {
                s.logs.remove(0);
            }
        }
    });

    tokio::spawn(run_tui(state.clone()));

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
