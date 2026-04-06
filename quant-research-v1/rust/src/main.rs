mod fetcher;
mod storage;

use clap::{Parser, Subcommand};
use anyhow::Result;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "quant-fetcher", about = "Rate-limited data fetcher for quant pipeline")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(long, default_value = "data/quant.duckdb")]
    db: String,

    #[arg(long, env = "FINNHUB_KEY")]
    finnhub_key: Option<String>,

    #[arg(long, env = "FRED_KEY")]
    fred_key: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch Finnhub company news for all symbols
    News {
        #[arg(long)]
        symbols: Vec<String>,
        #[arg(long, default_value = "3")]
        days_back: u32,
    },
    /// Fetch FRED macro series
    Macro {
        #[arg(long)]
        init: bool,
    },
    /// Fetch SEC EDGAR 8-K filings for symbols
    Filings {
        #[arg(long)]
        symbols: Vec<String>,
        #[arg(long, default_value = "14")]
        days_back: u32,
    },
    /// Fetch Polymarket macro event probabilities
    Polymarket,
    /// Fetch earnings calendar from Finnhub
    Earnings {
        #[arg(long)]
        symbols: Vec<String>,
        #[arg(long)]
        init: bool,
    },
    /// Fetch S&P 500 / Nasdaq 100 historical constituent changes
    IndexChanges,
    /// Run all fetchers in sequence
    All {
        #[arg(long)]
        symbols: Vec<String>,
        #[arg(long)]
        init: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("quant_fetcher=info".parse()?))
        .init();

    let cli = Cli::parse();
    let db = storage::duckdb::open(&cli.db)?;

    match cli.command {
        Command::News { symbols, days_back } => {
            let key = cli.finnhub_key.expect("--finnhub-key required for news");
            let n = fetcher::finnhub::fetch_news(&db, &symbols, &key, days_back).await?;
            info!("news fetched rows={}", n);
        }
        Command::Macro { init } => {
            let key = cli.fred_key.expect("--fred-key required for macro");
            let n = fetcher::fred::fetch_macro(&db, &key, init).await?;
            info!("macro fetched rows={}", n);
        }
        Command::Filings { symbols, days_back } => {
            let n = fetcher::sec_edgar::fetch_filings(&db, &symbols, days_back).await?;
            info!("filings fetched rows={}", n);
        }
        Command::Polymarket => {
            let n = fetcher::polymarket::fetch_markets(&db).await?;
            info!("polymarket markets fetched rows={}", n);
        }
        Command::Earnings { symbols, init } => {
            let key = cli.finnhub_key.expect("--finnhub-key required for earnings");
            let n = fetcher::finnhub::fetch_earnings(&db, &symbols, &key, init).await?;
            info!("earnings fetched rows={}", n);
        }
        Command::IndexChanges => {
            let key = cli.finnhub_key.expect("--finnhub-key required for index-changes");
            let n = fetcher::finnhub::fetch_index_changes(&db, &key).await?;
            info!("index changes fetched rows={}", n);
        }
        Command::All { symbols, init } => {
            let finnhub_key = cli.finnhub_key.expect("--finnhub-key required");
            let fred_key = cli.fred_key.expect("--fred-key required");

            // Run Finnhub (rate-limited 60 req/min) and non-Finnhub fetchers concurrently.
            // tokio::join! runs both futures on the same task — &db shared safely.
            // While Finnhub sleeps 1100ms between requests, SEC/FRED/Polymarket make progress.
            let finnhub_fut = async {
                let n1 = match fetcher::finnhub::fetch_news(&db, &symbols, &finnhub_key, 3).await {
                    Ok(n) => { info!("news done rows={}", n); n }
                    Err(e) => { warn!("news fetch failed (non-fatal): {}", e); 0 }
                };
                let n5 = match fetcher::finnhub::fetch_earnings(&db, &symbols, &finnhub_key, init).await {
                    Ok(n) => { info!("earnings done rows={}", n); n }
                    Err(e) => { warn!("earnings fetch failed (non-fatal): {}", e); 0 }
                };
                // index_changes is a premium Finnhub endpoint — non-fatal if it fails
                let n6 = match fetcher::finnhub::fetch_index_changes(&db, &finnhub_key).await {
                    Ok(n) => { info!("index_changes done rows={}", n); n }
                    Err(e) => { warn!("index_changes skipped (premium endpoint?): {}", e); 0 }
                };
                n1 + n5 + n6
            };

            let other_fut = async {
                let n2 = match fetcher::fred::fetch_macro(&db, &fred_key, init).await {
                    Ok(n) => { info!("macro done rows={}", n); n }
                    Err(e) => { warn!("macro fetch failed (non-fatal): {}", e); 0 }
                };
                let n3 = match fetcher::sec_edgar::fetch_filings(&db, &symbols, 14).await {
                    Ok(n) => { info!("filings done rows={}", n); n }
                    Err(e) => { warn!("filings fetch failed (non-fatal): {}", e); 0 }
                };
                let n4 = match fetcher::polymarket::fetch_markets(&db).await {
                    Ok(n) => { info!("polymarket done rows={}", n); n }
                    Err(e) => { warn!("polymarket fetch failed (non-fatal): {}", e); 0 }
                };
                n2 + n3 + n4
            };

            let (finnhub_total, other_total) = tokio::join!(finnhub_fut, other_fut);
            info!("all fetchers complete total_rows={}", finnhub_total + other_total);
        }
    }

    Ok(())
}
