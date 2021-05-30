use chrono::prelude::*;
use clap::Clap;
use std::io::{Error, ErrorKind};
use colored::*;
use yahoo_finance_api as yahoo;
use async_trait::async_trait;

#[derive(Clap)]
#[clap(
    version = "0.1",
    author = "Stephen Mizon",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}

struct PriceDifference;
struct MinPrice;
struct MaxPrice;
struct WindowedSMA { 
    window_size: usize 
}

/// A trait to provide a common interface for all signal calculations.
#[async_trait]
trait StockSignal {
    type SignalType;
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType>;
}

#[async_trait]
impl StockSignal for PriceDifference {
    type SignalType = (f64, f64);

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() {
            let (first, last) = (series.first().unwrap(), series.last().unwrap());
            let abs_diff = last - first;
            let first = if *first == 0.0 { 1.0 } else { *first };
            let rel_diff = abs_diff / first;
            Some((abs_diff, rel_diff))
        } else {
            None
        }
    }
}

#[async_trait]
impl StockSignal for MaxPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() {
            Some(series.iter().fold(f64::MIN, |acc, q| acc.max(*q)))
        } else {
            None
        }
    }
}

#[async_trait]
impl StockSignal for MinPrice {
    type SignalType = f64;

    async  fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MAX, |acc, q| acc.min(*q)))
        }
    }
}

#[async_trait]
impl StockSignal for WindowedSMA {
    type SignalType = Vec<f64>;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        let n = self.window_size;
        if !series.is_empty() && n > 1 {
            Some(
                series
                    .windows(n)
                    .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                    .collect(),
            )
        } else {
            None
        }
    }
}


// async fn process(symbol:&str, from: &DateTime<Utc>, to:&DateTime<Utc>, closes:&Vec<f64>) {
//     let min = MinPrice {};
//     let max = MinPrice {};
//     let diffence = PriceDifference {};
//     let sma = WindowedSMA { window_size: 30};

//     // min/max of the period. unwrap() because those are Option types
//     let period_max: f64 = max.calculate(&closes).await?;
//     let period_min: f64 = min.calculate(&closes).await?;
//     let (_, pct_change) = diffence.calculate(&closes).await.unwrap_or((0.0, 0.0));
//     let last_price = *closes.last().await.unwrap_or(&0.0);
//     let sma = sma.calculate(&closes).await.unwrap_or_default();

//     // a simple way to output CSV data
//     println!("period start,symbol,price,change %,min,max,30d avg");
//     println!(
//         "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
//         from.to_rfc3339(),
//         symbol,
//         last_price,
//         pct_change * 100.0,
//         period_min,
//         period_max,
//         sma.last().unwrap_or(&0.0)
//     );
// }

///
/// Retrieve data from a data source and extract the closing prices. 
/// Errors during download are mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();
    let response = provider
        .get_quote_history(symbol, *beginning, *end)
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    let mut quotes = response
        .quotes()
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        Ok(quotes.iter().map(|q| q.adjclose as f64).collect())
    } else {
        Ok(vec![])
    }
}

#[async_std::main]
async fn main() -> std::io::Result<()> {
    
let asci = r"
      /                       \
    /X/                       \X\
   |XX\         _____         /XX|
   |XXX\     _/       \_     /XXX|___________
    \XXXXXXX             XXXXXXX/            \\\
      \XXXX    /     \    XXXXX/                \\\
           |   0     0   |                         \
            |           |                           \
             \         /                            |______//
              \       /                             |
               | O_O | \                            |
                \ _ /   \________________           |
                           | |  | |      \         /
    Data Streaming         / |  / |       \______/
      with Rust            \ |  \ |        \ |  \ |
    (Stock Data)         __| |__| |      __| |__| |
       by Stephen Mizon  |___||___|      |___||___|
    ";
    println!("{}", asci.blue());



    let opts = Opts::parse();
    let from:DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let to = Utc::now();

    // 
    let min = MinPrice {};
    let max = MinPrice {};
    let diffence = PriceDifference {};
    let sma = WindowedSMA { window_size: 30};

    // a simple way to output a CSV header
    println!("period start,symbol,price,change %,min,max,30d avg");
    for symbol in opts.symbols.split(',') {
        //println!("{}", symbol);

        let closes = fetch_closing_data(&symbol, &from, &to).await?;


//        task::block_on(poll_data);

        if !closes.is_empty() {
                // min/max of the period. unwrap() because those are Option types
                let period_max: f64 = max.calculate(&closes).await.unwrap();
                let period_min: f64 = min.calculate(&closes).await.unwrap();
                let last_price = *closes.last().unwrap_or(&0.0);
                let (_, pct_change) = diffence.calculate(&closes).await.unwrap_or((0.0, 0.0));
                let sma = sma.calculate(&closes).await.unwrap_or_default();

            // a simple way to output CSV data
            println!(
                "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
                from.to_rfc3339(),
                symbol,
                last_price,
                pct_change * 100.0,
                period_min,
                period_max,
                sma.last().unwrap_or(&0.0)
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]
    use super::*;

    #[test]
    fn test_PriceDifference_calculate() {
        let signal = PriceDifference {};
        assert_eq!(signal.calculate(&[]), None);
        assert_eq!(signal.calculate(&[1.0]), Some((0.0, 0.0)));
        assert_eq!(signal.calculate(&[1.0, 0.0]), Some((-1.0, -1.0)));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]),
            Some((8.0, 4.0))
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]),
            Some((1.0, 1.0))
        );
    }

    #[test]
    fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]), None);
        assert_eq!(signal.calculate(&[1.0]), Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]), Some(0.0));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]),
            Some(1.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]),
            Some(0.0)
        );
    }

    #[test]
    fn test_MaxPrice_calculate() {
        let signal = MaxPrice {};
        assert_eq!(signal.calculate(&[]), None);
        assert_eq!(signal.calculate(&[1.0]), Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]), Some(1.0));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]),
            Some(10.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]),
            Some(6.0)
        );
    }

    #[test]
    fn test_WindowedSMA_calculate() {
        let series = vec![2.0, 4.5, 5.3, 6.5, 4.7];

        let signal = WindowedSMA { window_size: 3 };
        assert_eq!(
            signal.calculate(&series),
            Some(vec![3.9333333333333336, 5.433333333333334, 5.5])
        );

        let signal = WindowedSMA { window_size: 5 };
        assert_eq!(signal.calculate(&series), Some(vec![4.6]));

        let signal = WindowedSMA { window_size: 10 };
        assert_eq!(signal.calculate(&series), Some(vec![]));
    }
}
