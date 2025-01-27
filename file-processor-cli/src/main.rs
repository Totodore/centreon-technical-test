use file_processor::FileProcessorStreamExt;
use futures_util::{stream, StreamExt};
use tokio::{fs::File, io::BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    let files = std::env::args().skip(1).collect::<Vec<_>>();
    let data_stream = stream::iter(files.iter()).filter_map(open_file);
    let result = data_stream.count_line_words_concurrent().await;
    serde_json::to_writer_pretty(std::io::stdout(), &result)?;
    Ok(())
}

async fn open_file(path: &String) -> Option<(&str, BufReader<File>)> {
    let file = File::open(&path)
        .await
        .inspect_err(|e| log::warn!("Could not open {path}, {e}, skipping it."))
        .ok()?;
    Some((path, BufReader::new(file)))
}
