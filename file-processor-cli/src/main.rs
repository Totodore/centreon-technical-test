use file_processor::FileProcessorStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    let files = std::env::args().skip(1).collect::<Vec<_>>();
    let result = files
        .iter()
        .map(String::as_str)
        .count_line_words_concurrent()
        .await;
    serde_json::to_writer_pretty(std::io::stdout(), &result)?;
    Ok(())
}
