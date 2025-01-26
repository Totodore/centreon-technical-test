use file_processor::count_line_words_concurrent;
use futures_util::{stream, StreamExt};
use tokio::{fs::File, io::BufReader};

#[tokio::main]
async fn main() {
    env_logger::init();
    let files = std::env::args().skip(1).collect::<Vec<_>>();
    let stream = stream::iter(files.iter()).filter_map(open_file);
    let result = count_line_words_concurrent(stream).await;
    dbg!(result);
}

async fn open_file(path: &String) -> Option<(&str, BufReader<File>)> {
    let file = File::open(&path)
        .await
        .inspect_err(|e| log::warn!("Could not open {path}, {e}, skipping it."))
        .ok()?;
    Some((path, BufReader::new(file)))
}
