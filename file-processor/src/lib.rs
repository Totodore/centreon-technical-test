use std::collections::HashMap;

use futures_util::{Stream, StreamExt};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;

pub async fn count_line_words_concurrent<'a, R: AsyncBufRead + Unpin>(
    rds: impl Stream<Item = (&'a str, R)>,
) -> HashMap<&'a str, Vec<usize>> {
    let mut data: HashMap<&'a str, Vec<usize>> = HashMap::new();
    rds.flat_map_unordered(None, |(path, rd)| count_line_words(path, rd))
        .fold(&mut data, |acc, (path, count)| {
            acc.entry(path).or_default().push(count);
            async move { acc }
        })
        .await;

    data
}

/// Returns a stream of the number of words for each line of the input.
pub fn count_line_words<R: AsyncBufRead>(path: &str, rd: R) -> impl Stream<Item = (&str, usize)> {
    LinesStream::new(rd.lines())
        .map(|line| line.unwrap().split_whitespace().count())
        .map(move |itm| (path, itm))
}
