use std::{collections::HashMap, future::Future};

use futures_util::{Stream, StreamExt};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, BufReader},
};
use tokio_stream::wrappers::LinesStream;

/// Extension trait for iterators over file paths.
/// This is a convenience method to count the number of words in each line of each file concurrently.
/// For more control, use the `count_line_words_concurrent` function directly.
pub trait FileProcessorStreamExt<'a>: Iterator<Item = &'a str> + Sized {
    /// Open files concurrently and count the number of words in each line.
    /// Returns a map of paths to a vector of word counts for each line.
    ///
    /// If a file cannot be opened, it will be skipped with a warning log.
    fn count_line_words_concurrent(self) -> impl Future<Output = HashMap<&'a str, Vec<usize>>> {
        let files = futures_util::stream::iter(self).filter_map(open_file);
        count_line_words_concurrent(files)
    }
}
impl<'a, I: Iterator<Item = &'a str>> FileProcessorStreamExt<'a> for I {}

/// Count the number of words from a stream of file readers and associated paths.
/// Returns a map of paths to a vector of word counts for each line.
pub async fn count_line_words_concurrent<'a, R: AsyncBufRead + Unpin>(
    rds: impl Stream<Item = (&'a str, R)>,
) -> HashMap<&'a str, Vec<usize>> {
    let mut data: HashMap<&'a str, Vec<usize>> = HashMap::new();
    rds.flat_map_unordered(None, count_line_words)
        .fold(&mut data, |acc, (path, count)| {
            acc.entry(path).or_default().push(count);
            async move { acc }
        })
        .await;

    data
}

/// Returns a stream of the number of words for each line of the input.
pub fn count_line_words<R: AsyncBufRead>(
    (path, rd): (&str, R),
) -> impl Stream<Item = (&str, usize)> {
    LinesStream::new(rd.lines())
        .map(|line| line.unwrap().split_whitespace().count())
        .map(move |itm| (path, itm))
}

async fn open_file(path: &str) -> Option<(&str, BufReader<File>)> {
    let file = File::open(&path)
        .await
        .inspect_err(|e| log::warn!("Could not open {path}, {e}, skipping it."))
        .ok()?;
    Some((path, BufReader::new(file)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_count_line_words() {
        const DATA: (&str, &str) = (
            "file1.txt",
            r#""Lorem ipsum dolor sit amet,
            consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna\n aliqua.
            Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea\n commodo
            consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu\n fugiat
            nulla pariatur. Excepteur sint occaecat cupidatat non proident\n,
            sunt in culpa qui officia deserunt mollit anim id est laborum.""#,
        );
        let rd = BufReader::new(std::io::Cursor::new(DATA.1));
        let counts: Vec<usize> = count_line_words((DATA.0, rd))
            .map(|(_, count)| count)
            .collect()
            .await;
        assert_eq!(counts, [5, 14, 16, 15, 8, 11]);
    }
}
