use std::{collections::HashMap, future::Future};

use futures_util::{Stream, StreamExt};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;

/// Extension trait for stream over async readers bound to a string identifier.
///
/// This is a convenience method to count the number of words in each line of each data stream concurrently.
/// For more control, use the `count_line_words_concurrent` function directly.
pub trait FileProcessorStreamExt<'a, R>: Stream<Item = (&'a str, R)> + Sized
where
    R: AsyncBufRead + Unpin,
{
    /// Count the number of words from a stream of async readers and associated identifiers.
    /// Returns a map of the identifier to a vector of word counts for each line.
    ///
    /// The readers will be polled concurrently.
    fn count_line_words_concurrent(self) -> impl Future<Output = HashMap<&'a str, Vec<usize>>> {
        count_line_words_concurrent(self)
    }
}
impl<'a, R, S> FileProcessorStreamExt<'a, R> for S
where
    R: AsyncBufRead + Unpin,
    S: Stream<Item = (&'a str, R)>,
{
}

/// Count the number of words from a stream of async readers and associated paths.
/// Returns a map of paths to a vector of word counts for each line.
///
/// The files will be read concurrently.
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
/// The input path will be included in the output.
fn count_line_words<R: AsyncBufRead>((path, rd): (&str, R)) -> impl Stream<Item = (&str, usize)> {
    LinesStream::new(rd.lines())
        .map(|line| line.unwrap().split_whitespace().count())
        .map(move |itm| (path, itm))
}

#[cfg(test)]
mod tests {
    use std::io;

    use futures_util::stream;
    use tokio::io::BufReader;

    use super::*;

    #[tokio::test]
    async fn test_count_line_words_concurrent() {
        const FILE1: &str = include_str!("../../../tests/file1.txt");
        const FILE2: &str = include_str!("../../../tests/file2.txt");
        let iter = [("file1.txt", FILE1), ("file2.txt", FILE2)];
        let stream =
            stream::iter(iter.map(|(path, buff)| (path, BufReader::new(io::Cursor::new(buff)))));
        let result = count_line_words_concurrent(stream).await;
        assert_eq!(result.get("file1.txt").unwrap(), &[2, 3]);
        assert_eq!(result.get("file2.txt").unwrap(), &[2, 2]);
    }

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
        let rd = BufReader::new(io::Cursor::new(DATA.1));
        let counts: Vec<usize> = count_line_words((DATA.0, rd))
            .map(|(_, count)| count)
            .collect()
            .await;
        assert_eq!(counts, [5, 14, 16, 15, 8, 11]);
    }
}
