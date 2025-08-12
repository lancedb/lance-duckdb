use std::pin::Pin;

use arrow::array::RecordBatch;
use futures::stream::Stream;
use lance::Dataset;
use tokio::runtime::Runtime;

/// A stream wrapper that holds the Lance RecordBatchStream
pub struct LanceStream {
    runtime: Runtime,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, lance::Error>> + Send>>,
}

impl LanceStream {
    /// Create a new stream from a dataset path
    pub fn new(dataset: &Dataset, runtime: Runtime) -> Result<Self, Box<dyn std::error::Error>> {
        let scanner = dataset.scan();

        let stream = runtime.block_on(async { scanner.try_into_stream().await })?;

        Ok(Self {
            runtime,
            stream: Box::pin(stream),
        })
    }

    /// Get the next batch from the stream
    pub fn next(&mut self) -> Option<RecordBatch> {
        use futures::StreamExt;

        self.runtime.block_on(async {
            match self.stream.next().await {
                Some(Ok(batch)) => Some(batch),
                _ => None,
            }
        })
    }
}
