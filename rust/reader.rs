use std::sync::Arc;

use arrow::array::RecordBatch;
use lance::Dataset;
use tokio::runtime::Runtime;

pub struct LanceReader {
    pub dataset: Arc<Dataset>,
    pub runtime: Arc<Runtime>,
    batches: Option<Vec<RecordBatch>>,
    current_index: usize,
}

impl LanceReader {
    pub fn new(dataset: Arc<Dataset>, runtime: Arc<Runtime>) -> Self {
        Self {
            dataset,
            runtime,
            batches: None,
            current_index: 0,
        }
    }
    
    pub fn read_next_batch(&mut self) -> Option<RecordBatch> {
        // Load batches on first call
        if self.batches.is_none() {
            let scanner = self.dataset.scan();
            let loaded_batches = self.runtime.block_on(async {
                match scanner.try_into_stream().await {
                    Ok(mut stream) => {
                        use futures::StreamExt;
                        let mut result = Vec::new();
                        while let Some(batch) = stream.next().await {
                            if let Ok(b) = batch {
                                result.push(b);
                            }
                        }
                        if result.is_empty() {
                            None
                        } else {
                            Some(result)
                        }
                    }
                    Err(_) => None,
                }
            });
            self.batches = loaded_batches;
        }
        
        // Return next batch if available
        if let Some(ref batch_vec) = self.batches {
            if self.current_index < batch_vec.len() {
                let batch = batch_vec[self.current_index].clone();
                self.current_index += 1;
                return Some(batch);
            }
        }
        
        None
    }
}