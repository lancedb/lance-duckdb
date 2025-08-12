use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use lance::Dataset;
use tokio::runtime::Runtime;

pub struct LanceReader {
    pub dataset: Arc<Dataset>,
    pub runtime: Arc<Runtime>,
    batches: Mutex<Option<Vec<RecordBatch>>>,
    current_index: Mutex<usize>,
}

impl LanceReader {
    pub fn new(dataset: Arc<Dataset>, runtime: Arc<Runtime>) -> Self {
        Self {
            dataset,
            runtime,
            batches: Mutex::new(None),
            current_index: Mutex::new(0),
        }
    }
    
    pub fn read_next_batch(&self) -> Option<RecordBatch> {
        let mut batches = self.batches.lock().unwrap();
        let mut index = self.current_index.lock().unwrap();
        
        // Load batches on first call
        if batches.is_none() {
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
            *batches = loaded_batches;
        }
        
        // Return next batch if available
        if let Some(ref batch_vec) = *batches {
            if *index < batch_vec.len() {
                let batch = batch_vec[*index].clone();
                *index += 1;
                return Some(batch);
            }
        }
        
        None
    }
}