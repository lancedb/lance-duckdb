use std::sync::Arc;

use arrow::array::RecordBatch;
use lance::Dataset;
use tokio::runtime::Runtime;

pub struct LanceReader {
    pub dataset: Arc<Dataset>,
    pub runtime: Arc<Runtime>,
}

impl LanceReader {
    pub fn read_next_batch(&self) -> Option<RecordBatch> {
        // Simplified implementation - in production, this should stream batches
        let scanner = self.dataset.scan();
        
        let batches = self.runtime.block_on(async {
            scanner
                .try_into_batch()
                .await
                .ok()
        });
        
        batches
    }
}