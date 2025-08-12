use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatchIterator;
use lance::Dataset;
use lance::dataset::WriteParams;
use tokio::runtime::Runtime;

pub struct LanceWriter {
    path: String,
    schema: Schema,
    batches: Vec<RecordBatch>,
    runtime: Runtime,
}

impl LanceWriter {
    pub fn new(path: &str, schema: Schema) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            path: path.to_string(),
            schema,
            batches: Vec::new(),
            runtime: Runtime::new()?,
        })
    }
    
    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
        self.batches.push(batch);
        Ok(())
    }
    
    pub fn finish(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.batches.is_empty() {
            return Ok(());
        }
        
        let batches = std::mem::take(&mut self.batches);
        let path = self.path.clone();
        let schema = Arc::new(self.schema.clone());
        
        self.runtime.block_on(async move {
            let params = WriteParams::default();
            
            // Convert Vec<RecordBatch> to iterator of Result<RecordBatch, ArrowError>
            let batch_results = batches.into_iter().map(Ok);
            let reader = RecordBatchIterator::new(batch_results, schema);
            
            // Create a new dataset with the batches
            Dataset::write(
                reader,
                &path,
                Some(params),
            ).await
        })?;
        
        Ok(())
    }
}