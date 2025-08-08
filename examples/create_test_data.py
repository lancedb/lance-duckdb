#!/usr/bin/env python3
"""
Create sample Lance datasets for testing the DuckDB extension
"""

import lance
import pyarrow as pa
import numpy as np
import os

def create_local_dataset():
    """Create a local Lance dataset for testing"""
    
    # Create test directory
    os.makedirs("test/data", exist_ok=True)
    
    # Create sample data
    n_rows = 10000
    data = {
        "id": np.arange(n_rows),
        "value": np.random.randn(n_rows),
        "category": np.random.choice(["A", "B", "C", "D"], n_rows),
        "timestamp": pa.array(np.arange(n_rows), type=pa.timestamp('ms')),
        "description": [f"Item {i}" for i in range(n_rows)],
        "vector": [np.random.randn(128).tolist() for _ in range(n_rows)]
    }
    
    # Create Arrow table
    table = pa.table(data)
    
    # Write to Lance format
    lance.write_dataset(table, "test/data/sample.lance", mode="overwrite")
    print("Created local dataset: test/data/sample.lance")
    
    # Create a smaller dataset for quick tests
    small_table = table.slice(0, 100)
    lance.write_dataset(small_table, "test/data/small.lance", mode="overwrite")
    print("Created small dataset: test/data/small.lance")

def create_s3_dataset():
    """Create a Lance dataset on S3 (requires AWS credentials)"""
    
    # Example S3 path - update with your bucket
    s3_path = "s3://your-bucket/lance-test/dataset.lance"
    
    # Create sample data
    n_rows = 100000
    data = {
        "id": np.arange(n_rows),
        "metric": np.random.randn(n_rows),
        "tags": [["tag1", "tag2"] if i % 2 == 0 else ["tag3"] for i in range(n_rows)],
        "payload": [{"key": f"value_{i}"} for i in range(n_rows)]
    }
    
    table = pa.table(data)
    
    # Write to S3 (requires AWS credentials to be configured)
    try:
        lance.write_dataset(table, s3_path, mode="overwrite")
        print(f"Created S3 dataset: {s3_path}")
    except Exception as e:
        print(f"Failed to create S3 dataset: {e}")
        print("Make sure AWS credentials are configured")

if __name__ == "__main__":
    # Create local test datasets
    create_local_dataset()
    
    # Optionally create S3 dataset
    # create_s3_dataset()