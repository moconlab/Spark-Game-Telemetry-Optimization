import os
import pandas as pd
from src.ingestion import generate_events

def test_generate_events_creates_file(tmp_path):
    # Use a temporary directory for test
    output_dir = tmp_path / "data"
    output_dir.mkdir()
    
    # Generate one batch
    generate_events.EVENTS_PER_BATCH = 100  # small number for testing
    generate_events.OUTPUT_DIR = str(output_dir)
    generate_events.generate_batch(batch_id=1)
    
    # Check if file exists
    file_path = output_dir / "batch_1.csv"
    assert file_path.exists()
    
    # Check file has correct columns
    df = pd.read_csv(file_path)
    expected_columns = ["player_id", "match_id", "event_type", "timestamp", "region", "latency_ms"]
    assert all(col in df.columns for col in expected_columns)
    assert len(df) == 100