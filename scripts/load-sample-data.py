#!/usr/bin/env python3
"""
Sample data loader for valkey-timeseries.

Connects to a running Valkey instance and loads sample datasets
from /sample_data/ (or the path specified by SAMPLE_DATA_DIR).

Usage:
    python3 load-sample-data.py [--host HOST] [--port PORT]

Environment variables:
    VALKEY_HOST          Valkey host (default: localhost)
    VALKEY_PORT          Valkey port (default: 6379)
    SAMPLE_DATA_DIR      Path to sample data files (default: /sample_data)
    LOAD_SAMPLE_DATA     Comma-separated list of datasets to load, or "all".
                         Options: cpu, memory, power, web
                         (default: cpu,memory)
"""

import os
import sys
import argparse
import time

# Add the script directory to path so we can import data_helpers
# when running from the repo's tests/ directory.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), 'tests')
if os.path.isdir(TESTS_DIR):
    sys.path.insert(0, TESTS_DIR)

try:
    from valkey import Valkey
except ImportError:
    print("ERROR: 'valkey' Python package is required. Install with: pip install valkey",
          file=sys.stderr)
    sys.exit(1)

try:
    from data_helpers import (
        load_cpu_data,
        load_memory_data,
        load_power_consumption_data,
        load_amazon_web_traffic_data,
        _ingest_samples,
        ingest_power_consumption_data,
        ingest_amazon_web_traffic_data,
    )
    _has_data_helpers = True
except ImportError:
    print("WARNING: data_helpers module not found. Only CPU/memory datasets will be available.",
          file=sys.stderr)
    _has_data_helpers = False


# ── Built-in loaders (no dependency on data_helpers) ────────────────

def _load_text_samples(filepath):
    """Yield (line_index, value) tuples from a text file (one value per line)."""
    with open(filepath, 'r') as f:
        for i, line in enumerate(f):
            value = line.strip()
            if value:
                yield i, float(value)


def load_text_dataset(conn, filepath, key_prefix, metric_name, chunk_size=512):
    """
    Load a simple text dataset (one numeric value per line) into a time series.

    Timestamps are generated starting from (now - N * step) where step is 60s.
    """
    import time as _time
    samples = list(_load_text_samples(filepath))
    if not samples:
        print(f"  No samples found in {filepath}")
        return 0

    total = len(samples)
    now_ms = int(_time.time() * 1000)
    step_ms = 60_000  # 1 minute between samples

    key = f"{key_prefix}:{metric_name}"
    try:
        conn.execute_command('TS.CREATE', key, 'CHUNK_SIZE', chunk_size, 'LABELS', '__name__', metric_name)
    except Exception:
        pass  # key may already exist

    pipe = conn.pipeline(transaction=False)
    count = 0
    batch = 0

    for i, value in samples:
        ts = now_ms - (total - i) * step_ms
        pipe.execute_command('TS.ADD', key, ts, value)
        count += 1
        batch += 1
        if batch >= 500:
            pipe.execute()
            batch = 0
            pipe = conn.pipeline(transaction=False)

    if batch > 0:
        pipe.execute()

    return count


# ── Main loader ─────────────────────────────────────────────────────

def load_all_datasets(conn, data_dir, datasets):
    """Load the requested sample datasets into Valkey."""
    total = 0

    if 'cpu' in datasets:
        cpu_path = os.path.join(data_dir, 'cpu-values.txt')
        if os.path.isfile(cpu_path):
            print("Loading CPU data...")
            n = load_text_dataset(conn, cpu_path, 'system', 'cpu')
            print(f"  Loaded {n} CPU samples")
            total += n
        else:
            print(f"CPU data file not found: {cpu_path}")

    if 'memory' in datasets:
        mem_path = os.path.join(data_dir, 'valkey-memory.txt')
        if os.path.isfile(mem_path):
            print("Loading memory data...")
            n = load_text_dataset(conn, mem_path, 'system', 'memory')
            print(f"  Loaded {n} memory samples")
            total += n
        else:
            print(f"Memory data file not found: {mem_path}")

    if 'power' in datasets:
        pwr_path = os.path.join(data_dir, 'power_consumption.json')
        if os.path.isfile(pwr_path) and _has_data_helpers:
            print("Loading power consumption data...")
            try:
                ingest_power_consumption_data(conn)
                print("  Power consumption data loaded")
                total += 1
            except Exception as e:
                print(f"  Error loading power data: {e}")
        elif not _has_data_helpers:
            print("Skipping power data: data_helpers module not available")
        else:
            print(f"Power data file not found: {pwr_path}")

    if 'web' in datasets:
        web_path = os.path.join(data_dir, 'amazon-web-traffic-dataset.csv')
        if os.path.isfile(web_path) and _has_data_helpers:
            print("Loading Amazon web traffic data (this may take a while)...")
            try:
                ingest_amazon_web_traffic_data(conn)
                print("  Amazon web traffic data loaded")
                total += 1
            except Exception as e:
                print(f"  Error loading web traffic data: {e}")
        elif not _has_data_helpers:
            print("Skipping web traffic data: data_helpers module not available")
        else:
            print(f"Web traffic data file not found: {web_path}")

    return total


def main():
    parser = argparse.ArgumentParser(
        description='Load sample datasets into valkey-timeseries'
    )
    parser.add_argument('--host', default=os.environ.get('VALKEY_HOST', 'localhost'),
                        help='Valkey host (default: localhost)')
    parser.add_argument('--port', default=int(os.environ.get('VALKEY_PORT', '6379')),
                        type=int, help='Valkey port (default: 6379)')
    parser.add_argument('--data-dir', default=os.environ.get('SAMPLE_DATA_DIR', '/data'),
                        help='Path to sample data files')
    parser.add_argument('--datasets', default=os.environ.get('LOAD_SAMPLE_DATA', 'cpu,memory,power_consumption,web'),
                        help='Comma-separated datasets to load: cpu,memory,power,web or "all"')
    args = parser.parse_args()

    # Resolve datasets
    datasets_str = args.datasets.strip().lower()
    if datasets_str == 'all':
        datasets = ['cpu', 'memory', 'power', 'web']
    else:
        datasets = [d.strip() for d in datasets_str.split(',') if d.strip()]

    print(f"Connecting to Valkey at {args.host}:{args.port}...")
    conn = Valkey(host=args.host, port=args.port, decode_responses=True)

    # Wait for Valkey to be ready
    for attempt in range(30):
        try:
            conn.ping()
            break
        except Exception:
            if attempt == 0:
                print("Waiting for Valkey to be ready...")
            time.sleep(1)
    else:
        print("ERROR: Could not connect to Valkey after 30 seconds", file=sys.stderr)
        sys.exit(1)

    print(f"Loading datasets: {', '.join(datasets)}")
    print(f"Data directory: {args.data_dir}")

    total = load_all_datasets(conn, args.data_dir, datasets)
    print(f"\nDone. Total data points loaded: {total}")


if __name__ == '__main__':
    main()
