# Blockchain Data

A Python library for extracting blockchain data using [cryo](https://github.com/paradigmxyz/cryo) with incremental and backfill support. Data is stored in Delta Lake format with automatic partitioning by block number.

## Features

- **Incremental extraction**: Automatically continues from the last extracted block
- **Backfill mode**: Extract historical data using timestamps or time deltas
- **Delta Lake storage**: Efficient columnar storage with partitioning
- **Retry logic**: Built-in exponential backoff for failed requests
- **Rate limiting**: Configurable RPC request throttling

## Usage

Set your Alchemy API key:
```bash
export ALCHEMY_API_KEY=your_api_key
```

Extract blockchain data using the CLI:
```bash
# Extract blocks from the last 7 days
python -m blockchain_data --datatype blocks --delta-time 7d

# Extract transactions with custom timestamp range
python -m blockchain_data --datatype txs --start-timestamp 1609459200 --end-timestamp latest

# Extract logs from Arbitrum
python -m blockchain_data --datatype logs --network arb-mainnet --chain arbitrum --delta-time 30d
```

The module automatically detects whether to run in incremental or backfill mode based on existing data in the output directory.
