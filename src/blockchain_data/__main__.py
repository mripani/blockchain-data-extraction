import argparse
import os
from pathlib import Path
from typing import Optional

from blockchain_data.core import (
    process_data,
    write_delta_table,
)
from blockchain_data.logger import get_logger

logger = get_logger(__name__)


def main(
    datatype: str,
    network: str,
    chain: str,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    delta_time: Optional[str] = None,
    requests_per_second: int = 10,
    max_concurrent_requests: int = 5,
):
    """
    Main orchestration function for blockchain data extraction.

    Args:
        datatype: Cryo datatype to extract
        network: Network identifier (e.g., eth-mainnet, arb-mainnet)
        chain: Chain name (e.g., ethereum, arbitrum)
        start_timestamp: Starting timestamp for backfill mode
        end_timestamp: Ending timestamp for backfill mode
        delta_time: Time delta for backfill mode (e.g., '7d', '1M')
        requests_per_second: Rate limit for RPC requests
        max_concurrent_requests: Max concurrent requests
    """
    # Validate argument combinations
    if delta_time and (start_timestamp or end_timestamp):
        raise ValueError(
            "--delta-time cannot be used with --start-timestamp or --end-timestamp"
        )

    # Load secrets from environment
    alchemy_api_key = os.getenv("ALCHEMY_API_KEY")
    if not alchemy_api_key:
        raise ValueError("ALCHEMY_API_KEY environment variable must be set")

    rpc_url = cf"https://{network}.g.alchemy.com/v2/{alchemy_api_key}"

    # Auto-generate output directory based on datatype
    output_dir = Path(f"./data/{datatype}")

    logger.info("Starting data extraction pipeline")
    logger.info(f"Datatype: {datatype}")
    logger.info(f"Chain: {chain}")
    logger.info(f"Network: {network}")
    logger.info(f"Output directory: {output_dir}")
    logger.info("-" * 60)

    df = process_data(
        datatype=datatype,
        rpc_url=rpc_url,
        output_dir=output_dir,
        chain=chain,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        delta_time=delta_time,
        requests_per_second=requests_per_second,
        max_concurrent_requests=max_concurrent_requests,
    )
    write_delta_table(df, output_dir, mode="append")

    logger.info("-" * 60)
    logger.info("✓ Pipeline completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract EVM blockchain data using Cryo in default format. "
        "Supports all cryo datatypes: blocks, txs, logs, traces, contracts, state, "
        "storage_diffs, balance_diffs, nonce_diffs, code_diffs, vm_traces, etc."
    )
    parser.add_argument(
        "--datatype",
        type=str,
        required=True,
        help="Cryo datatype to extract (e.g., txs, blocks, logs, traces, contracts, "
        "state, storage_diffs, balance_diffs, nonce_diffs, code_diffs, vm_traces)",
    )
    parser.add_argument(
        "--network",
        type=str,
        default="eth-mainnet",
        help="Network identifier (e.g., eth-mainnet, arb-mainnet, default: eth-mainnet)",
    )
    parser.add_argument(
        "--chain",
        type=str,
        default="ethereum",
        help="Chain name (e.g., ethereum, arbitrum, default: ethereum)",
    )
    parser.add_argument(
        "--start-timestamp",
        type=str,
        help="Starting timestamp for backfill mode (e.g., 1609459200, 365d, 12M, 1y)",
    )
    parser.add_argument(
        "--end-timestamp",
        type=str,
        help="Ending timestamp for backfill mode (e.g., 1640995200, latest, 366d)",
    )
    parser.add_argument(
        "--delta-time",
        type=str,
        help="Extract data from the last N time period (e.g., 7d, 2w, 1M, 1y). "
        "Cannot be used with --start-timestamp or --end-timestamp.",
    )
    parser.add_argument(
        "--requests-per-second",
        type=int,
        default=9,
        help="Rate limit for RPC requests per second (default: 25)",
    )
    parser.add_argument(
        "--max-concurrent-requests",
        type=int,
        default=3,
        help="Maximum number of concurrent RPC requests (default: 5)",
    )

    args = parser.parse_args()

    main(
        datatype=args.datatype,
        network=args.network,
        chain=args.chain,
        start_timestamp=args.start_timestamp,
        end_timestamp=args.end_timestamp,
        delta_time=args.delta_time,
        requests_per_second=args.requests_per_second,
        max_concurrent_requests=args.max_concurrent_requests,
    )
