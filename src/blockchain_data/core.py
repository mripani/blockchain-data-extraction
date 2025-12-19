from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import time

from cryo import collect
import polars as pl

from blockchain_data.helpers import (
    convert_to_timestamp,
    timestamp_to_block,
    parse_delta_time,
    get_latest_block_number_from_delta,
    get_current_block_number,
)
from blockchain_data.logger import get_logger

logger = get_logger(__name__)


def build_block_range(
    rpc_url: str,
    datatype: str,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
) -> tuple[list[str], str]:
    """Build block range for cryo extraction from blocks or timestamps."""
    if start_block is not None and end_block is not None:
        blocks = [f"{start_block}:{end_block}"]
        logger.info(f"Extracting {datatype} from block {start_block} to {end_block}...")
    elif start_timestamp is not None and end_timestamp is not None:
        # Convert timestamps to block numbers
        start_ts = convert_to_timestamp(start_timestamp)
        end_ts = convert_to_timestamp(end_timestamp)

        # Get block numbers from timestamps
        start_block = timestamp_to_block(rpc_url, start_ts)
        end_block = timestamp_to_block(rpc_url, end_ts)

        blocks = [f"{start_block}:{end_block}"]

        logger.info(
            f"Extracting {datatype} from block {start_block} to {end_block}"
            f" (timestamps {start_ts} to {end_ts})..."
        )
    else:
        raise ValueError("Must provide either block range or timestamp range")

    return blocks


def extract_data_with_cryo(
    datatype: str,
    rpc_url: str,
    chain: str = "ethereum",
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    requests_per_second: int = 10,
    max_concurrent_requests: int = 5,
) -> pl.DataFrame:
    """Generic extraction function for any cryo datatype that returns a DataFrame."""
    # Build block range or timestamp range
    blocks = build_block_range(
        rpc_url=rpc_url,
        datatype=datatype,
        start_block=start_block,
        end_block=end_block,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )

    # Call cryo.collect with retry logic
    max_retries = 5
    base_delay = 100  # milliseconds

    for attempt in range(max_retries):
        try:
            df = collect(
                datatype=datatype,
                blocks=blocks,
                rpc=rpc_url,
                network_name=chain,
                requests_per_second=requests_per_second,
                max_concurrent_requests=max_concurrent_requests,
                chunk_size=100,
                no_verbose=False,
            )
            logger.info(f"✓ Collected {datatype} with {len(df)} rows")
            return df
        except Exception as e:
            if attempt < max_retries - 1:
                delay_ms = base_delay * (
                    2**attempt
                )  # Exponential backoff in milliseconds
                logger.warning(
                    f"Cryo collection attempt {attempt + 1}/{max_retries} failed: {e}. "
                    f"Retrying in {delay_ms}ms..."
                )
                time.sleep(delay_ms / 1000)
            else:
                logger.error(
                    f"Cryo collection failed after {max_retries} attempts: {e}"
                )
                raise RuntimeError(
                    f"Cryo collection failed after {max_retries} attempts: {e}"
                )
    return None


def _process_backfill(
    datatype: str,
    rpc_url: str,
    chain: str,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    delta_time: Optional[str] = None,
    requests_per_second: int = 10,
    max_concurrent_requests: int = 5,
) -> pl.DataFrame:
    """
    Process backfill extraction using timestamp ranges or delta time.

    Args:
        datatype: Cryo datatype to extract
        rpc_url: RPC endpoint URL
        chain: Chain name (e.g., 'ethereum', 'arbitrum')
        start_timestamp: Starting timestamp
        end_timestamp: Ending timestamp
        delta_time: Time delta (e.g., '7d', '1M')
        requests_per_second: Rate limit for RPC requests
        max_concurrent_requests: Max concurrent requests

    Returns:
        Polars DataFrame with extracted data
    """
    logger.info("No existing data found, running backfill mode...")

    # Handle delta-time for backfill
    if delta_time:
        logger.info(f"Using delta-time: {delta_time}")
        delta_seconds = parse_delta_time(delta_time)
        end_timestamp = "latest"
        # Calculate a start timestamp as (now - delta)
        now = datetime.now()
        start_dt = now - timedelta(seconds=delta_seconds)
        start_timestamp = str(int(start_dt.timestamp()))
        logger.info(
            f"Calculated time range: {start_timestamp} to {end_timestamp} "
            f"(last {delta_time})"
        )
    elif not start_timestamp or not end_timestamp:
        raise ValueError(
            "Backfill mode requires either --delta-time or both "
            "--start-timestamp and --end-timestamp"
        )

    logger.info(f"Backfill mode: extracting from {start_timestamp} to {end_timestamp}")

    return extract_data_with_cryo(
        datatype=datatype,
        rpc_url=rpc_url,
        chain=chain,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        requests_per_second=requests_per_second,
        max_concurrent_requests=max_concurrent_requests,
    )


def _process_incremental(
    datatype: str,
    rpc_url: str,
    chain: str,
    latest_block: int,
    current_block: int,
    requests_per_second: int = 10,
    max_concurrent_requests: int = 5,
) -> pl.DataFrame:
    """
    Process incremental extraction from latest persisted block to current block.

    Args:
        datatype: Cryo datatype to extract
        rpc_url: RPC endpoint URL
        chain: Chain name (e.g., 'ethereum', 'arbitrum')
        latest_block: Latest block number in existing data
        current_block: Current block number from chain
        requests_per_second: Rate limit for RPC requests
        max_concurrent_requests: Max concurrent requests

    Returns:
        Polars DataFrame with extracted data
    """
    logger.info("Incremental mode: extracting from latest persisted block to current")

    if latest_block >= current_block:
        logger.info(
            f"Already up to date (latest: {latest_block}, current: {current_block})"
        )
        return pl.DataFrame()

    # Extract from the latest + 1 to current
    start_block = latest_block + 1
    logger.info(f"Extracting {datatype} from blocks {start_block} to {current_block}")

    return extract_data_with_cryo(
        datatype=datatype,
        rpc_url=rpc_url,
        chain=chain,
        start_block=start_block,
        end_block=current_block,
        requests_per_second=requests_per_second,
        max_concurrent_requests=max_concurrent_requests,
    )


def process_data(
    datatype: str,
    rpc_url: str,
    output_dir: Path,
    chain: str,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    delta_time: Optional[str] = None,
    requests_per_second: int = 10,
    max_concurrent_requests: int = 5,
) -> pl.DataFrame:
    """
    Process data extraction in both incremental and backfill mode.

    Automatically detects mode based on existing data:
    - If no existing data found: backfill mode using timestamps/delta_time
    - If existing data found: incremental mode from latest block to current

    Args:
        datatype: Cryo datatype to extract
        rpc_url: RPC endpoint URL
        output_dir: Output directory for parquet files
        chain: Chain name (e.g., 'ethereum', 'arbitrum')
        start_timestamp: Starting timestamp (backfill only)
        end_timestamp: Ending timestamp (backfill only)
        delta_time: Time delta (e.g., '7d', '1M') (backfill only)
        requests_per_second: Rate limit for RPC requests
        max_concurrent_requests: Max concurrent requests

    Returns:
        Polars DataFrame with extracted data
    """
    # Check for existing data
    latest_block = get_latest_block_number_from_delta(output_dir)
    current_block = get_current_block_number(rpc_url)

    if latest_block is None:
        # Backfill mode: no existing data
        return _process_backfill(
            datatype=datatype,
            rpc_url=rpc_url,
            chain=chain,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            delta_time=delta_time,
            requests_per_second=requests_per_second,
            max_concurrent_requests=max_concurrent_requests,
        )
    else:
        # Incremental mode: existing data found
        return _process_incremental(
            datatype=datatype,
            rpc_url=rpc_url,
            chain=chain,
            latest_block=latest_block,
            current_block=current_block,
            requests_per_second=requests_per_second,
            max_concurrent_requests=max_concurrent_requests,
        )


def write_delta_table(
    df: pl.DataFrame,
    output_dir: Path,
    mode: str = "append",
) -> None:
    """
    Write a Polars DataFrame to a Delta table partitioned by block_number.

    Args:
        df: Polars DataFrame to write
        output_dir: Output directory for the Delta table
        mode: Write mode - 'append', 'overwrite', or 'error'
    """
    if df.is_empty():
        logger.info("Empty DataFrame, skipping Delta write")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        df.write_delta(
            str(output_dir),
            mode=mode,
            delta_write_options={
                "partition_by": "block_number",
            },
        )
        logger.info(f"✓ Written {len(df)} rows to Delta table at {output_dir}")
    except Exception as e:
        logger.error(f"Delta write failed: {e}")
        raise RuntimeError(f"Delta write failed: {e}")
