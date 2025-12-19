from datetime import datetime
from pathlib import Path
from typing import Union, Optional
import re

import requests
import polars as pl

from blockchain_data.logger import get_logger

logger = get_logger(__name__)


def parse_delta_time(delta_str: str) -> int:
    """
    Parse a delta time string (e.g., '7d', '2w', '1M', '1y') and return seconds.

    Supported units:
    - s: seconds
    - m: minutes
    - h: hours
    - d: days
    - w: weeks
    - M: months (30 days)
    - y: years (365 days)
    """
    match = re.match(r"^(\d+)([smhdwMy])$", delta_str)
    if not match:
        raise ValueError(
            f"Invalid delta-time format: '{delta_str}'. "
            "Expected format: <number><unit> (e.g., 7d, 2w, 1M, 1y)"
        )

    value, unit = int(match.group(1)), match.group(2)

    unit_to_seconds = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 604800,
        "M": 2592000,  # 30 days
        "y": 31536000,  # 365 days
    }

    return value * unit_to_seconds[unit]


def timestamp_to_block(rpc_url: str, timestamp: str | int) -> int:
    """Convert a timestamp to a block number using binary search."""
    # Handle "latest" special case
    if timestamp == "latest":
        return get_current_block_number(rpc_url)

    target_ts = int(timestamp)

    # Get the latest block as the upper bound
    response = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
    )
    response.raise_for_status()
    hex_block = response.json()["result"]
    high = int(hex_block, 16)

    # Binary search for a block with the closest timestamp
    low = 0

    logger.info(f"Finding block number for timestamp {target_ts}...")

    while low < high:
        mid = (low + high) // 2

        # Get a block timestamp
        response = requests.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(mid), False],
                "id": 1,
            },
        )
        response.raise_for_status()
        block_data = response.json()["result"]

        if block_data is None:
            high = mid - 1
            continue

        block_ts = int(block_data["timestamp"], 16)

        if block_ts < target_ts:
            low = mid + 1
        else:
            high = mid

    logger.info(f"Found block {low} for timestamp {target_ts}")
    return low


def convert_to_timestamp(value: Union[str, int]) -> Union[str, int]:
    """Convert a timestamp"""
    # Timestamp already
    if isinstance(value, int):
        return value

    # Special values that cryo understands
    if isinstance(value, str):
        if value.lower() in ["latest"]:
            return "latest"
        if value.endswith(("m", "d", "y", "h")):
            raise ValueError(f"Invalid timestamp '{value}'")

    # If it's already a timestamp number (string of digits)
    if value.isdigit():
        return int(value)

    # Try to parse as ISO format datetime and convert to timestamp
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    timestamp = int(dt.timestamp())
    logger.debug(f"Converted datetime '{value}' to timestamp {timestamp}")
    return timestamp


def get_latest_block_number_from_delta(output_dir: Path) -> Optional[int]:
    """Get the latest block number from the existing Delta table."""
    try:
        df = pl.read_delta(output_dir)

        if len(df) == 0:
            logger.info("Delta table exists but is empty")
            return None

        latest_block = df.select(pl.col("block_number").max()).item()
        logger.info(f"Latest block in existing data: {latest_block}")
        return latest_block
    except Exception as e:
        logger.info(f"No existing Delta table found: {e}")
        return None


def get_current_block_number(rpc_url: str) -> int:
    """Get the current block number from the RPC endpoint."""
    response = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
    )
    response.raise_for_status()

    hex_block = response.json()["result"]
    current_block = int(hex_block, 16)
    logger.info(f"Current block number: {current_block}")
    return current_block
