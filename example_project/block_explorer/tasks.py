from abstract_block_dumper.v1.decorators import block_task


@block_task
def process_every_block(block_number: int, netuid: int | None = None) -> str:
    """
    Example task that processes every block.

    Args:
        block_number (int): The block number to process.
        netuid (int | None): The NetUID to process, if applicable.

    Returns:
        str: A message indicating the block has been processed.

    """
    return f"Processed block {block_number} for NetUID {netuid}"


@block_task(
    backfilling_lookback=100,
)
def backfill_previous_100_blocks(block_number: int, netuid: int | None = None) -> str:
    """
    Example task that backfills the previous 100 blocks.

    Args:
        block_number (int): The block number to process.
        netuid (int | None): The NetUID to process, if applicable.

    Returns:
        str: A message indicating the block has been processed.

    """
    return f"Processed block {block_number} for NetUID {netuid}"


@block_task(
    condition=lambda bn, netuid: (bn + netuid) % 50 == 0,
    args=[{"netuid": i} for i in range(10, 15)],  # All subnets
    backfilling_lookback=1000,
    celery_kwargs={"retry": True},
)
def subnet_analysis(block_number, netuid) -> str:
    # Analyze subnet data
    return f"Processed block {block_number} for NetUID {netuid}"
