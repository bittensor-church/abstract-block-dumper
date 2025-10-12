from abstract_block_dumper.decorators import block_task


@block_task(
    condition=lambda bn: True,
)
def process_every_block(block_number: int, netuid: int | None = None):
    return f"Processed block {block_number} for NetUID {netuid}"


@block_task(
    condition=lambda bn: True,
    backfilling_lookback=100,
)
def backfill_previous_100_blocks(block_number: int, netuid: int | None = None):
    return f"Processed block {block_number} for NetUID {netuid}"


@block_task(
    condition=lambda bn, netuid: (bn + netuid) % 50 == 0,
    args=[{"netuid": i} for i in range(10, 15)],  # All subnets
    backfilling_lookback=1000,
    celery_kwargs={"retry": True},
)
def subnet_analysis(block_number, netuid):
    # Analyze subnet data
    return f"Processed block {block_number} for NetUID {netuid}"
