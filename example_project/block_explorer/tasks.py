from abstract_block_dumper.shortcuts import every_block, every_n_blocks


@every_block(name="process_every_block")
def process_block(block_number: int, netuid: int | None = None):
    # Placeholder for block processing logic
    print(f"Processing block number: {block_number} NetUID: {netuid}")
    # Here you would add the logic to process the block
    return f"Block {block_number} processed"


@every_n_blocks(n=10, offset=3, name="process_every_10_blocks")
def process_every_10_blocks(block_number: int, netuid: int | None = None):
    # Placeholder for block processing logic every 10 blocks with an offset of 3
    print(f"Processing every 10th block with offset 3: {block_number} NetUID: {netuid}")
    # Here you would add the logic to process the block
    return f"Every 10th Block {block_number} processed"


@every_n_blocks(
    n=3,
    name="process_every_3_blocks",
    netuid=[
        1,
        2,
    ],
)
def process_every_3_blocks(block_number: int, netuid: int | None = None):
    # Placeholder for block processing logic every 3 blocks
    print(f"Processing every 3rd block: {block_number} NetUID: {netuid}")
    # Here you would add the logic to process the block
    return f"Every 3rd Block {block_number} processed"


@every_block(name="randomly_broken_task")
def randomly_broken_task(block_number: int, netuid: int | None = None):
    import random

    if random.random() < 0.3:
        raise ValueError("Simulated random failure")
    print(f"Randomly broken task processed block: {block_number} NetUID: {netuid}")
    return f"Randomly broken task Block {block_number} processed"
