from typing import TypedDict

from abstract_block_dumper.models import ConditionType


class BlockDumperRequestSchema(TypedDict):
    name: str
    description: str
    function_path: str
    condition_type: ConditionType
    condition_params: dict
    netuid_type: str
    netuid_values: list[int] | int | str | None
    queue: str
    max_retries: int
    retry_backoff: int
    timeout: int | None
    is_active: bool
