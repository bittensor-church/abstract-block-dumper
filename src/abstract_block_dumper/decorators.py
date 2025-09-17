import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any

import cloudpickle

from abstract_block_dumper.schemas import BlockDumperRequestSchema

from .models import DEFAULT_MAX_RETRIES, DEFAULT_QUEUE, DEFAULT_RETRY_BACKOFF, ConditionType, NetuidType


class BlockDumperRegistry:
    """
    Register all decorated functions.
    """

    _functions = {}
    _pending_registrations = []

    @classmethod
    def register(cls, config_data: BlockDumperRequestSchema, func: Callable) -> None:
        cls._functions[config_data["name"]] = {
            "function": func,
            "config": config_data,
        }
        cls._pending_registrations.append((config_data, func))

    @classmethod
    def get_pending_registrations(cls) -> list[tuple[dict, Callable]]:
        return cls._pending_registrations

    @classmethod
    def clear_pendings(cls):
        cls._pending_registrations = []


def block_task(
    name: str | None = None,
    condition: Callable | ConditionType = ConditionType.EVERY_BLOCK,
    netuid: int | list[int] | str | None = None,
    queue: str = DEFAULT_QUEUE,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff: int = DEFAULT_RETRY_BACKOFF,
    timeout: int | None = None,
    description: str = "",
    **condition_kwargs,
) -> Callable:
    """
    Register a function to be executed on block events.
    """

    def decorator(func: Callable) -> Callable:
        # validate signature
        _validate_function_signature(func)

        executable_path = ".".join([func.__module__, func.__name__])
        config_name = name or executable_path

        config_data: BlockDumperRequestSchema = {
            "name": config_name,
            "description": description,
            "function_path": executable_path,
            "condition_type": _determine_condition_type(condition),
            "condition_params": _build_condition_params(condition, condition_kwargs),
            "netuid_type": _determine_netuid_type(netuid),
            "netuid_values": _build_netuid_values(netuid),
            "queue": queue,
            "max_retries": max_retries,
            "retry_backoff": retry_backoff,
            "is_active": True,
            "timeout": timeout,
        }

        BlockDumperRegistry.register(config_data, func)

        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            return func(*args, **kwargs)

        setattr(wrapper, "_block_dumper_config", config_data)
        return wrapper

    return decorator


def _validate_function_signature(func) -> None:
    signature = inspect.signature(func)
    params = list(signature.parameters.keys())

    if "block_number" not in params:
        raise ValueError(f"Function {func.__name__} must have 'block_number' parameter.")

    block_number = signature.parameters["block_number"]
    if block_number.annotation not in (int, inspect.Parameter.empty):
        raise ValueError("block_number parameter should be annotated as int")

    if "netuid" in params:
        netuid_parameter = signature.parameters["netuid"]
        if netuid_parameter.default is inspect.Parameter.empty:
            raise ValueError("netuid parameter should have default value of None")


def _determine_condition_type(condition: Callable | ConditionType) -> ConditionType:
    if isinstance(condition, ConditionType):
        return condition
    elif callable(condition):
        return ConditionType.CUSTOM
    else:
        raise ValueError("Condition must be a string or callable")


def _build_condition_params(condition: Callable | ConditionType, condition_kwargs: dict) -> dict:
    params = condition_kwargs.copy()
    if isinstance(condition, ConditionType):
        if condition == ConditionType.MODULO:
            if "modulo" not in params:
                raise ValueError("'modulo' condition requires 'modulo' parameter")
        elif condition in [ConditionType.EPOCH_START, ConditionType.EPOCH_MIDDLE, ConditionType.EPOCH_END]:
            params.setdefault("tempo", 360)
    elif callable(condition):
        params["function_bytes"] = cloudpickle.dumps(condition)
    return params


def _determine_netuid_type(netuid: int | list[int] | str | None) -> str:
    if netuid is None:
        return NetuidType.NONE
    elif isinstance(netuid, list):
        return NetuidType.MULTIPLE
    elif isinstance(netuid, int):
        return NetuidType.SINGLE
    elif netuid == "all":
        return NetuidType.ALL
    else:
        raise ValueError("netuid must be int, list of int, str or None")


def _build_netuid_values(netuid: int | list[int] | str | None) -> list:
    if netuid is None or netuid == "all":
        return []
    elif isinstance(netuid, list):
        return netuid
    elif isinstance(netuid, int):
        return [netuid]
    else:
        raise ValueError("netuid must be int, list of int, str or None")
