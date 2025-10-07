import importlib

import structlog

logger = structlog.get_logger(__name__)


def ensure_modules_loaded() -> None:
    """
    Ensure common tasks modules are imported to trigger @block_task registration.

    @block_task must be loaded, otherwise it won't be registered.
    """
    from django.apps import apps

    for app_config in apps.get_app_configs():
        for module_suffix in ["tasks", "block_tasks"]:
            try:
                importlib.import_module(f"{app_config.name}.{module_suffix}")
            except ModuleNotFoundError:
                continue
            except ImportError as e:
                logger.warning(f"Failed to import {app_config.name}.{module_suffix}: {e}")
                continue
