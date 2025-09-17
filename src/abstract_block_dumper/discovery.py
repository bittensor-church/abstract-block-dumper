def ensure_modules_loaded():
    """
    Ensure common tasks modules are imported to trigger @block_task registration.

    @block_task must be loaded, otherwise it won't be registered.
    """
    from django.apps import apps

    for app_config in apps.get_app_configs():
        for module_suffix in ["tasks", "block_tasks"]:
            try:
                __import__(f"{app_config.name}.{module_suffix}")
            except ModuleNotFoundError:
                continue


def sync_block_task_functions(ensure_loaded: bool = True) -> int:
    """
    Sync all registered block dumper functions with the database.
    """
    if ensure_loaded:
        ensure_modules_loaded()

    from .decorators import BlockDumperRegistry
    from .models import BlockDumperConfig

    sync_count = 0
    registrations = BlockDumperRegistry.get_pending_registrations()
    for config_data, _ in registrations:
        BlockDumperConfig.objects.get_or_create(name=config_data["name"], defaults=config_data)
        sync_count += 1
    BlockDumperRegistry.clear_pendings()
    return sync_count
