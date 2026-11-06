from django.apps import AppConfig


class AbstractBlockDumperConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "abstract_block_dumper"
    verbose_name = "Abstract Block Dumper"

    def ready(self) -> None:
        import abstract_block_dumper._internal.checks  # noqa: F401, PLC0415  registers the system checks
