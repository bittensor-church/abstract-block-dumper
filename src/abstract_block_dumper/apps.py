from django.apps import AppConfig


class AbstractBlockDumperConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "abstract_block_dumper"
    verbose_name = "Abstract Block Dumper"
