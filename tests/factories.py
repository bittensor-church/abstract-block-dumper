import factory
from django.utils import timezone

from abstract_block_dumper.models import (
    BlockDumperConfig,
    BlockDumperExecution,
    ConditionType,
    NetuidType,
    ScheduledTask,
)


class BlockDumperConfigFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = BlockDumperConfig

    name = factory.Sequence(lambda n: f"test_task_{n}")
    description = factory.Faker("sentence", nb_words=6)
    function_path = factory.Faker("pystr_format", string_format="test.module.function_{{random_int}}")
    condition_type = ConditionType.EVERY_BLOCK
    condition_params = factory.Dict({})
    netuid_type = NetuidType.NONE
    netuid_values = factory.List([])
    queue = "celery"
    max_retries = 3
    retry_backoff = 2
    timeout = None
    is_active = True


class ModuloConfigFactory(BlockDumperConfigFactory):
    condition_type = ConditionType.MODULO
    condition_params = factory.Dict({"modulo": 10, "offset": 0})


class NetuidConfigFactory(BlockDumperConfigFactory):
    netuid_type = NetuidType.MULTIPLE
    netuid_values = factory.List([1, 2, 3])


class AllNetuidConfigFactory(BlockDumperConfigFactory):
    netuid_type = NetuidType.ALL
    netuid_values = factory.List([])


class BlockDumperExecutionFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = BlockDumperExecution

    block_number = factory.Sequence(lambda n: 100 + n)
    total_tasks_scheduled = 1
    tasks_completed = 0
    tasks_failed = 0
    tasks_pending = 1
    started_at = factory.LazyFunction(timezone.now)
    all_completed = False
    has_failures = False


class ScheduledTaskFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = ScheduledTask

    config = factory.SubFactory(BlockDumperConfigFactory)
    block_number = factory.Sequence(lambda n: 100 + n)
    netuid = None
    status = ScheduledTask.Status.PENDING
    retry_count = 0
    max_retries_reached = False


class RunningTaskFactory(ScheduledTaskFactory):
    status = ScheduledTask.Status.RUNNING
    started_at = factory.LazyFunction(timezone.now)
    celery_task_id = factory.Faker("uuid4")


class SuccessTaskFactory(ScheduledTaskFactory):
    status = ScheduledTask.Status.SUCCESS
    started_at = factory.LazyFunction(timezone.now)
    finished_at = factory.LazyFunction(timezone.now)
    result_data = factory.Dict({"result": "success"})


class FailedTaskFactory(ScheduledTaskFactory):
    status = ScheduledTask.Status.FAILED
    started_at = factory.LazyFunction(timezone.now)
    finished_at = factory.LazyFunction(timezone.now)
    error_message = factory.Faker("sentence")
    error_traceback = factory.Faker("text")
    retry_count = 1
