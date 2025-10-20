import factory

from abstract_block_dumper.models import TaskAttempt


class TaskAttemptFactory(factory.django.DjangoModelFactory):
    block_number = factory.Sequence(lambda n: n)

    class Meta:
        model = TaskAttempt

    class Params:
        is_pending = factory.Trait(status=TaskAttempt.Status.PENDING)
        is_success = factory.Trait(status=TaskAttempt.Status.RUNNING)
        is_success = factory.Trait(status=TaskAttempt.Status.SUCCESS)
        is_failed = factory.Trait(status=TaskAttempt.Status.FAILED)
