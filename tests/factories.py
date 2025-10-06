import factory

from abstract_block_dumper.models import TaskAttempt


class TaskAttemptFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = TaskAttempt

    block_number = factory.Sequence(lambda n: 100 + n)
