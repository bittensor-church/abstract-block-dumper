"""Management command to create admin superuser if it doesn't exist."""

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """Create admin superuser if it doesn't exist."""

    help = "Create admin superuser with username 'admin' and password 'admin' if it doesn't exist"

    def handle(self, *args, **options):
        """Create the admin user if it doesn't exist."""
        username = "admin"
        password = "admin"
        email = "admin@example.com"

        if User.objects.filter(username=username).exists():
            self.stdout.write(self.style.WARNING(f"Superuser '{username}' already exists."))
        else:
            User.objects.create_superuser(username=username, email=email, password=password)
            self.stdout.write(
                self.style.SUCCESS(f"Superuser '{username}' created successfully with password '{password}'.")
            )
