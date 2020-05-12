from django.core.management.base import BaseCommand

from backupman.models import Repository


class Command(BaseCommand):
    help = "Test command"

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        for repo in Repository.objects.all():
            snapshots = repo.sync_snapshots()
            repo.sync_stats()
            for snap in snapshots:
                snap.sync_stats()
                snap.sync_contents()
