import logging

from django_better_admin_arrayfield.models.fields import ArrayField
from polymorphic.models import PolymorphicModel

from django.db import models
from django.utils import timezone

from backupman.resticlib import ResticWrapper
from backupman.utils.bulk_sync import bulk_sync
from backupman.utils.misc import humansize

logger = logging.getLogger(__name__)


class Repository(PolymorphicModel):
    description = models.CharField(max_length=255, blank=True)
    password = models.CharField(max_length=255)
    total_size = models.BigIntegerField(null=True)
    total_file_count = models.IntegerField(null=True)
    created_on = models.DateTimeField(auto_now_add=True)
    fetched_on = models.DateTimeField(null=True)

    class Meta:
        verbose_name = "Repository"
        verbose_name_plural = "Repositories"

    def __str__(self):
        return self.description or self.repository_uri()

    def type(self):
        return self.polymorphic_ctype

    type.admin_order_field = "polymorphic_ctype"

    def repository_uri(self):
        return ""

    def environment(self):
        return {}

    def human_size(self):
        return self.total_size and humansize(self.total_size)

    human_size.admin_order_field = "total_size"
    human_size.short_description = "Total size"

    @property
    def restic(self):
        return ResticWrapper(self.repository_uri(), self.password, self.environment())

    def sync(self, full=False):
        snapshots = self.sync_snapshots()
        self.sync_stats()
        for snap in snapshots:
            snap.sync(full)

    def sync_snapshots(self):
        logger.info("Syncing '%s' snapshots", self)
        snapshots = self.restic.snapshots()
        snapshots = [Snapshot.from_restic(s, self) for s in snapshots]
        exclude = ("total_size", "total_file_count")
        bulk_sync(
            snapshots, ["id"], filters=models.Q(repository=self), exclude=exclude,
        )
        self.fetched_on = timezone.now()
        self.save()
        return self.snapshot_set.all()

    def sync_stats(self):
        logger.info("Syncing '%s' stats", self)
        stats = self.restic.stats()
        self.total_size = stats["total_size"]
        self.total_file_count = stats["total_file_count"]
        self.save()
        return stats


class LocalRepository(Repository):
    path = models.CharField(max_length=200, default="/srv/repository")

    class Meta:
        verbose_name = "Local Repository"
        verbose_name_plural = "Local Repositories"

    def repository_uri(self):
        return f"{self.path}"


class SFTPRepository(Repository):
    host = models.CharField(max_length=200, default="localhost")
    user = models.CharField(max_length=200, default="", blank=True)
    path = models.CharField(max_length=200, default="", blank=True)

    class Meta:
        verbose_name = "SFTP Repository"
        verbose_name_plural = "SFTP Repositories"

    def repository_uri(self):
        server = f"{self.user}@{self.host}" if self.user else self.host
        return f"sftp:{server}:{self.path}"


class S3Repository(Repository):
    endpoint = models.CharField(max_length=255, default="s3.eu-central-1.wasabisys.com")
    bucket = models.CharField(max_length=255)
    access_key_id = models.CharField(max_length=255)
    secret_access_key = models.CharField(max_length=255)

    class Meta:
        verbose_name = "S3 Repository"
        verbose_name_plural = "S3 Repositories"

    def repository_uri(self):
        return f"s3:{self.endpoint}/{self.bucket}"

    def environment(self):
        return {
            "AWS_ACCESS_KEY_ID": self.access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.secret_access_key,
        }


class Snapshot(models.Model):
    id = models.CharField(primary_key=True, editable=False, max_length=64)
    repository = models.ForeignKey(Repository, on_delete=models.CASCADE)
    hostname = models.CharField(max_length=100)
    username = models.CharField(max_length=100)
    paths = ArrayField(base_field=models.CharField(max_length=100))
    tags = ArrayField(base_field=models.CharField(max_length=100), blank=True)
    time = models.DateTimeField()
    short_id = models.CharField(editable=False, max_length=8)
    parent = models.CharField(max_length=64)
    tree = models.CharField(max_length=64)
    created_on = models.DateTimeField(auto_now_add=True)
    fetched_on = models.DateTimeField()
    total_size = models.BigIntegerField(null=True)
    total_file_count = models.IntegerField(null=True)

    class Meta:
        verbose_name = "Snapshot"
        verbose_name_plural = "Snapshots"
        ordering = ("-time",)

    def __str__(self):
        return self.short_id

    def human_size(self):
        return self.total_size and humansize(self.total_size)

    human_size.admin_order_field = "total_size"
    human_size.short_description = "Total size"

    @classmethod
    def from_restic(cls, object, repository):
        return cls(**object, fetched_on=timezone.now(), repository=repository)

    def sync(self, full=False):
        if full or self.total_size is None:
            self.sync_stats()
        self.get_root().get_children(force_sync=full)

    def sync_stats(self):
        logger.info("Syncing %s: %s stats", self.repository, self)
        stats = self.repository.restic.stats(self.id)
        self.total_size = stats["total_size"]
        self.total_file_count = stats["total_file_count"]
        self.save()

    def get_root(self):
        path, _ = SnapshotPath.objects.get_or_create(
            snapshot=self, path="/", defaults={"name": "root", "type": "dir"}
        )
        return path


class SnapshotPath(models.Model):
    snapshot = models.ForeignKey(Snapshot, on_delete=models.CASCADE)
    name = models.CharField(max_length=255)
    type = models.CharField(max_length=10)
    path = models.CharField(max_length=1000)
    uid = models.IntegerField(null=True)
    gid = models.IntegerField(null=True)
    size = models.BigIntegerField(null=True)
    mode = models.BigIntegerField(null=True)
    mtime = models.DateTimeField(null=True)
    atime = models.DateTimeField(null=True)
    ctime = models.DateTimeField(null=True)
    fetched_on = models.DateTimeField(null=True)

    class Meta:
        verbose_name = "Snapshot path"
        verbose_name_plural = "Snapshot paths"

    def __str__(self):
        return self.path

    @classmethod
    def from_restic(cls, object, snapshot):
        return cls(**object, snapshot=snapshot)

    def children_q(self):
        this = models.Q(pk=self.pk)
        return models.Q(snapshot=self.snapshot, path__startswith=self.path) & ~this

    def get_children(self, force_sync=False):
        repo = self.snapshot.repository
        if self.fetched_on and not force_sync:
            return SnapshotPath.objects.filter(self.children_q())
        logger.info("Fetching '%s %s %s' contents", repo, self.snapshot, self)
        files = repo.restic.ls(self.snapshot_id, self.path)
        files = [SnapshotPath.from_restic(f, self.snapshot) for f in files]
        bulk_sync(
            files, ("snapshot_id", "path"), filters=self.children_q(),
        )
        self.fetched_on = timezone.now()
        self.save()
        return files
