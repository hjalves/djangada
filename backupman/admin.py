from django.contrib import admin
from django_better_admin_arrayfield.admin.mixins import DynamicArrayMixin
from polymorphic.admin import (
    PolymorphicParentModelAdmin,
    PolymorphicChildModelAdmin,
    PolymorphicChildModelFilter,
)

from backupman.models import (
    Repository,
    Snapshot,
    SnapshotPath,
    S3Repository,
    SFTPRepository,
    LocalRepository,
)


class TagFilter(admin.SimpleListFilter):
    title = 'tags'
    parameter_name = 'tags'

    def lookups(self, request, model_admin):
        tags = Snapshot.objects.values_list("tags", flat=True)
        tags = [(kw, kw) for sublist in tags for kw in sublist if kw]
        tags = sorted(set(tags))
        return tags

    def queryset(self, request, queryset):
        lookup_value = self.value()  # The clicked keyword. It can be None!
        if lookup_value:
            # the __contains lookup expects a list, so...
            queryset = queryset.filter(tags__contains=[lookup_value])
        return queryset


class SnapshotInline(admin.TabularInline, DynamicArrayMixin):
    model = Snapshot
    can_delete = False
    exclude = ("parent", "tree", "fetched_on", "total_size")
    readonly_fields = (
        "hostname",
        "username",
        "paths",
        "tags",
        "time",
        "human_size",
        "total_file_count",
    )
    show_change_link = True

    def has_add_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Repository)
class RepositoryAdmin(PolymorphicParentModelAdmin):
    polymorphic_list = True
    list_display = ("__str__", "type", "repository_uri", "human_size", "fetched_on")
    list_filter = (PolymorphicChildModelFilter,)
    readonly_fields = ("fetched_on", "human_size", "total_file_count")
    exclude = ("total_size",)
    actions = ("sync", "full_sync")
    child_models = (S3Repository, SFTPRepository, LocalRepository)

    def sync(self, request, queryset):
        for repo in queryset:
            repo.sync()

    sync.short_description = "Sync"

    def full_sync(self, request, queryset):
        for repo in queryset:
            repo.sync(full=True)

    full_sync.short_description = "Full sync"


class BaseRepositoryAdmin(PolymorphicChildModelAdmin):
    readonly_fields = ("repository_uri", "fetched_on", "human_size", "total_file_count")
    exclude = ("total_size",)

    inlines = [SnapshotInline]


@admin.register(S3Repository)
class S3RepositoryAdmin(BaseRepositoryAdmin):
    prepopulated_fields = {"description": ("bucket",)}


@admin.register(SFTPRepository)
class SFTPRepositoryAdmin(BaseRepositoryAdmin):
    prepopulated_fields = {"description": ("host", "path")}


@admin.register(LocalRepository)
class LocalRepositoryAdmin(BaseRepositoryAdmin):
    prepopulated_fields = {"description": ("path",)}


class SnapshotPathInline(admin.TabularInline):
    model = SnapshotPath
    fields = (
        "path",
        "type",
        "size",
        "mtime",
    )
    can_delete = False

    def has_add_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Snapshot)
class SnapshotAdmin(admin.ModelAdmin, DynamicArrayMixin):
    list_display = ("__str__", "time", "hostname", "tags", "paths", "human_size")
    readonly_fields = (
        "repository",
        "hostname",
        "username",
        "paths",
        "time",
        "id",
        "tree",
        "parent",
        "created_on",
        "fetched_on",
        "human_size",
        "total_file_count",
    )
    fieldsets = [
        (None, {"fields": ("id", "repository", "time", "fetched_on")}),
        ("Identification", {"fields": ("hostname", "username", "paths", "tags")}),
        ("Stats", {"fields": ("human_size", "total_file_count")}),
    ]
    exclude = ("total_size",)
    list_filter = ("repository", "hostname", TagFilter)
    actions = ["sync", "full_sync"]
    inlines = [SnapshotPathInline]

    def has_add_permission(self, request):
        return False

    def sync(self, request, queryset):
        for snap in queryset:
            snap.sync()

    def full_sync(self, request, queryset):
        for repo in queryset:
            repo.sync(full=True)

    full_sync.short_description = "Full sync"
