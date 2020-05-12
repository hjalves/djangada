# https://github.com/SIMBAChain/django-bulk-sync/blob/feature/django_22_bulk_update/bulk_sync/__init__.py
from collections import OrderedDict
import logging

from django.db import transaction

logger = logging.getLogger(__name__)


def bulk_sync(
    objs, key_fields, filters, fields=None, exclude=None, batch_size=None,
):
    """Combine bulk create, update, and delete.
    Make the DB match a set of in-memory objects.

    :param objs: Django ORM objects that are the desired state.
        They may or may not have `id` set.
    :param key_fields: Identifying attribute name(s) to match up `new_models` items
        with database rows. If a foreign key is being used as a key field, be sure
        to pass the `fieldname_id` rather than the `fieldname`.
    :param filters: Q() filters specifying the subset of the database to work in.
    :param fields: a list of fields to update - passed to django's bulk_update
    :param exclude: a list of fields to exclude from update
    :param batch_size: passes through to Django `bulk_create.batch_size` and
        `bulk_update.batch_size`, and controls how many objects are created/updated
        per SQL query.
    :return: Statistics
    """
    db_class = objs[0].__class__

    if not fields:
        # Get a list of fields that aren't PKs and aren't editable (e.g. auto_add_now)
        # for bulk_update
        fields = [
            field.name
            for field in db_class._meta.fields
            if not field.primary_key and not field.auto_created and field.editable
        ]

    if exclude:
        fields = [field for field in fields if field not in exclude]

    with transaction.atomic():
        subset_qs = db_class.objects.all()
        if filters:
            subset_qs = subset_qs.filter(filters)
        subset_qs = subset_qs.only("pk", *key_fields).select_for_update()

        def get_key(obj):
            return tuple(getattr(obj, k) for k in key_fields)

        existing = {get_key(obj): obj for obj in subset_qs}

        new_objs = []
        upd_objs = []
        for obj in objs:
            existing_obj = existing.pop(get_key(obj), None)
            if existing_obj is None:
                # halves: we should be able to provide the pk for new objects, right?
                # new_obj.pk = None
                new_objs.append(obj)
            else:
                obj.pk = existing_obj.pk
                upd_objs.append(obj)

        db_class.objects.bulk_create(new_objs, batch_size=batch_size)
        db_class.objects.bulk_update(upd_objs, fields=fields, batch_size=batch_size)
        # delete stale ones...
        subset_qs.filter(pk__in=[_.pk for _ in list(existing.values())]).delete()

        assert len(upd_objs) == len(objs) - len(new_objs)

        stats = {
            "created": len(new_objs),
            "updated": len(objs) - len(new_objs),
            "deleted": len(existing),
        }

        logger.debug(
            "{}: {} created, {} updated, {} deleted.".format(
                db_class.__name__, stats["created"], stats["updated"], stats["deleted"]
            )
        )

    return stats


def bulk_compare(old_models, new_models, key_fields, ignore_fields=None):
    """ Compare two sets of models by `key_fields`.
    `old_models`: Iterable of Django ORM objects to compare.
    `new_models`: Iterable of Django ORM objects to compare.
    `key_fields`: Identifying attribute name(s) to match up `new_models`
    items with database rows.  If a foreign key
            is being used as a key field, be sure to pass the `fieldname_id`
            rather than the `fieldname`.
    `ignore_fields`: (optional) If set, provide field names that should not be
    considered when comparing objects.

    Returns: dict of
        'added': list of all added objects.
        'unchanged': list of all unchanged objects.
        'updated': list of all updated objects.
        'updated_details': dict of {obj: {field_name: (old_value, new_value)}}
        for all changed fields in each updated object.
        'removed': list of all removed objects.

    """

    def get_key(obj):
        return tuple(getattr(obj, k) for k in key_fields)

    old_obj_dict = OrderedDict((get_key(obj), obj) for obj in old_models)

    new_objs = []
    change_details = {}
    updated_objs = []
    unchanged_objs = []

    for new_obj in new_models:
        old_obj = old_obj_dict.pop(get_key(new_obj), None)
        if old_obj is None:
            # This is a new object, so create it.
            # Make sure the primary key field is clear.
            new_obj.pk = None
            new_objs.append(new_obj)
        else:
            new_obj.id = old_obj.id

            cmp_result = compare_objs(old_obj, new_obj, ignore_fields)
            if cmp_result:
                updated_objs.append(new_obj)
                change_details[new_obj] = cmp_result
            else:
                unchanged_objs.append(new_obj)

    return {
        "added": new_objs,
        "unchanged": unchanged_objs,
        "updated": updated_objs,
        "updated_details": change_details,
        "removed": old_obj_dict.values(),
    }


def compare_objs(obj1, obj2, ignore_fields=None):
    """ Compare two Django ORM objects (presumably of the same model class).

    `obj1`: The first object to compare.
    `obj2`: The second object to compare.
    `key_fields`: Identifying attribute name(s) to match up `new_models` items
    with database rows.  If a foreign key
            is being used as a key field, be sure to pass the `fieldname_id`
            rather than the `fieldname`.
    `ignore_fields`: (optional) If set, provide field names that should not be
    considered when comparing objects.
            If a foreign key is being used as an ignore_field, be sure to pass
            the `fieldname_id` rather than the `fieldname`.

    Returns: dict of changed fields and their old/new values:
    {field_name: (old_value, new_value)}
    """

    ret = {}
    fields = obj1._meta.get_fields()
    for f in fields:
        if ignore_fields and f.attname in ignore_fields:
            continue

        v1 = f.to_python(getattr(obj1, f.attname))
        v2 = f.to_python(getattr(obj2, f.attname))
        if v1 != v2:
            ret[f.name] = (v1, v2)

    return ret
