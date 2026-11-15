"""Apply database migrations while holding a Postgres advisory lock.

Several consumer replicas start at once, and each applies migrations before
consuming. Run that way, two could apply the same migration together. This
command takes a session advisory lock first, so replicas migrate one after
another: the first applies what is pending, and the rest find nothing left
to do. The lock is released when migrations finish or fail, and with the
connection if the process dies.
"""

import logging
from typing import Any

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger("seldon.migrate")

#: The Postgres advisory lock key (a fixed 64-bit integer) held while
#: migrating.
MIGRATE_LOCK_KEY = 0x5E1D0_316E8


class Command(BaseCommand):
    """Migrate the default database, one process at a time."""

    help = (
        "Apply migrations while holding a Postgres advisory lock, so several "
        "processes starting together migrate one at a time."
    )

    def handle(self, *args: Any, **options: Any) -> None:
        """Wait for the lock, migrate, and release the lock.

        Args:
            *args: Unused.
            **options: The parsed options; ``verbosity`` is passed on.
        """
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(%s)", [MIGRATE_LOCK_KEY])
        logger.info("Holding the migration lock")
        try:
            call_command(
                "migrate", interactive=False, verbosity=options.get("verbosity", 1)
            )
        finally:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", [MIGRATE_LOCK_KEY])
