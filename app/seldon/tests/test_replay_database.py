"""Tests for replacing a replay database that another process still uses."""

import copy

from django.db import OperationalError, connection, connections
from django.db.utils import ConnectionHandler
from django.test import TransactionTestCase

from seldon.management.commands.replay_alerts import create_database


class ReplaceDatabaseTests(TransactionTestCase):
    """Process 0's --replace against a database a worker is attached to."""

    def setUp(self) -> None:
        """Name a scratch database and make sure it does not exist."""
        self.name = f"{connection.settings_dict['NAME']}_replace"
        self._drop()

    def tearDown(self) -> None:
        """Drop the scratch database."""
        self._drop()

    def _drop(self) -> None:
        """Drop the scratch database if it exists, ending its connections."""
        quoted = connection.ops.quote_name(self.name)
        with connection._nodb_cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {quoted} WITH (FORCE)")

    def test_replace_ends_connections_to_the_old_database(self) -> None:
        """A worker holds a connection to the database from an earlier
        replay. Replacing it still succeeds, and the worker's connection is
        ended rather than left writing into the dropped database."""
        create_database(self.name, replace=False)
        settings = copy.deepcopy(connections.settings["default"])
        settings["NAME"] = self.name
        worker = ConnectionHandler({"default": settings})["default"]
        with worker.cursor() as cursor:
            cursor.execute("SELECT 1")

        create_database(self.name, replace=True)

        with self.assertRaises(OperationalError):
            with worker.cursor() as cursor:
                cursor.execute("SELECT 1")
        worker.close()
