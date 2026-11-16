"""Drop the old forecast and refusal tables.

Their rows were copied into per-head outcomes by the two previous
migrations. Dropping them is irreversible: reversing recreates the empty
tables only. Old intake code that kept running after the previous migration
could still have stored rows, so the copy and the move to the owed state run
once more first; each statement skips what is already done.
"""

from importlib import import_module

from django.db import migrations

copy_outcomes = import_module("seldon.migrations.0006_copy_outcomes")
owed_states = import_module("seldon.migrations.0007_detection_owed_states")


class Migration(migrations.Migration):

    dependencies = [
        ("seldon", "0007_detection_owed_states"),
    ]

    operations = [
        migrations.RunSQL(copy_outcomes.FORWARD, reverse_sql=migrations.RunSQL.noop),
        migrations.RunSQL(owed_states.TO_OWED, reverse_sql=migrations.RunSQL.noop),
        migrations.DeleteModel(
            name="Forecast",
        ),
        migrations.DeleteModel(
            name="Refusal",
        ),
    ]
