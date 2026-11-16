"""Make every forecast or refused detection owed, and drop those two states.

Forecasts and refusals now live per deployment and head, so a detection's
own state stops at owed. The previous migration copied the old outcomes, but
the old intake may have kept running after it and stored more, so the copy
runs again first; its statements skip what is already copied. That also
gives each newly copied detection its owed time (from its earliest outcome)
and qualification snapshot, before its state becomes owed.

Reversing sets a detection with an old forecast back to forecast, else one
with an old refusal back to refused; the copied outcomes are left for the
previous migration's reversal to remove.
"""

from importlib import import_module

from django.db import migrations, models

copy_outcomes = import_module("seldon.migrations.0006_copy_outcomes")

TO_OWED = """
UPDATE seldon_detection
SET state = 'owed'
WHERE state IN ('forecast', 'refused')
"""

FROM_OWED = [
    """
    UPDATE seldon_detection AS det SET state = 'refused'
    WHERE det.state = 'owed'
      AND EXISTS (SELECT 1 FROM seldon_refusal WHERE detection_id = det.id)
    """,
    """
    UPDATE seldon_detection AS det SET state = 'forecast'
    WHERE det.state IN ('owed', 'refused')
      AND EXISTS (SELECT 1 FROM seldon_forecast WHERE detection_id = det.id)
    """,
]


class Migration(migrations.Migration):

    dependencies = [
        ("seldon", "0006_copy_outcomes"),
    ]

    operations = [
        migrations.RunSQL(copy_outcomes.FORWARD, reverse_sql=migrations.RunSQL.noop),
        migrations.RunSQL(TO_OWED, reverse_sql=FROM_OWED),
        migrations.AlterField(
            model_name="detection",
            name="state",
            field=models.CharField(
                choices=[
                    ("pending", "Pending"),
                    ("owed", "Owed"),
                    ("missed", "Missed"),
                ],
                max_length=16,
            ),
        ),
    ]
