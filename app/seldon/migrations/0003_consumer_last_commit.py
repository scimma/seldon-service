from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("seldon", "0002_intake_lag"),
    ]

    operations = [
        migrations.CreateModel(
            name="ConsumerLastCommit",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("broker", models.CharField(max_length=64)),
                ("instance", models.CharField(max_length=255)),
                ("committed_at", models.DateTimeField()),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("broker", "instance"),
                        name="unique_last_commit_per_instance",
                    )
                ],
            },
        ),
    ]
