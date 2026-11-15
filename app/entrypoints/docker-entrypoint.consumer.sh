#!/bin/bash

set -eo pipefail

# Several consumer replicas may start at once; the lock makes them migrate
# one after another.
python manage.py migrate_locked
exec python manage.py consume_alerts
