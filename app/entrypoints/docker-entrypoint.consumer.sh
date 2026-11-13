#!/bin/bash

set -eo pipefail

python manage.py migrate --noinput
exec python manage.py consume_alerts
