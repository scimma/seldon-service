"""Django settings for the SELDON service.

Framework settings only. Everything model-related (checkpoint location, torch
thread count) lives in the ``SELDON_``-prefixed pydantic settings at
``seldon.config.settings``.
"""

import os
from pathlib import Path


def resolve_app_version() -> str:
    """Resolve ``APP_VERSION`` from the environment.

    The ``or`` form makes an empty ``APP_VERSION=`` fall back too, so a
    deployment that fails to template the value cannot impersonate a release.

    Returns:
        The value of ``APP_VERSION`` from the environment, or ``local`` when
        unset or empty.
    """
    return os.environ.get("APP_VERSION") or "local"


APP_VERSION = resolve_app_version()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get(
    "SECRET_KEY", "django-insecure-seldon-local-development-only-not-a-real-key"
)

DEBUG = os.environ.get("DJANGO_DEBUG", "false").lower() == "true"

ALLOWED_HOSTS = os.environ.get("DJANGO_ALLOWED_HOSTS", "*").split(",")

INSTALLED_APPS = [
    "seldon",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "seldon_project.urls"

WSGI_APPLICATION = "seldon_project.wsgi.application"

# The service has no models yet, so it has no database.
DATABASES: dict = {}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = False
USE_TZ = True

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
        },
    },
    "loggers": {
        "seldon": {
            "handlers": ["console"],
            "level": os.environ.get("SELDON_LOG_LEVEL", "INFO"),
        },
    },
}
