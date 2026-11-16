"""What the service sends a model package, built from intake's own values.

Intake stores Rubin measurements in a broker-neutral form; a model package
reads contract packets and head requests. This package is the translation
between the two. Nothing here imports Django, so it is tested and reused
without the database.
"""
