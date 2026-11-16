"""Forecast workers: turn owed detections into stored outcomes for one deployment.

Intake only records which detections are owed. A worker process, running in
an image that holds exactly one model package, claims that work for its
deployment, calls the package with no database lock held, and stores what
each head returned. Nothing here imports torch; the package does, in its
own process.
"""
