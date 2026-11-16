"""The pinned SELDON checkpoint, packaged as a model-contract package.

Installed, it registers ``seldon-reference`` under the ``seldon.model_packages``
entry-point group; ``package.ReferencePackage`` implements ``describe`` and
``predict``. The checkpoint ships inside the package, and ``runtime`` loads it
once per process.
"""
