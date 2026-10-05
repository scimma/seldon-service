"""Domain types and services for the SELDON service.

Nothing here imports Django. The models import no torch either; the services
drive the model adapter, so importing one imports torch.
"""
