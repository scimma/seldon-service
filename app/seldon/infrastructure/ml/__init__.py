"""Django-free adapter that loads SELDON checkpoints for CPU inference.

The library compiles at construction and imports ``matplotlib.pyplot`` at
module scope, so compilation must be disabled and a non-interactive backend
chosen before torch or the library is first imported. The container sets both
in its environment; this is the backstop for any other process that imports
the adapter first (a script, a management command, a future MCP server).
``setdefault`` leaves a value the environment already carries untouched.
"""

import os

os.environ.setdefault("TORCH_COMPILE_DISABLE", "1")
os.environ.setdefault("MPLBACKEND", "Agg")
