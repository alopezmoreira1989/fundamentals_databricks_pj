"""Put the repo root and the Streamlit app dir on sys.path so the test modules can
``import fundamentals_pipeline._core.*`` and ``import lib.*`` without an installed package.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STREAMLIT_APP = ROOT / "fundamentals_pipeline" / "60_streamlit_app"

for _p in (ROOT, STREAMLIT_APP):
    _sp = str(_p)
    if _sp not in sys.path:
        sys.path.insert(0, _sp)
