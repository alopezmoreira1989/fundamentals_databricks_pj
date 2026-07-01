"""Put the Streamlit app dir on sys.path so the test modules can ``import lib.*`` — the
Streamlit app's internal, app-relative helpers (Option A: the live app's lib/ is not
repackaged).

``fundamentals_pipeline`` itself is imported as an installed package
(``pip install -e .``, declared in requirements-dev.txt), so no repo-root sys.path entry
is needed for it.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STREAMLIT_APP = ROOT / "fundamentals_pipeline" / "60__frontends" / "61__streamlit"

# fundamentals_pipeline is imported as an installed package (pip install -e .); only the
# Streamlit app dir needs to be on sys.path, for the app-relative `import lib.*` (Option A).
_sp = str(STREAMLIT_APP)
if _sp not in sys.path:
    sys.path.insert(0, _sp)
