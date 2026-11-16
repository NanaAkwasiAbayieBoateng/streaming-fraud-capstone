# Puts the repo root on sys.path so `import streaming_fraud` works when
# pytest is invoked from anywhere (required pattern in this workspace).
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
