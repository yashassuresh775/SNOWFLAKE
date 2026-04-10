"""
Streamlit in Snowflake often uses this filename as the main entry.

Keep app.py in the same folder. If Snowsight only has one file, replace this
entire file with the contents of app.py (do not keep the old template that
imports plotly).
"""

from pathlib import Path

_app = Path(__file__).resolve().parent / "app.py"
_src = _app.read_text(encoding="utf-8")
exec(compile(_src, str(_app), "exec"), globals())
