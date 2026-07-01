# utils.py
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]


def fmt_inr(val: float) -> str:
    if val is None:
        return "₹0"
    return f"₹{val:,.0f}"


def load_doctors() -> dict[str, float]:
    """Doctor name -> default fee percentage, read fresh from doctors.toml each call
    so edits to the file don't require an app restart."""
    cfg = Path(__file__).parent / "doctors.toml"
    with open(cfg, "rb") as f:
        return tomllib.load(f)["doctors"]
