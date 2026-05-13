"""Alpha sleeve registry."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import Sleeve


def build_sleeves(
    us_db: Path,
    cn_db: Path,
    factor_lab_db: Path,
    start: date,
    as_of: date,
    min_money_n: int,
) -> list["Sleeve"]:
    from .cn import build_cn_sleeves
    from .factor_lab import load_factor_lab_sleeves
    from .us import build_us_sleeves

    sleeves: list[Sleeve] = []
    sleeves.extend(build_us_sleeves(us_db, start, as_of, min_money_n))
    sleeves.extend(build_cn_sleeves(cn_db, start, as_of, min_money_n))
    sleeves.extend(load_factor_lab_sleeves(factor_lab_db, start, as_of, min_money_n))
    return sleeves


__all__ = ["Sleeve", "build_sleeves"]
