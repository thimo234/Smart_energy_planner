"""Test import helpers."""

from __future__ import annotations

import sys
import types
from pathlib import Path


def install_package_stub() -> None:
    """Import integration submodules without executing Home Assistant setup code."""

    repo_root = Path(__file__).resolve().parents[1]
    package_dir = repo_root / "custom_components" / "smart_energy_planner"

    custom_components = sys.modules.setdefault("custom_components", types.ModuleType("custom_components"))
    custom_components.__path__ = [str(repo_root / "custom_components")]

    package = sys.modules.setdefault(
        "custom_components.smart_energy_planner",
        types.ModuleType("custom_components.smart_energy_planner"),
    )
    package.__path__ = [str(package_dir)]
