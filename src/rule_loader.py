from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_quality_rules_path() -> Path:
    return get_project_root() / "configs" / "quality_rules.yaml"


def load_quality_rules() -> dict[str, Any]:
    rules_path = get_quality_rules_path()

    with open(rules_path, "r", encoding="utf-8") as file:
        rules = yaml.safe_load(file)

    if not isinstance(rules, dict):
        raise ValueError("Quality rules file must contain a dictionary at the top level.")

    return rules
