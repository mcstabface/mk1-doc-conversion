from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class CapabilityDefinition:
    capability_id: str
    label: str
    description: str
    enabled_by_default: bool
    view_import_path: str


def get_registered_capabilities() -> tuple[CapabilityDefinition, ...]:
    ...