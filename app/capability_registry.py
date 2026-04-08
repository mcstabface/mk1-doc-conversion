from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CapabilityDefinition:
    capability_id: str
    label: str
    description: str
    enabled_by_default: bool
    view_import_path: str


def get_registered_capabilities() -> tuple[CapabilityDefinition, ...]:
    return (
        CapabilityDefinition(
            capability_id="ingestion",
            label="Ingestion",
            description="Run deterministic ingestion and inspect run outputs.",
            enabled_by_default=True,
            view_import_path="app.capabilities.ingestion.view",
        ),
        CapabilityDefinition(
            capability_id="redaction",
            label="PII Redaction",
            description="Plan, approve, preview, and commit deterministic redactions.",
            enabled_by_default=True,
            view_import_path="app.capabilities.redaction.view",
        ),
        CapabilityDefinition(
            capability_id="audit_history",
            label="Audit / History",
            description="Inspect runs, plans, approvals, and truth overrides.",
            enabled_by_default=True,
            view_import_path="app.capabilities.audit_history.view",
        ),
    )