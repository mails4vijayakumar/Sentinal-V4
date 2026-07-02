from shared.models import (
    IncidentSource, OrchestratorEvent, Severity, IncidentFlow,
)

def test_alertmanager_source_value():
    assert IncidentSource.ALERTMANAGER == "alertmanager"

def test_orchestrator_event_accepts_environment_and_group_key():
    e = OrchestratorEvent(
        source=IncidentSource.ALERTMANAGER,
        external_id="am-deadbeef",
        severity=Severity.HIGH,
        flow=IncidentFlow.PRIMARY,
        title="HighCPU",
        environment="staging",
        am_group_key="{}:{alertname=\"HighCPU\"}",
    )
    assert e.environment == "staging"
    assert e.am_group_key.startswith("{}:")

def test_orchestrator_event_fields_default_to_none():
    e = OrchestratorEvent(
        source=IncidentSource.DYNATRACE,
        external_id="P-1",
        severity=Severity.HIGH,
        flow=IncidentFlow.PRIMARY,
        title="x",
    )
    assert e.environment is None
    assert e.am_group_key is None
